import os
import io
import ftplib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, request, Response, abort

app = Flask(__name__)

# ====== ENV (accetta GME_FTP_* o FTP_*) ======
FTP_HOST = os.getenv("GME_FTP_HOST") or os.getenv("FTP_HOST") or "download.mercatoelettrico.org"
FTP_USER = os.getenv("GME_FTP_USER") or os.getenv("FTP_USER")
FTP_PASS = os.getenv("GME_FTP_PASS") or os.getenv("FTP_PASS")
FTP_DIR  = os.getenv("GME_FTP_DIR")  or os.getenv("GME_FTP_PATH") or os.getenv("FTP_PATH") or "/MercatiElettrici/MGP_Prezzi"
USE_FTPS = (os.getenv("USE_FTPS") or os.getenv("FTPS") or "0") == "1"
TIMEOUT  = int(os.getenv("FTP_TIMEOUT", "120"))

# ====== Utils ======
def daterange(d1, d2):
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)

def _dec(s):
    if not s: return None
    # PUN può avere la virgola decimale negli XML
    try:
        return float(s.replace(",", ".").strip())
    except:
        return None

def _safe_int(s):
    try: return int((s or "").strip())
    except: return None

# ====== FTP ======
def open_ftp():
    FTPClass = ftplib.FTP_TLS if USE_FTPS else ftplib.FTP
    ftp = FTPClass(FTP_HOST, timeout=TIMEOUT)
    ftp.login(FTP_USER, FTP_PASS)
    if USE_FTPS:
        ftp.auth()
        ftp.prot_p()
    ftp.set_pasv(True)
    if FTP_DIR:
        ftp.cwd(FTP_DIR)
    return ftp

def possible_names(day):
    ymd = day.strftime("%Y%m%d")
    return [f"{ymd}MGPPrezzi.xml", f"MGPPrezzi_{ymd}.xml", f"Prezzi_{ymd}.xml"]

def retrieve_day(ftp, day):
    # prova nomi tipici
    for fn in possible_names(day):
        buf = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {fn}", buf.write)
            return buf.getvalue()
        except Exception:
            continue
    # fallback: cerca tra i file presenti
    try:
        ymd = day.strftime("%Y%m%d")
        files = ftp.nlst()
        for fn in sorted([f for f in files if ymd in f and ("MGPPrezzi" in f or "Prezzi" in f)], key=len):
            buf = io.BytesIO()
            try:
                ftp.retrbinary(f"RETR {fn}", buf.write)
                return buf.getvalue()
            except Exception:
                continue
    except Exception:
        pass
    return None

# ====== Parser ======
def iter_rows_from_xml(xml_bytes, the_date):
    """Genera tuple (data_str, ora_int, pun_str_with_comma)."""
    root = ET.fromstring(xml_bytes)
    for n in root.iter():
        if n.tag.split("}",1)[-1] != "Prezzi":  # ignora namespace
            continue
        data_str = the_date.strftime("%Y-%m-%d")
        ora = _safe_int(n.findtext("Ora"))
        pun_txt = n.findtext("PUN")
        if pun_txt is None:
            pun_str = ""
        else:
            # converti a float e poi rimetti la virgola per CSV IT
            val = _dec(pun_txt)
            pun_str = (str(val).replace(".", ",")) if val is not None else pun_txt
        yield (data_str, ora if ora is not None else "", pun_str)

# ====== CSV streaming ======
def stream_csv(d1, d2):
    # intestazione
    yield "data;ora;PUN\n"
    # una sola connessione FTP per tutto l'intervallo
    try:
        ftp = open_ftp()
    except Exception as e:
        # messaggio leggibile nel CSV in caso di errore iniziale
        yield f"# ERRORE FTP: {e}\n"
        return

    try:
        for day in daterange(d1, d2):
            xml = retrieve_day(ftp, day)
            if not xml:
                # giorno mancante → salta
                continue
            for data_str, ora, pun_str in iter_rows_from_xml(xml, day):
                yield f"{data_str};{ora};{pun_str}\n"
    finally:
        try: ftp.quit()
        except: pass

# ====== Endpoint ======
@app.route("/download")
def download():
    # Solo CSV (nessun parametro format)
    start = request.args.get("start")
    end   = request.args.get("end")
    if not (start and end):
        return abort(400, "start e end obbligatori (YYYY-MM-DD)")

    if not (FTP_USER and FTP_PASS):
        return abort(500, "Credenziali FTP non configurate sul server.")

    try:
        d1 = datetime.strptime(start, "%Y-%m-%d").date()
        d2 = datetime.strptime(end,   "%Y-%m-%d").date()
    except ValueError:
        return abort(400, "Date in formato errato. Usa YYYY-MM-DD")
    if d2 < d1:
        return abort(400, "end dev’essere >= start")

    # risposta stream: parte subito e regge intervalli lunghi
    filename = f"PUN_{d1}_{d2}.csv"
    return Response(
        stream_csv(d1, d2),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
