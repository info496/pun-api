import os
import io
import ftplib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, request, send_file, abort
import pandas as pd

app = Flask(__name__)

# ========= Config via ENV (accetta sia GME_FTP_* che FTP_*) =========
FTP_HOST = os.getenv("GME_FTP_HOST") or os.getenv("FTP_HOST") or "download.mercatoelettrico.org"
FTP_USER = os.getenv("GME_FTP_USER") or os.getenv("FTP_USER")
FTP_PASS = os.getenv("GME_FTP_PASS") or os.getenv("FTP_PASS")
FTP_DIR  = os.getenv("GME_FTP_DIR")  or os.getenv("GME_FTP_PATH") or os.getenv("FTP_PATH") or "/MercatiElettrici/MGP_Prezzi"
USE_FTPS = (os.getenv("USE_FTPS") or os.getenv("FTPS") or "0") == "1"
TIMEOUT  = int(os.getenv("FTP_TIMEOUT", "120"))

ZONES = ["NORD","CNOR","CSUD","SUD","SICI","SARD","CALA","NAT"]  # estendibile


# ========= Utility =========
def daterange(d1, d2):
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)

def _dec(txt):
    if not txt:
        return None
    return float(txt.replace(",", ".").strip())

def _tagname(el):
    # rimuove eventuale namespace
    return el.tag.split("}", 1)[-1]


# ========= FTP helpers =========
def open_ftp():
    FTPClass = ftplib.FTP_TLS if USE_FTPS else ftplib.FTP
    ftp = FTPClass(FTP_HOST, timeout=TIMEOUT)
    ftp.login(FTP_USER, FTP_PASS)
    if USE_FTPS:
        ftp.auth(); ftp.prot_p()
    ftp.set_pasv(True)
    if FTP_DIR:
        ftp.cwd(FTP_DIR)
    return ftp

def retrieve_day(ftp, day):
    """
    Prova prima con elenco 'nlst' cercando un file che contenga YYYYMMDD
    e 'MGPPrezzi' (o 'Prezzi'). Se non trovato, prova una lista di nomi standard.
    Ritorna bytes oppure None.
    """
    ymd = day.strftime("%Y%m%d")
    try:
        files = ftp.nlst()
    except Exception:
        files = []

    # candidati tramite lista
    cand = [f for f in files if ymd in f and ("MGPPrezzi" in f or "Prezzi" in f)]
    # ordina per lunghezza (spesso quello 'giusto' è corto)
    cand.sort(key=len)

    tried = []
    for fname in cand + [
        f"{ymd}MGPPrezzi.xml",
        f"MGPPrezzi_{ymd}.xml",
        f"Prezzi_{ymd}.xml",
    ]:
        if fname in tried:
            continue
        tried.append(fname)
        buf = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {fname}", buf.write)
            return buf.getvalue()
        except Exception:
            continue
    return None


# ========= Parsing XML (stile GME 'MGPPrezzi') =========
def parse_xml(xml_bytes, the_date):
    """
    Estrae righe con: data, ora, PUN e (opzionale) zone.
    Struttura tipica: molti nodi <Prezzi> con figli: Ora, PUN, NORD, SUD, ...
    """
    rows = []
    root = ET.fromstring(xml_bytes)

    # cerca tutti i nodi che si chiamano 'Prezzi' ignorando namespace
    prezzi_nodes = [n for n in root.iter() if _tagname(n) == "Prezzi"]
    for n in prezzi_nodes:
        rec = {
            "data": the_date.strftime("%Y-%m-%d"),
            "ora": _safe_int(n.findtext("./Ora")),
            "PUN": _dec(n.findtext("./PUN")),
        }
        for z in ZONES:
            v = n.findtext("./" + z)
            if v is not None:
                rec[z] = _dec(v)
        rows.append(rec)

    return rows

def _safe_int(txt):
    try:
        return int((txt or "").strip())
    except Exception:
        return None


# ========= Endpoint =========
@app.route("/download")
def download():
    if not (FTP_USER and FTP_PASS):
        return abort(500, "Credenziali FTP non configurate sul server.")

    fmt   = (request.args.get("format") or "csv").lower()
    start = request.args.get("start")
    end   = request.args.get("end")

    if fmt not in ("csv", "xlsx"):
        return abort(400, "format deve essere csv o xlsx")
    if not (start and end):
        return abort(400, "start e end obbligatori (YYYY-MM-DD)")

    try:
        d1 = datetime.strptime(start, "%Y-%m-%d").date()
        d2 = datetime.strptime(end,   "%Y-%m-%d").date()
    except ValueError:
        return abort(400, "Date in formato errato. Usa YYYY-MM-DD")
    if d2 < d1:
        return abort(400, "end dev’essere >= start")

    # ---- Scarico con UNA SOLA CONNESSIONE per tutto l'intervallo ----
    all_rows = []
    try:
        ftp = open_ftp()
    except Exception as e:
        return abort(502, f"Connessione FTP fallita: {e}")

    try:
        for day in daterange(d1, d2):
            xml = retrieve_day(ftp, day)
            if not xml:
                # se vuoi essere "rigoroso", decommenta:
                # ftp.quit(); return abort(404, f"File mancante per {day}")
                continue
            rows = parse_xml(xml, day)
            all_rows.extend(rows)
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    if not all_rows:
        return abort(404, "Nessun dato trovato nell’intervallo richiesto.")

    df = pd.DataFrame(all_rows).sort_values(["data", "ora"])

    if fmt == "csv":
        data = df.to_csv(index=False).encode("utf-8")
        return send_file(io.BytesIO(data),
                         mimetype="text/csv",
                         as_attachment=True,
                         download_name=f"PUN_{d1}_{d2}.csv")

    # XLSX reale (sempre)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="PUN", index=False)
    bio.seek(0)
    return send_file(bio,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True,
                     download_name=f"PUN_{d1}_{d2}.xlsx")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
