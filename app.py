import os, io, ftplib, xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, request, send_file, abort
import pandas as pd

app = Flask(__name__)

# ========= Config via ENV =========
# Legge prima GME_FTP_*, altrimenti FTP_*
FTP_HOST = os.getenv("GME_FTP_HOST") or os.getenv("FTP_HOST") or "download.mercatoelettrico.org"
FTP_USER = os.getenv("GME_FTP_USER") or os.getenv("FTP_USER")
FTP_PASS = os.getenv("GME_FTP_PASS") or os.getenv("FTP_PASS")
FTP_DIR  = os.getenv("GME_FTP_DIR")  or os.getenv("FTP_PATH") or "/MercatiElettrici/MGP_Prezzi"
USE_FTPS = (os.getenv("USE_FTPS") or os.getenv("FTPS") or "0") == "1"
TIMEOUT  = int(os.getenv("FTP_TIMEOUT", "120"))  # più alto per range lunghi

def daterange(d1, d2):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def possible_filenames(d):
    ymd = d.strftime("%Y%m%d")
    return [
        f"{ymd}MGPPrezzi.xml",     # es. 20250817MGPPrezzi.xml
        f"MGPPrezzi_{ymd}.xml",    # variante
        f"Prezzi_{ymd}.xml"
    ]

def dec(txt):
    if not txt: return None
    return float(txt.replace(",", ".").strip())

def parse_xml(xml_bytes, day):
    root = ET.fromstring(xml_bytes)
    rows = []
    for n in root.findall(".//Prezzi"):
        rows.append({
            "data": day.strftime("%Y-%m-%d"),
            "ora": int((n.findtext("Ora") or "0")),
            "PUN": dec(n.findtext("PUN")),
            "NORD": dec(n.findtext("NORD")),
            "CNOR": dec(n.findtext("CNOR")),
            "CSUD": dec(n.findtext("CSUD")),
            "SUD":  dec(n.findtext("SUD")),
            "SICI": dec(n.findtext("SICI")),
            "SARD": dec(n.findtext("SARD")),
        })
    return rows

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
    for fname in possible_filenames(day):
        buf = io.BytesIO()
        try:
            ftp.retrbinary(f"RETR {fname}", buf.write)
            return buf.getvalue()
        except Exception:
            continue
    return None

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

    all_rows = []
    # ----- UNA SOLA CONNESSIONE PER TUTTO L'INTERVALLO -----
    try:
        ftp = open_ftp()
    except Exception as e:
        return abort(502, f"Connessione FTP fallita: {e}")

    try:
        for day in daterange(d1, d2):
            xml = retrieve_day(ftp, day)
            if not xml:
                # Se vuoi fallire quando manca un giorno:
                # ftp.quit(); return abort(404, f"File mancante per {day}")
                continue
            all_rows.extend(parse_xml(xml, day))
    finally:
        try:
            ftp.quit()
        except Exception:
            pass
    # --------------------------------------------------------

    if not all_rows:
        return abort(404, "Nessun dato trovato nell’intervallo richiesto.")

    df = pd.DataFrame(all_rows).sort_values(["data","ora"])

    if fmt == "csv":
        data = df.to_csv(index=False).encode("utf-8")
        return send_file(io.BytesIO(data),
                         mimetype="text/csv",
                         as_attachment=True,
                         download_name=f"PUN_{d1}_{d2}.csv")
    else:
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
