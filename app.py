from flask import Flask, request, send_file, abort
import os
import io
from ftplib import FTP
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd

app = Flask(__name__)

# ---- Variabili ambiente (Render → Environment) ----
FTP_HOST = os.getenv("GME_FTP_HOST")
FTP_USER = os.getenv("GME_FTP_USER")
FTP_PASS = os.getenv("GME_FTP_PASS")
FTP_PATH = os.getenv("GME_FTP_PATH", "/MercatiElettrici/MGP_Prezzi")


# ---- Utilità ----
def daterange(start_date, end_date):
    """Genera tutte le date tra start_date e end_date inclusi"""
    for n in range((end_date - start_date).days + 1):
        yield start_date + timedelta(n)


def open_ftp():
    """Apre connessione FTP al GME"""
    ftp = FTP(FTP_HOST)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_PATH)
    return ftp


def retrieve_day(ftp, day):
    """Scarica file XML di un singolo giorno"""
    filename = f"PrezziMGP_{day.strftime('%Y%m%d')}.xml"
    bio = io.BytesIO()
    try:
        ftp.retrbinary(f"RETR {filename}", bio.write)
        bio.seek(0)
        return bio.read()
    except Exception:
        return None


def parse_xml(xml_bytes, day):
    """Parsa XML PUN giornaliero e ritorna lista di righe"""
    rows = []
    try:
        root = ET.fromstring(xml_bytes)
        for elem in root.findall(".//PrezzoOra"):
            ora = int(elem.find("Ora").text)
            pun = float(elem.find("PUN").text.replace(",", "."))
            rows.append({"data": day, "ora": ora, "pun": pun})
    except Exception:
        return []
    return rows


# ---- API principale ----
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

    # ---- Scarico unico con stessa connessione FTP ----
    all_rows = []
    try:
        ftp = open_ftp()
    except Exception as e:
        return abort(502, f"Connessione FTP fallita: {e}")

    try:
        for day in daterange(d1, d2):
            xml = retrieve_day(ftp, day)
            if not xml:
                continue
            all_rows.extend(parse_xml(xml, day))
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    if not all_rows:
        return abort(404, "Nessun dato trovato nell’intervallo richiesto.")

    df = pd.DataFrame(all_rows).sort_values(["data", "ora"])

    # ---- Output CSV ----
    if fmt == "csv":
        data = df.to_csv(index=False).encode("utf-8")
        return send_file(
            io.BytesIO(data),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"PUN_{d1}_{d2}.csv"
        )

    # ---- Output XLSX (reale, sempre valido) ----
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="PUN", index=False)
    bio.seek(0)
    return send_file(
        bio,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"PUN_{d1}_{d2}.xlsx"
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
