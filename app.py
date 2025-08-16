from flask import Flask, request, Response
import io
import csv
import xlsxwriter
import datetime

app = Flask(__name__)

@app.route("/download")
def download():
    fmt = request.args.get("format", "csv")
    start = request.args.get("start")
    end = request.args.get("end")

    # Per ora mettiamo dati fittizi, poi li sostituiamo con FTP
    start_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")
    data = []
    day = start_date
    while day <= end_date:
        for hour in range(1, 25):
            data.append([day.strftime("%Y-%m-%d"), hour, 100 + hour])  # PUN fittizio
        day += datetime.timedelta(days=1)

    if fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Data", "Ora", "PUN"])
        writer.writerows(data)
        return Response(output.getvalue(),
                        mimetype="text/csv",
                        headers={"Content-Disposition": "attachment;filename=pun.csv"})
    elif fmt == "xlsx":
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()
        worksheet.write_row(0, 0, ["Data", "Ora", "PUN"])
        for idx, row in enumerate(data, start=1):
            worksheet.write_row(idx, 0, row)
        workbook.close()
        output.seek(0)
        return Response(output.read(),
                        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        headers={"Content-Disposition": "attachment;filename=pun.xlsx"})
    else:
        return "Formato non supportato", 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
