import bs4
import requests
import csv
import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def CrawInformationDB():
    # Buoc 1
    lines = []

    try:
        # Buoc 2
        with open(r"D:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
            # Buoc 3
            for line in file:
                # Buoc 4
                lines.append(line.strip())
        return lines
    # Buoc 5
    except Exception as e:
        # Buoc 6
        print(f"Lỗi đọc file cấu hình: {e}")
        return []
# Method to write logs to the database (insertLog)
def write_log_to_db(status, note, process, log_date=None):
    lines = CrawInformationDB()
    if not lines or len(lines) < 4:
        print("Không thể ghi log do không có thông tin kết nối database.")
        return

    try:
        # Kết nối đến database
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        with connection:
            with connection.cursor() as cursor:
                # Gọi stored procedure InsertLog
                procedure_name = "InsertLog"
                log_date = log_date if log_date else datetime.now()

                try:
                    # Thực thi stored procedure
                    cursor.callproc(procedure_name, (status, note, process, log_date))
                    connection.commit()
                    print("Log đã được ghi thành công!")
                except Exception as e:
                    print(f"Lỗi khi thực thi stored procedure: {e}")
    except Exception as e:
        print(f"Lỗi khi kết nối database: {e}")