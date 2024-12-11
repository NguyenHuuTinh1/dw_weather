import pymysql
from datetime import datetime

def CrawInformationDB():
    """
    Đọc thông tin cấu hình kết nối từ file connect_db.txt.
    """
    lines = []
    try:
        with open(r"D:\\dw_weather\\dw_weather\\src\\connect_db.txt", "r", encoding="utf-8") as file:
            for line in file:
                lines.append(line.strip())
        return lines[:4]  # Chỉ lấy 4 dòng đầu tiên
    except Exception as e:
        print(f"Lỗi đọc file cấu hình: {e}")
        return []

def write_log_to_db(status, note, process, log_date=None):
    """
    Ghi log vào bảng log trong database bằng stored procedure InsertLog.
    """
    lines = CrawInformationDB()
    if not lines or len(lines) < 4:
        print("Không thể ghi log do không có thông tin kết nối database.")
        return

    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        with connection:
            with connection.cursor() as cursor:
                procedure_name = "InsertLog"
                log_date = log_date if log_date else datetime.now()

                try:
                    cursor.callproc(procedure_name, (status, note, process, log_date))
                    connection.commit()
                    print("Log đã được ghi thành công!")
                except Exception as e:
                    print(f"Lỗi khi thực thi stored procedure: {e}")
    except Exception as e:
        print(f"Lỗi khi kết nối database: {e}")

class DatabaseConnection:
    """
    Quản lý kết nối database thông qua context manager.
    """
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def __enter__(self):
        self.connection = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

def transform_staging_to_fact():
    """
    Transform dữ liệu từ bảng staging sang bảng fact, bao gồm ánh xạ các dim và xử lý dữ liệu mới.
    """
    db_credentials = CrawInformationDB()
    if not db_credentials or len(db_credentials) != 4:
        write_log_to_db("ERROR", "Thiếu dữ liệu để kết nối tới database trong quá trình transform to fact.", "Changing to Fact")
        return

    try:
        with DatabaseConnection(*db_credentials) as conn:
            with conn.cursor() as cursor:
                write_log_to_db("INFO", "Fetching lookup table data.", "Changing to Fact")

                cursor.callproc('GetCountryMap')
                country_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.callproc('GetLocationMap')
                location_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.callproc('GetWeatherDescription')
                weather_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.callproc('GetLatestReport')
                latest_report_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.callproc('GetDateDim')
                date_map = {row[1]: row[0] for row in cursor.fetchall()}

                write_log_to_db("INFO", "Fetching data from staging table.", "Changing to Fact")

                cursor.callproc('GetStagingData')
                staging_data = cursor.fetchall()  # Lấy dữ liệu trả về từ stored procedure

                write_log_to_db("INFO", f"Đã lấy thành công {len(staging_data)} dòng dữ liệu từ bảng staging.", "Changing to Fact")
                fact_inserts = []

                for record in staging_data:
                    (nation, temperature, weather_status, location, current_time, latest_report,
                     visibility, pressure, humidity, dew_point, dead_time) = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)
                    report_id = latest_report_map.get(latest_report)

                    if current_time not in date_map:
                        day = current_time.day
                        month = current_time.month
                        year = current_time.year

                        cursor.callproc('InsertDateDim', (current_time, day, month, year))
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
                        date_map[current_time] = date_id
                    else:
                        date_id = date_map[current_time]

                    if country_id and location_id and weather_id and report_id and date_id:
                        fact_inserts.append((country_id, location_id, weather_id, date_id, report_id,
                                             temperature, visibility, pressure, humidity, dew_point, dead_time))
                    else:
                        write_log_to_db("WARNING", f"Bỏ qua dữ liệu do thiếu ánh xạ: {record}", "Changing to Fact")

                if fact_inserts:
                    write_log_to_db("INFO", "Thêm dữ liệu vào bảng fact.", "Changing to Fact")
                    for fact in fact_inserts:
                        try:
                            cursor.callproc('InsertFactTable', fact)
                            conn.commit()
                        except Exception as e:
                            write_log_to_db("ERROR", f"Lỗi khi thêm dữ liệu vào fact: {fact} - {e}", "Changing to Fact")
                    write_log_to_db("SUCCESS", f"Thành công thêm {len(fact_inserts)} dòng vào bảng fact.", "Changing to Fact")
                else:
                    write_log_to_db("INFO", "Không có dữ liệu hợp lệ để thêm vào bảng fact.", "Changing to Fact")
    except Exception as e:
        write_log_to_db("ERROR", f"Error during staging to fact transformation: {e}", "Changing to Fact")

# Entry point
transform_staging_to_fact()
