import pymysql
from datetime import datetime


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
def write_log_to_db(status, note, process,log_date=None ):
    lines = CrawInformationDB()
    if not lines:
        print("Không thể ghi log do không có thông tin kết nối database.")
        return

    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )
        if connection.open:
            cursor = connection.cursor()
            sql_query = """INSERT INTO log (status, note, process, log_date) VALUES (%s, %s, %s, %s)"""
            data = (status, note, process, log_date if log_date else datetime.now())
            cursor.execute(sql_query, data)
            connection.commit()
    except Exception as e:
        print(f"Lỗi khi ghi log: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

# Database connection manager class
class DatabaseConnection:
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


# Method to transform staging data to fact table
def transform_staging_to_fact():
    """
    Transforms data from staging tables into the fact table,
    including mapping to `date_dim` and `latest_report`.
    """
    # Buoc 1
    db_credentials = CrawInformationDB()
    # Buoc 2
    if not db_credentials:
        write_log_to_db("ERROR", "Thiếu dũ liệu để kết nối tới db trong quá trình tranform to fact.", "Changing to Fact")
        return
    # Buoc 3
    try:
        with DatabaseConnection(*db_credentials) as conn:
            with conn.cursor() as cursor:
                # Buoc 4
                write_log_to_db("INFO", "Fetching lookup table data.", "Changing to Fact")
                # Buoc 5
                cursor.execute("SELECT id, values_country FROM country")
                country_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_location FROM location")
                location_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_weather FROM weather_description")
                weather_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, time FROM latesreport")
                latest_report_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, date_values FROM date_dim")
                date_map = {row[1]: row[0] for row in cursor.fetchall()}

                # Buoc 6
                write_log_to_db("INFO", "Fetching data from staging table.", "Changing to Fact")
                # Buoc 7 GetStagingData
                cursor.execute("""
                    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time
                    FROM staging
                """)
                staging_data = cursor.fetchall()
                # Buoc 8
                write_log_to_db("INFO", f"Đã lấy thành công {len(staging_data)} dòng dữ liệu từ bảng staging.", "Changing to Fact")


                fact_inserts = []
                # Buoc 9
                for record in staging_data:
                    nation, temperature, weather_status, location, current_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)
                    report_id = latest_report_map.get(latest_report)

                    # Buoc 9.1
                    if current_time not in date_map:
                        # Buoc 9.2.2
                        day = current_time.day
                        month = current_time.month
                        year = current_time.year
                        # Buoc 9.2.3 insertDateDim
                        cursor.execute(
                            "INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)",
                            (current_time, day, month, year)
                        )
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
                        # Buoc 9.2.4
                        date_map[current_time] = date_id
                    else:
                        # Buoc 9.2.1
                        date_id = date_map[current_time]

                    # Buoc 9.3
                    if country_id and location_id and weather_id and report_id and date_id:
                        # Buoc 9.3.1
                        fact_inserts.append((country_id, location_id, weather_id, date_id, report_id,
                                             temperature, visibility, pressure, humidity, dew_point, dead_time))
                    else:
                        # Buoc 9.3.2
                        write_log_to_db("WARNING", f"Bỏ qua cột dư liệu do thiếu ánh xạ: {record}", "Changing to Fact")

                # Buoc 9.4
                if fact_inserts:
                    # Buoc 9.4.1
                    write_log_to_db("INFO", "Thêm dữ liệu vào bảng fact.", "Changing to Fact")
                    # Buoc 9.4.2 insertFactTable
                    insert_query = """
                        INSERT INTO fact_table (
                            country_id, location_id, weather_id, date_id, report_id, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, fact_inserts)
                    conn.commit()
                    # Buoc 9.4.3
                    write_log_to_db("SUCCESS", f"Thành công thêm {len(fact_inserts)} vào bảng fact.", "Changing to Fact")
                else:
                    # Buoc 9.4.4
                    write_log_to_db("INFO", "Khong có dự liệu hợp lệ để thêm vào bảng fact.", "Changing to Fact")
    # Buoc 10
    except Exception as e:
        # Buoc 10.1
        write_log_to_db("ERROR", f"Error during staging to fact transformation: {e}", "Changing to Fact")



# Entry point
transform_staging_to_fact()
