import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Phương thức đọc cấu hình từ file
def CrawInformationDB():
<<<<<<< Updated upstream
    """
    Đọc cấu hình từ file và trả về danh sách các giá trị.
    Số lượng giá trị trả về tùy thuộc vào nội dung file.
    """
=======
    # Buoc 1
    lines = []

>>>>>>> Stashed changes
    try:
        # Buoc 2
        with open(r"D:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
<<<<<<< Updated upstream
            return [line.strip() for line in file]
=======
            # Buoc 3
            for line in file:
                # Buoc 4
                lines.append(line.strip())
        return lines
    # Buoc 5
>>>>>>> Stashed changes
    except Exception as e:
        # Buoc 6
        print(f"Lỗi đọc file cấu hình: {e}")
        return []

# Phương thức gửi email report
def send_email(subject, body):
    db_config = CrawInformationDB()
    if len(db_config) < 3:
        print("Không đủ thông tin email trong file cấu hình.")
        return

    email, password, email_sent = db_config[-3:]
    try:
        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()  # Bật chế độ bảo mật
        session.login(email, password)

        msg = MIMEMultipart()
        msg['From'] = email
        msg['To'] = email_sent
        msg['Subject'] = subject  # Đặt tiêu đề email
        msg.attach(MIMEText(body, 'plain'))
        session.sendmail(email, email_sent, msg.as_string())
        session.quit()
        print('Email sent!')
    except Exception as e:
        print(f"Lỗi khi gửi email: {e}")

# Ghi log vào cơ sở dữ liệu
def write_log_to_db(status, note, process, log_date=None):
    db_config = CrawInformationDB()
    if len(db_config) < 4:
        print("Không đủ thông tin kết nối để ghi log.")
        return

    host, user, password, database = db_config[:4]
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        with connection.cursor() as cursor:
            sql_query = """INSERT INTO log (status, note, process, log_date) VALUES (%s, %s, %s, %s)"""
            data = (status, note, process, log_date if log_date else datetime.now())
            cursor.execute(sql_query, data)
            connection.commit()
    except Exception as e:
        print(f"Lỗi khi ghi log: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

# Lớp quản lý kết nối cơ sở dữ liệu
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

# Biến đổi dữ liệu từ staging sang fact table
def transform_staging_to_fact():
    """
    Biến đổi dữ liệu từ bảng staging sang bảng fact.
    """
<<<<<<< Updated upstream
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_config = CrawInformationDB()

    if len(db_config) < 4:
        write_log_to_db("ERROR", "Thiếu thông tin cấu hình kết nối database.", "Changing to Fact")
        return

    host, user, password, database = db_config[:4]

    try:
        with DatabaseConnection(host, user, password, database) as conn:
            with conn.cursor() as cursor:
                # Fetch mappings từ lookup table
=======
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
>>>>>>> Stashed changes
                cursor.execute("SELECT id, values_country FROM country")
                country_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_location FROM location")
                location_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_weather FROM weather_description")
                weather_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, date_values FROM date_dim")
                date_map = {row[1]: row[0] for row in cursor.fetchall()}

<<<<<<< Updated upstream
                # Fetch raw data từ staging
=======
                # Buoc 6
                write_log_to_db("INFO", "Fetching data from staging table.", "Changing to Fact")
                # Buoc 7
>>>>>>> Stashed changes
                cursor.execute("""
                    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time
                    FROM staging
                """)
                staging_data = cursor.fetchall()
                # Buoc 8
                write_log_to_db("INFO", f"Đã lấy thành công {len(staging_data)} dòng dữ liệu từ bảng staging.", "Changing to Fact")

<<<<<<< Updated upstream
=======

>>>>>>> Stashed changes
                fact_inserts = []
                # Buoc 9
                for record in staging_data:
                    nation, temperature, weather_status, location, record_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)

<<<<<<< Updated upstream
                    # Kiểm tra hoặc thêm dữ liệu vào bảng date_dim
                    if record_time not in date_map:
                        day = record_time.day
                        month = record_time.month
                        year = record_time.year
=======
                    # Buoc 9.1
                    if current_time not in date_map:
                        # Buoc 9.2.2
                        day = current_time.day
                        month = current_time.month
                        year = current_time.year
                        # Buoc 9.2.3
>>>>>>> Stashed changes
                        cursor.execute(
                            "INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)",
                            (record_time, day, month, year)
                        )
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
<<<<<<< Updated upstream
                        date_map[record_time] = date_id
                    else:
                        date_id = date_map[record_time]

                    # Thêm bản ghi vào fact table
                    if country_id and location_id and weather_id:
                        fact_inserts.append((
                            country_id, location_id, weather_id, date_id, latest_report, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ))

                if fact_inserts:
=======
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
                    # Buoc 9.4.2
>>>>>>> Stashed changes
                    insert_query = """
                        INSERT INTO fact_table (
                            country_id, location_id, weather_id, date_id, report_id, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, fact_inserts)
                    conn.commit()
<<<<<<< Updated upstream
                    print(f"Đã thêm {len(fact_inserts)} bản ghi vào bảng fact")
                    write_log_to_db("SUCCESS", f"Đã thêm {len(fact_inserts)} bản ghi vào bảng fact.", "Changing to Fact")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi trong quá trình biến đổi: {e}", "Changing to Fact")
        send_email("[ERROR] Changing to Fact", f"Lỗi xuất hiện: {e}\nThời gian: {current_time}")
=======
                    # Buoc 9.4.3
                    write_log_to_db("SUCCESS", f"Thành công thêm {len(fact_inserts)} vào bảng fact.", "Changing to Fact")
                else:
                    # Buoc 9.4.4
                    write_log_to_db("INFO", "Khong có dự liệu hợp lệ để thêm vào bảng fact.", "Changing to Fact")
    # Buoc 10
    except Exception as e:
        # Buoc 10.1
        write_log_to_db("ERROR", f"Error during staging to fact transformation: {e}", "Changing to Fact")
>>>>>>> Stashed changes

# Điểm khởi chạy
transform_staging_to_fact()
