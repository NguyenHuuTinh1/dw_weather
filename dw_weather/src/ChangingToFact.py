import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Phương thức đọc cấu hình từ file
def CrawInformationDB():
    """
    Đọc cấu hình từ file và trả về danh sách các giá trị.
    Số lượng giá trị trả về tùy thuộc vào nội dung file.
    """
    try:
        with open(r"D:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
            return [line.strip() for line in file]
    except Exception as e:
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
                cursor.execute("SELECT id, values_country FROM country")
                country_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_location FROM location")
                location_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_weather FROM weather_description")
                weather_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, date_values FROM date_dim")
                date_map = {row[1]: row[0] for row in cursor.fetchall()}

                # Fetch raw data từ staging
                cursor.execute("""
                    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time
                    FROM staging
                """)
                staging_data = cursor.fetchall()

                fact_inserts = []
                for record in staging_data:
                    nation, temperature, weather_status, location, record_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)

                    # Kiểm tra hoặc thêm dữ liệu vào bảng date_dim
                    if record_time not in date_map:
                        day = record_time.day
                        month = record_time.month
                        year = record_time.year
                        cursor.execute(
                            "INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)",
                            (record_time, day, month, year)
                        )
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
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
                    insert_query = """
                        INSERT INTO fact_table (
                            country_id, location_id, weather_id, date_id, report_id, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, fact_inserts)
                    conn.commit()
                    print(f"Đã thêm {len(fact_inserts)} bản ghi vào bảng fact")
                    write_log_to_db("SUCCESS", f"Đã thêm {len(fact_inserts)} bản ghi vào bảng fact.", "Changing to Fact")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi trong quá trình biến đổi: {e}", "Changing to Fact")
        send_email("[ERROR] Changing to Fact", f"Lỗi xuất hiện: {e}\nThời gian: {current_time}")

# Điểm khởi chạy
transform_staging_to_fact()
