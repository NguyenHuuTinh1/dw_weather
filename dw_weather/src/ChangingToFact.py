import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


email = ''
password = ''
email_sent = ''
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def CrawInformationDB():
    lines = []
    try:
        with open(r"D:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
            for line in file:
                lines.append(line.strip())
        return lines
    except Exception as e:
        print(f"Lỗi đọc file cấu hình: {e}")
        return []
# Phương thức gửi gmail report
def send_email(subject, body):
    set_values();
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()  # Enable security
    session.login(email, password)

    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = email_sent
    msg['Subject'] = subject  # Correct placement of the subject

    # Email body
    body = body
    msg.attach(MIMEText(body, 'plain'))
    session.sendmail(email, email_sent, msg.as_string())
    session.quit()  # Close the session
    print('Email sent!')

#Gắn các giá trị trả về từ database vào biến toàn cục
def set_values():
    # B1.1 Tạo biến
    global email, password, email_sent
    # B1.2 Kết nối đến cơ sở dữ liệu và lấy thông tin cấu hình
    control_info = select_data_control_file_config()
    # B1.3 kiểm tra
    if control_info:
        email, password, email_sent = control_info
    else:
        write_log_to_db("ERROR", "Không thể gán giá trị từ database", "Craw data")

# Kết nối đến cơ sở dữ liệu và lấy thông tin cấu hình
def select_data_control_file_config():
    lines = CrawInformationDB()
    if not lines:
        write_log_to_db("ERROR", "Không lấy được thông tin cấu hình từ file connect_db.txt", "Craw data")
        return None

    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],  # Đảm bảo mật khẩu đúng
            database=lines[3]
        )

        if connection.open:
            cursor = connection.cursor()
            sql_query = """SELECT email_report, pass_email, email_sent FROM control_data_config LIMIT 1"""
            cursor.execute(sql_query)
            result = cursor.fetchone()  # Lấy 1 dòng dữ liệu từ kết quả truy vấn
            if result:
                write_log_to_db("SUCCESS", "Lấy thông tin cấu hình từ database thành công", "Craw data")
                return result
            else:
                write_log_to_db("ERROR", "Không có dữ liệu trả về từ bảng control_data_config", "Craw data")
                return None
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi truy vấn dữ liệu cấu hình: {e}", "Craw data")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

# Method to write logs to the database
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
    db_credentials = CrawInformationDB()
    if not db_credentials:
        write_log_to_db("ERROR", "Missing database credentials for staging to fact transformation.", "Changing to Fact")
        send_email("[ERROR] Changing to Fact", "Missing database credentials for staging to fact transformation. \n Lỗi xuất hiện vào lúc {current_time}")
        return

    try:
        with DatabaseConnection(*db_credentials) as conn:
            with conn.cursor() as cursor:

                # Fetch mappings from lookup tables
                write_log_to_db("INFO", "Fetching lookup table data.", "Changing to Fact")
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

                # Fetch raw data from the staging table
                write_log_to_db("INFO", "Fetching data from staging table.", "Changing to Fact")
                cursor.execute("""
                    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time
                    FROM staging
                """)
                staging_data = cursor.fetchall()

                write_log_to_db("INFO", f"Fetched {len(staging_data)} records from staging table.", "Changing to Fact")

                # Transform data and prepare for fact table insertion
                fact_inserts = []
                for record in staging_data:
                    nation, temperature, weather_status, location, current_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)
                    report_id = latest_report_map.get(latest_report)

                    # Check or insert into date_dim
                    if current_time not in date_map:
                        day = current_time.day
                        month = current_time.month
                        year = current_time.year
                        cursor.execute(
                            "INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)",
                            (current_time, day, month, year)
                        )
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
                        date_map[current_time] = date_id
                    else:
                        date_id = date_map[current_time]

                    # Verify mappings
                    if country_id and location_id and weather_id and report_id and date_id:
                        fact_inserts.append((country_id, location_id, weather_id, date_id, report_id,
                                             temperature, visibility, pressure, humidity, dew_point, dead_time))
                    else:
                        write_log_to_db("WARNING", f"Skipping record due to missing mapping: {record}", "Changing to Fact")

                # Insert transformed data into the fact table
                if fact_inserts:
                    write_log_to_db("INFO", "Inserting data into the fact table.", "Changing to Fact")
                    insert_query = """
                        INSERT INTO fact_table (
                            country_id, location_id, weather_id, date_id, report_id, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, fact_inserts)
                    conn.commit()
                    write_log_to_db("SUCCESS", f"Inserted {len(fact_inserts)} records into fact table.", "Changing to Fact")
                else:
                    write_log_to_db("INFO", "No valid data to insert into fact table.", "Changing to Fact")

    except Exception as e:
        write_log_to_db("ERROR", f"Error during staging to fact transformation: {e}", "Changing to Fact")
        send_email("[ERROR] Changing to Fact",f"Error during staging to fact transformation: {e} \n Lỗi xuất hiện vào lúc {current_time}")


# Entry point
transform_staging_to_fact()
