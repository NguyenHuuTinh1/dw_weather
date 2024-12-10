import csv
import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Các biến
email = ''
password = ''
email_sent = ''
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Hàm chuyển đổi kiểu dữ liệu
def parse_float(value):
    """Chuyển đổi giá trị thành float hoặc None nếu giá trị không hợp lệ."""
    try:
        return float(value) if value != 'N/A' else None
    except ValueError:
        return None
def parse_int(value):
    """Chuyển đổi giá trị thành int hoặc None nếu giá trị không hợp lệ."""
    try:
        return int(value) if value != 'N/A' else None
    except ValueError:
        return None
def parse_datetime(value):
    """Chuyển chuỗi ngày thành kiểu DATETIME của MySQL hoặc None nếu không hợp lệ."""
    if not value or value.strip() == "":  # Kiểm tra giá trị rỗng
        return None
    formats = [
        '%d %b %Y, %H:%M:%S',  # Định dạng đầy đủ
        '%d %b %Y, %H:%M',     # Thiếu giây
        '%Y-%m-%d %H:%M:%S',   # Định dạng khác đầy đủ
        '%Y-%m-%d %H:%M',      # Thiếu giây
        '%d/%m/%Y %H:%M:%S',   # Định dạng ngày/tháng/năm đầy đủ
        '%d/%m/%Y %H:%M',      # Thiếu giây
        '%d/%m/%Y'             # Chỉ ngày/tháng/năm
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)  # Trả về datetime đầy đủ
        except ValueError:
            continue

    print(f"Could not parse date: {value}")  # Debug thông tin nếu không parse được
    return None
def convert_time_to_period(time):
    """Chuyển đổi giờ trong ngày sang sáng/chiều/tối/đêm."""
    if time is None:
        return None
    hour = time.hour
    if 5 <= hour < 12:
        return "SÁNG"
    elif 12 <= hour < 18:
        return "CHIỀU"
    elif 18 <= hour < 22:
        return "TỐI"
    else:
        return "ĐÊM"

# Đọc file chứa thông tin cấu hình cơ sở dữ liệu
def CrawInformationDB():
    lines = []
    try:
        with open(r"D:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
            for line in file:
                lines.append(line.strip())
        return lines
    except Exception as e:
        return []

# Lấy thông tin địa chỉ lưu file CSV
def select_location_file_csv():
    lines = CrawInformationDB()
    if not lines:
        write_log_to_db("ERROR", "Không lấy được thông tin cấu hình từ file connect_db.txt", "Loading data in Staging")
        return None
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],  # Đảm bảo mật khẩu đúng
            database=lines[3]
        )
        write_log_to_db("SUCCESS", "Kết nối database thành công.", "Loading data in Staging")
        if connection.open:
            cursor = connection.cursor()
            sql_query = """SELECT location FROM control_data_config LIMIT 1"""
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                first_value = result[0]  # Trích xuất giá trị đầu tiên từ tuple
                write_log_to_db("SUCCESS", f"Lấy thông tin location file csv từ database: {first_value}", "Loading data in Staging")
                return first_value
            else:
                write_log_to_db("ERROR", "Không có dữ liệu trả về từ bảng control_data_config.", "Loading data in Staging")
                return None
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi truy vấn database: {e}", "Loading data in Staging")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

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


# Phương thức ghi log
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
            print("Log đã được ghi thành công!")
    except Exception as e:
        print(f"Lỗi khi ghi log: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

# Đọc dữ liệu từ file CSV
def get_data_from_csv(filepath):
    """Trích xuất và định dạng dữ liệu từ CSV để chèn vào MySQL."""
    data = []
    try:
        with open(filepath, mode='r', encoding='utf-8', errors='ignore') as file:
            reader = csv.reader(file)
            for row in reader:
                current_time = parse_datetime(row[4]).date()  # Chỉ lấy ngày
                latest_report_time = parse_datetime(row[5])  # Đầy đủ datetime
                latest_report = convert_time_to_period(latest_report_time)  # Sáng/Chiều/Tối
                dead_time = parse_datetime(row[10]).date()  # Chỉ lấy ngày

                data.append((
                    row[0],  # nation
                    parse_float(row[1]),  # temperature
                    row[2],  # weather_status
                    row[3],  # location
                    current_time,  # Chỉ ngày
                    latest_report,  # Sáng/Chiều/Tối/Đêm
                    parse_float(row[6]),  # visibility
                    parse_float(row[7]),  # pressure
                    parse_int(row[8]),  # humidity
                    parse_float(row[9]),  # dew_point
                    dead_time  # Chỉ ngày
                ))
                # Debug thông tin
                print("INFO", f"Row data: {row[0]}, {row[3]} - current_time={current_time}, latest_report={latest_report}", "Loading data in Staging")
        write_log_to_db("SUCCESS", "Dữ liệu CSV đã được trích xuất thành công.", "Loading data in Staging")
        return data
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi đọc dữ liệu CSV: {e}", "Loading data in Staging")
        return []

# Chèn dữ liệu weather vào bảng staging
def insert_data_weather_in_DB():
    lines = CrawInformationDB()
    csv_file_path = select_location_file_csv()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],  # Đảm bảo mật khẩu đúng
            database=lines[3]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối database thành công.", "Loading data in Staging")
            cursor = connection.cursor()
            sql_query = """INSERT INTO staging 
                          (nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            data = get_data_from_csv(csv_file_path)
            if data:
                cursor.executemany(sql_query, data)
                connection.commit()
                write_log_to_db("SUCCESS", "Dữ liệu Weather đã được chèn thành công!", "Loading data in Staging")
            else:
                write_log_to_db("ERROR", "Không có dữ liệu để chèn.", "Loading data in Staging")
                send_email("[ERROR] Loading data in Staging",f"Không có dữ liệu để chèn. \n Lỗi xuất hiện vào lúc {current_time}")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu: {e}", "Loading data in Staging")
        send_email("[ERROR] Loading data in Staging",f"Lỗi khi chèn dữ liệu: {e} \n Lỗi xuất hiện vào lúc {current_time}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL.", "Loading data in Staging")

# Gọi hàm thực thi
insert_data_weather_in_DB()
