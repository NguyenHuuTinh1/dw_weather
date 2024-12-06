import csv
import pymysql
from datetime import datetime

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
        write_log_to_db("ERROR", "Không lấy được thông tin cấu hình từ file connect_db.txt")
        return None
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',  # Đảm bảo mật khẩu đúng
            database=lines[2]
        )
        write_log_to_db("SUCCESS", "Kết nối database thành công.")
        if connection.open:
            cursor = connection.cursor()
            sql_query = """SELECT location FROM control_data_config LIMIT 1"""
            cursor.execute(sql_query)
            result = cursor.fetchone()
            if result:
                first_value = result[0]  # Trích xuất giá trị đầu tiên từ tuple
                write_log_to_db("SUCCESS", f"Lấy thông tin location file csv từ database: {first_value}")
                return first_value
            else:
                write_log_to_db("ERROR", "Không có dữ liệu trả về từ bảng control_data_config.")
                return None
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi truy vấn database: {e}")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

# Phương thức ghi log
def write_log_to_db(status, note, log_date=None):
    lines = CrawInformationDB()
    if not lines:
        print("Không thể ghi log do không có thông tin kết nối database.")
        return

    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',
            database=lines[2]
        )
        if connection.open:
            cursor = connection.cursor()
            sql_query = """INSERT INTO log (status, note, log_date) VALUES (%s, %s, %s)"""
            data = (status, note, log_date if log_date else datetime.now())
            cursor.execute(sql_query, data)
            connection.commit()
            print(f"Log đã được ghi thành công! - {status}: {note}")
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
                write_log_to_db("INFO", f"Row data: {row[0]}, {row[3]} - current_time={current_time}, latest_report={latest_report}")
        write_log_to_db("SUCCESS", "Dữ liệu CSV đã được trích xuất thành công.")
        return data
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi đọc dữ liệu CSV: {e}")
        return []

# Chèn dữ liệu weather vào bảng staging
def insert_data_weather_in_DB():
    lines = CrawInformationDB()
    csv_file_path = select_location_file_csv()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',  # Đảm bảo mật khẩu đúng
            database=lines[2]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối database thành công.")
            cursor = connection.cursor()
            sql_query = """INSERT INTO staging 
                          (nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            data = get_data_from_csv(csv_file_path)
            if data:
                cursor.executemany(sql_query, data)
                connection.commit()
                write_log_to_db("SUCCESS", "Dữ liệu Weather đã được chèn thành công!")
            else:
                write_log_to_db("ERROR", "Không có dữ liệu để chèn.")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL.")

# Gọi hàm thực thi
insert_data_weather_in_DB()
