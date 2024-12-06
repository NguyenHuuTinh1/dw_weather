import csv
import pymysql
from datetime import datetime

# def biến đổi dữ liệu
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
    # Danh sách các định dạng ngày giờ có thể có trong CSV
    formats = [
        '%d %b %Y, %H:%M:%S',  # Định dạng có đủ ngày giờ phút giây
        '%d %b %Y, %H:%M',  # Định dạng thiếu giây
        '%Y-%m-%d %H:%M:%S',  # Định dạng khác có đủ ngày giờ phút giây
        '%Y-%m-%d %H:%M',  # Định dạng khác thiếu giây
        '%d/%m/%Y %H:%M:%S',  # Định dạng ngày/tháng/năm đủ ngày giờ phút giây
        '%d/%m/%Y %H:%M',  # Định dạng ngày/tháng/năm thiếu giây
        '%d/%m/%Y'  # Chỉ có ngày/tháng/năm, không có giờ
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    print(f"Could not parse date: {value}")
    return None  # Trả về None nếu không thể phân tích cú pháp

# Đọc file chứa thông tin cấu hình cơ sở dữ liệu
def CrawInformationDB():
    lines = []
    try:
        with open(r"E:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
            for line in file:
                lines.append(line.strip())
        return lines
    except Exception as e:
        print(f"Lỗi đọc file cấu hình: {e}")
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
            password=lines[2],  # Đảm bảo mật khẩu đúng
            database=lines[3]
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
            password=lines[2],
            database=lines[3]
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

# đọc lấy dữ liệu từ cột location
def get_data_location_from_csv(filepath):
    data = set()  # Sử dụng tập hợp (set) để lưu trữ các địa điểm duy nhất
    with open(filepath, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.reader(file)
        next(reader)  # Bỏ qua tiêu đề
        for row in reader:
            location = row[3]
            data.add(location)  # Thêm địa điểm vào tập hợp
            print(f"Dữ liệu dòng: location={location}")
    return list(data)  # Chuyển tập hợp thành danh sách nếu cần

# đọc lấy dữ liệu từ cột country
def get_data_country_from_csv(filepath):
    data = set()  # Sử dụng tập hợp (set) để lưu trữ các địa điểm duy nhất
    with open(filepath, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.reader(file)
        next(reader)  # Bỏ qua tiêu đề
        for row in reader:
            country = row[0]
            data.add(country)  # Thêm địa điểm vào tập hợp
            print(f"Dữ liệu dòng: country={country}")
    return list(data)  # Chuyển tập hợp thành danh sách nếu cần

# đọc lấy dữ liệu từ cột weather_description
def get_data_weather_description_from_csv(filepath):
    data = set()  # Sử dụng tập hợp (set) để lưu trữ các địa điểm duy nhất
    with open(filepath, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.reader(file)
        next(reader)  # Bỏ qua tiêu đề
        for row in reader:
            weather = row[2]
            data.add(weather)  # Thêm địa điểm vào tập hợp
            print(f"Dữ liệu dòng: weather={weather}")
    return list(data)  # Chuyển tập hợp thành danh sách nếu cần

# -----------------------------------------------------------------
# Chèn dữ liệu location để làm bảng dim
def insert_data_location_in_DB():
    lines = CrawInformationDB()
    csv_file_path = select_location_file_csv()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối thành công đến MySQL cho bảng location")
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO location (values_location) VALUES (%s)"""

            data = get_data_location_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            write_log_to_db("SUCCESS", "Dữ liệu location đã được chèn thành công!")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu location: {e}")
        print(f"Lỗi khi chèn dữ liệu: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL")
            print("Đã đóng kết nối MySQL")

# Chèn dữ liệu country để làm bảng dim
def insert_data_country_in_DB():
    lines = CrawInformationDB()
    csv_file_path = select_location_file_csv()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối thành công đến MySQL cho bảng country")
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO country (values_country) VALUES (%s)"""

            data = get_data_country_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            write_log_to_db("SUCCESS", "Dữ liệu country đã được chèn thành công!")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu country: {e}")
        print(f"Lỗi khi chèn dữ liệu: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL")
            print("Đã đóng kết nối MySQL")

# Chèn dữ liệu weather_description để làm bảng dim
def insert_data_weather_description_in_DB():
    lines = CrawInformationDB()
    csv_file_path = select_location_file_csv()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối thành công đến MySQL cho bảng weather_description")
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO weather_description (values_weather) VALUES (%s)"""

            data = get_data_weather_description_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            write_log_to_db("SUCCESS", "Dữ liệu weather_description đã được chèn thành công!")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu weather_description: {e}")
        print(f"Lỗi khi chèn dữ liệu: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL")
            print("Đã đóng kết nối MySQL")

# Chèn dữ liệu latesReport để làm bảng dim
def insert_data_late_report_in_DB():
    lines = CrawInformationDB()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        if connection.open:
            write_log_to_db("SUCCESS", "Kết nối thành công đến MySQL cho bảng latesReport")
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO latesReport (time) VALUES (%s)"""

            data = ["SÁNG", "CHIỀU", "TỐI", "ĐÊM"]

            cursor.executemany(sql_query, data)
            connection.commit()
            write_log_to_db("SUCCESS", "Dữ liệu latesReport đã được chèn thành công!")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi chèn dữ liệu latesReport: {e}")
        print(f"Lỗi khi chèn dữ liệu: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            write_log_to_db("SUCCESS", "Đã đóng kết nối MySQL")
            print("Đã đóng kết nối MySQL")
# ----------------------------------------------------------------
# Hàm insert gọi tất cả các hàm chèn dữ liệu
def insert():
    try:
        write_log_to_db("INFO", "Bắt đầu tạo các bảng Dim")
        insert_data_location_in_DB()
        insert_data_country_in_DB()
        insert_data_weather_description_in_DB()
        insert_data_late_report_in_DB()
        write_log_to_db("SUCCESS", "Quá trình tạo các bảng Dim thành công")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi trong quá trình insert: {e}")
        print(f"Lỗi trong quá trình insert: {e}")
insert()
