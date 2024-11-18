import csv
import pymysql
from datetime import datetime

# lấy thông tin địa chỉ lưu file csv từ db control --> chưa code xong
csv_file_path = r"/src/output.csv"

# đọc file để lấy thông tin connect database
def CrawInformationDB():
    lines = []
    with open(r"/src/connect_db.txt", "r", encoding="utf-8") as file:
        for line in file:
            lines.append(line.strip())
    return lines

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

# câu query để chèn location
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

# câu query để chèn country
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

# câu query để chèn weather_description
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
# chèn dữ liệu location để làm bảng dim
def insert_data_location_in_DB():
    lines = CrawInformationDB()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',  # Đảm bảo mật khẩu đúng
            database=lines[2]
        )

        if connection.open:
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO location (values_location) VALUES (%s)"""

            data = get_data_location_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            print("Dữ liệu Location đã được chèn thành công!")
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Đã đóng kết nối MySQL")

# chèn dữ liệu country để làm bảng dim
def insert_data_country_in_DB():
    lines = CrawInformationDB()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',  # Đảm bảo mật khẩu đúng
            database=lines[2]
        )

        if connection.open:
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO country (values_country) VALUES (%s)"""

            data = get_data_country_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            print("Dữ liệu country đã được chèn thành công!")
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Đã đóng kết nối MySQL")

# chèn dữ liệu weather_description để làm bảng dim
def insert_data_weather_description_in_DB():
    lines = CrawInformationDB()
    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password='',  # Đảm bảo mật khẩu đúng
            database=lines[2]
        )

        if connection.open:
            print("Kết nối thành công đến MySQL")
            cursor = connection.cursor()
            sql_query = """INSERT INTO weather_description (values_weather) VALUES (%s)"""

            data = get_data_weather_description_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            print("Dữ liệu weather_description đã được chèn thành công!")
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Đã đóng kết nối MySQL")

# -----------------------------------------------------------------
def insert():
    insert_data_location_in_DB()
    insert_data_country_in_DB()
    insert_data_weather_description_in_DB()

insert()
