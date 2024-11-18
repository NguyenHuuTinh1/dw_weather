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

# các def biến đổi dữ liệu
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

# câu query để chèn weather vào staing
def get_data_from_csv(filepath):
    """Trích xuất và định dạng dữ liệu từ CSV để chèn vào MySQL."""
    data = []
    with open(filepath, mode='r', encoding='utf-8', errors='ignore') as file:
        reader = csv.reader(file)
        for row in reader:
            current_time = parse_datetime(row[4])
            latest_report = parse_datetime(row[5])
            dead_time = parse_datetime(row[10])

            data.append((
                row[0],  # nation
                parse_float(row[1]),  # temperature
                row[2],  # weather_status
                row[3],  # location
                current_time,  # current_time
                latest_report,  # latest_report
                parse_float(row[6]),  # visibility
                parse_float(row[7]),  # pressure
                parse_int(row[8]),  # humidity
                parse_float(row[9]),  # dew_point
                dead_time  # dead_time
            ))
            # Print parsed data for debugging
            print(f"Row data: nation={row[0]}, temperature={row[1]}, weather_status={row[2]}, location={row[3]}, "
                  f"current_time={current_time}, latest_report={latest_report}, visibility={row[6]}, "
                  f"pressure={row[7]}, humidity={row[8]}, dew_point={row[9]}, dead_time={dead_time}")
    return data
# chèn dữ liệu weather được cào về để load vào staging
def insert_data_weather_in_DB():
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
            sql_query = """INSERT INTO staging 
                          (nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            data = get_data_from_csv(csv_file_path)

            cursor.executemany(sql_query, data)
            connection.commit()
            print("Dữ liệu Weather đã được chèn thành công!")
    except Exception as e:
        print(f"Lỗi khi chèn dữ liệu: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
            print("Đã đóng kết nối MySQL")

insert_data_weather_in_DB()


