import bs4
import requests
import csv
import pymysql
from datetime import datetime
import os

# Khởi tạo các biến
list_temperature = []
list_status = []

list_title = 'Nation, Temperature, Weather status, Location, Current Time, Latest Report, Visibility, Pressure, Humidity, Dew Point, Dead Time'
list_title = list_title.split(', ')

# Biến toàn cục
url_main_web = ''
url_web = ''
mainFilePath = ''


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
            sql_query = """SELECT url_main_web, url_web, location FROM control_data_config LIMIT 1"""
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


# Gắn các giá trị trả về từ database vào biến toàn cục
def set_values():
    global url_main_web, url_web, mainFilePath

    control_info = select_data_control_file_config()

    if control_info:
        url_main_web, url_web, mainFilePath = control_info
        print(f"url_main_web: {url_main_web}")
        print(f"url_web: {url_web}")
        print(f"location: {mainFilePath}")
    else:
        write_log_to_db("ERROR", "Không thể gán giá trị từ database", "Craw data")


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


# Lấy nội dung trang web
def GetPageContent(url):
    try:
        page = requests.get(url, headers={"Accept-Language": "en-US"})
        return bs4.BeautifulSoup(page.text, "html.parser")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi lấy nội dung từ URL {url}: {e}", "Craw data")
        return None


# Lấy danh sách link từ trang chủ
def CrawLinkCountry(url):
    soup = GetPageContent(url)
    if not soup:
        return []

    try:
        table = soup.find("table", class_="zebra fw tb-theme")
        list_link_country_web = []
        if table:
            for table_row in table.find_all('tr'):
                columns = table_row.find_all('a')
                for column in columns:
                    link = column['href']
                    list_link_country_web.append(link)
        return list_link_country_web
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi lấy link quốc gia từ URL {url}: {e}", "Craw data")
        return []


# Phương thức lấy ra tên các quốc gia từ liên kết trên trang chủ
def CrawCountry(url):
    try:
        list_country = []
        soup = GetPageContent(url)

        # Kiểm tra nếu không lấy được nội dung trang
        if not soup:
            write_log_to_db("ERROR", f"Không thể truy cập nội dung trang: {url}", "Craw data")
            return []

        table = soup.find("table", class_="zebra fw tb-theme")

        # Kiểm tra nếu không tìm thấy bảng dữ liệu
        if not table:
            write_log_to_db("WARNING", f"Không tìm thấy bảng dữ liệu trên trang: {url}", "Craw data")
            return []

        for table_row in table.find_all('tr'):
            columns = table_row.find_all('a')
            for column in columns:
                country = column.text
                # Tách lấy phần tên quốc gia (trước dấu phẩy nếu có)
                first_part = country.split(",")[0]
                list_country.append(first_part)

        # Ghi log thành công
        write_log_to_db("SUCCESS", "Lấy danh sách quốc gia thành công", "Craw data")
        return list_country
    except Exception as e:
        # Ghi log lỗi nếu xảy ra ngoại lệ
        write_log_to_db("ERROR", f"Lỗi trong phương thức CrawCountry: {e}", "Craw data")
        return []


# Lấy thông tin bổ sung
def CrawInformation(url):
    list_link_country_web = CrawLinkCountry(url_main_web)
    if not list_link_country_web:
        write_log_to_db("ERROR", "Không thể lấy danh sách quốc gia", "Craw data")
        return

    for country in list_link_country_web:
        try:
            url_country = url + country
            soup = GetPageContent(url_country)
            if not soup:
                continue
            qlook_div = soup.find('div', id='qlook')
            if qlook_div:
                h2_tag = qlook_div.find('div', class_='h2')
                p_tag = qlook_div.find('p')
                if h2_tag and p_tag:
                    list_temperature.append(h2_tag.text.strip().rstrip('°C').split()[0])
                    list_status.append(p_tag.text.strip())
            else:
                list_temperature.append("No data available")
                list_status.append("No p tag found")
        except Exception as e:
            write_log_to_db("ERROR", f"Lỗi khi lấy thông tin thời tiết từ URL {url_country}: {e}", "Craw data")

# Phương thức lấy ra thông tin chính về thời tiết của các link và kết hợp với CrawInformation
def CrawDetailedInformation(url):
    try:
        # Lấy danh sách link và quốc gia
        list_link_country_web = CrawLinkCountry(url_main_web)
        list_country = CrawCountry(url_main_web)
        if not list_link_country_web or not list_country:
            write_log_to_db("ERROR", "Không thể lấy danh sách link quốc gia hoặc tên quốc gia", "Craw data")
            return []

        # Lấy thông tin bổ sung về thời tiết
        CrawInformation(url_web)

        list_data_country_weather = []
        i = 0

        for country in list_link_country_web:
            try:
                # Truy cập URL của từng quốc gia
                url_country = url + country
                soup = GetPageContent(url_country)
                if not soup:
                    write_log_to_db("ERROR", f"Không thể lấy nội dung từ URL: {url_country}", "Craw data")
                    continue

                title = soup.find('div', class_="bk-focus__qlook")
                table = soup.find("table", class_="table table--left table--inner-borders-rows")

                # Nếu không tìm thấy bảng, ghi log và bỏ qua quốc gia này
                if not table:
                    write_log_to_db("WARNING", f"Không tìm thấy dữ liệu bảng tại URL: {url_country}", "Craw data")
                    continue

                output_row = []
                output_row.append(list_country[i])  # Tên quốc gia
                output_row.append(list_temperature[i])  # Nhiệt độ
                output_row.append(list_status[i])  # Trạng thái thời tiết

                # Lấy dữ liệu chi tiết từ bảng
                j = 0
                for table_row in table.find_all('tr'):
                    list_td = table_row.find_all('td')
                    for td in list_td:
                        content = td.text.strip()
                        if j > 2:
                            content = td.text.strip().split()[0]  # Lấy phần tử đầu tiên nếu cần
                        if j == 5:
                            content = td.text.rstrip('%')  # Loại bỏ ký tự '%'
                        output_row.append(content)
                        j += 1

                # Thêm dữ liệu thời gian giả lập
                output_row.append("24/12/2999")
                list_data_country_weather.append(output_row)
                print(f"Dữ liệu quốc gia {list_country[i]}: {output_row}")
            except Exception as e:
                write_log_to_db("ERROR", f"Lỗi khi xử lý dữ liệu quốc gia {list_country[i]}: {e}", "Craw data")
            i += 1

        write_log_to_db("SUCCESS", "Hoàn tất crawl thông tin chi tiết thời tiết", "Craw data")
        return list_data_country_weather
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi trong phương thức CrawDetailedInformation: {e}", "Craw data")
        return []
# Insert data into date_dim table
def InsertDateDim():
    lines = CrawInformationDB()
    if not lines:
        print("Không thể ghi vào date_dim do không có thông tin kết nối database.")
        return

    try:
        # Connect to the database
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )
        if connection.open:
            cursor = connection.cursor()

            # Get the current date values
            current_date = datetime.now()
            date_values = current_date.strftime('%Y-%m-%d %H:%M:%S')  # Store the full datetime value
            day = current_date.day  # Extract integer day
            month = current_date.month  # Extract integer month
            year = current_date.year  # Extract integer year

            # Insert the record into the date_dim table
            sql_query = """INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)"""
            data = (date_values, day, month, year)
            cursor.execute(sql_query, data)
            connection.commit()
            print("Dữ liệu đã được thêm vào bảng date_dim thành công!")
            write_log_to_db("SUCCESS", "Dữ liệu được thêm vào bảng date_dim thành công", "Craw data")
    except Exception as e:
        print(f"Lỗi khi thêm dữ liệu vào bảng date_dim: {e}")
        write_log_to_db("ERROR", f"Lỗi khi thêm dữ liệu vào bảng date_dim: {e}", "Craw data")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()

# Xuất dữ liệu ra file CSV
def ExportCsv(data, filePath):
    import os

    try:
        # Kiểm tra nếu filePath rỗng
        if not filePath:
            raise ValueError("Đường dẫn file (filePath) không được để trống")

        # Tạo thư mục nếu cần
        directory = os.path.dirname(filePath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)

        # Kiểm tra file đã tồn tại
        file_exists = os.path.isfile(filePath)

        # Mở file để ghi thêm dữ liệu
        with open(filePath, 'a' if file_exists else 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            # Thêm tiêu đề nếu file mới
            if not file_exists:
                csv_writer.writerow(list_title)  # list_title chứa tiêu đề các cột

            # Ghi dữ liệu
            csv_writer.writerows(data)

        write_log_to_db("SUCCESS", f"Dữ liệu được thêm vào file CSV thành công: {filePath}", "Craw data")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi thêm dữ liệu vào file CSV: {e}", "Craw data")


# Quá trình chính
def CrawData():
    set_values()  # Lấy cấu hình
    if not url_web or not url_main_web or not mainFilePath:
        write_log_to_db("ERROR", "URL hoặc đường dẫn file không hợp lệ", "Craw data")
        return

    try:
        InsertDateDim()
        data = CrawDetailedInformation(url_web)
        ExportCsv(data, mainFilePath)
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi crawl data: {e}", "Craw data")

CrawData()
