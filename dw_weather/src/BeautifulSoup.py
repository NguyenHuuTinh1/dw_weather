import bs4
import requests
import csv
import pymysql
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Khởi tạo các biến
list_temperature = []
list_status = []

# Biến toàn cục
url_main_web = ''
url_web = ''
mainFilePath = ''
email = ''
password = ''
email_sent = ''


current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Đọc file chứa thông tin cấu hình cơ sở dữ liệu
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


# Kết nối đến cơ sở dữ liệu và lấy thông tin cấu hình (GetControlDataConfig)
def select_data_control_file_config():
    lines = CrawInformationDB()
    if not lines:
        write_log_to_db("ERROR", "Không lấy được thông tin cấu hình từ file connect_db.txt", "Craw data")
        return None

    try:
        connection = pymysql.connect(
            host=lines[0],
            user=lines[1],
            password=lines[2],
            database=lines[3]
        )

        if connection.open:
            cursor = connection.cursor()
            procedure_name = "GetControlDataConfig"  # Thay 'your_procedure_name' bằng tên thủ tục thực tế
            cursor.callproc(procedure_name)
            result = cursor.fetchone()
            if result:
                write_log_to_db("SUCCESS", "Lấy thông tin cấu hình từ database thành công", "Craw data")
                return result
            else:
                write_log_to_db("ERROR", "Không có dữ liệu trả về từ bảng control_data_config", "Craw data")
                return None
    except Exception as e:
        error_message = f"Lỗi truy vấn dữ liệu cấu hình: {e}"
        write_log_to_db("ERROR", error_message, "Craw data")
        try:
            # Nếu xảy ra lỗi, gửi email thông báo
            subject = "Lỗi kết nối cơ sở dữ liệu"
            body = f"Không kết nối được đến cơ sở dữ liệu.\n\nChi tiết lỗi: {error_message}"
            # Các thông tin email cần lấy từ `lines`
            main_email = lines[4]
            main_pass = lines[5]
            main_email_sent = lines[6]
            send_email_main(subject, body, main_email, main_pass, main_email_sent)
        except Exception as email_error:
            write_log_to_db("ERROR", f"Lỗi khi gửi email thông báo: {email_error}", "Craw data")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()


# Gắn các giá trị trả về từ database vào biến toàn cục
def set_values():
    # 1.1
    global url_main_web, url_web, mainFilePath, email, password, email_sent
    # 1.2
    control_info = select_data_control_file_config()
    # 1.3
    if control_info:
        # 1.4
        url_main_web, url_web, mainFilePath, email, password, email_sent = control_info
    else:
        # 1.3.1
        write_log_to_db("ERROR", "Không thể gán giá trị từ database", "Craw data")


# Phương thức ghi log (insertLog)
def write_log_to_db(status, note, process, log_date=None):
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
            # Gọi stored procedure InsertLog
            procedure_name = "InsertLog"
            log_date = log_date if log_date else datetime.now()

            # Thực thi stored procedure với các tham số
            cursor.callproc(procedure_name, (status, note, process, log_date))

            connection.commit()
            write_log_to_db("INFO", "Log đã được ghi thành công!", "CrawData")
    except Exception as e:
        print(f"Lỗi khi ghi log: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()


# Phương thức gửi gmail report cho ds gmail trong db
def send_email(subject, body):
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_body = f"{body}\n\nThời gian gửi: {current_time}"

        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()  # Enable security
        session.login(email, password)

        msg = MIMEMultipart()
        msg['From'] = email
        msg['To'] = email_sent
        msg['Subject'] = subject

        msg.attach(MIMEText(full_body, 'plain'))
        session.sendmail(email, email_sent, msg.as_string())
        session.quit()  # Close the session
        print('Email sent!')
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi gửi email: {e}", "Craw data")


# Phương thức gửi gmail report cho email chính
def send_email_main(subject, body, main_email, main_pass, main_email_sent):
    try:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_body = f"{body}\n\nThời gian gửi: {current_time}"

        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()  # Enable security
        session.login(main_email, main_pass)

        msg = MIMEMultipart()
        msg['From'] = main_email
        msg['To'] = main_email_sent
        msg['Subject'] = subject

        msg.attach(MIMEText(full_body, 'plain'))
        session.sendmail(main_email, main_email_sent, msg.as_string())
        session.quit()  # Close the session
        print('Main email sent!')
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi gửi email chính: {e}", "Craw data")

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
    # B1
    soup = GetPageContent(url)
    # B2
    if not soup:
        # B2.1
        return []

    try:
        # B3
        table = soup.find("table", class_="zebra fw tb-theme")
        # B4
        list_link_country_web = []
        # B5
        if table:
            # B6
            for table_row in table.find_all('tr'):
                #6.1
                columns = table_row.find_all('a')
                for column in columns:
                    # B6.2
                    link = column['href']
                    # B6.3
                    list_link_country_web.append(link)
        #             B7
        return list_link_country_web

    except Exception as e:
        #8.
        write_log_to_db("ERROR", f"Lỗi khi lấy link quốc gia từ URL {url}: {e}", "Craw data")
        # b8.1
        return []


# Phương thức lấy ra tên các quốc gia từ liên kết trên trang chủ
def CrawCountry(url):
    try:
        # 1
        list_country = []
        # 2
        soup = GetPageContent(url)
        # Kiểm tra nếu không lấy được nội dung trang // 3
        if not soup:
            # 3.1
            write_log_to_db("ERROR", f"Không thể truy cập nội dung trang: {url}", "Craw data")
            # 3.2
            return []
        # 4
        table = soup.find("table", class_="zebra fw tb-theme")
        # Kiểm tra nếu không tìm thấy bảng dữ liệu
        # 5
        if not table:
            # 5.1
            write_log_to_db("WARNING", f"Không tìm thấy bảng dữ liệu trên trang: {url}", "Craw data")
            # 5.2
            return []
        # 6
        for table_row in table.find_all('tr'):
            columns = table_row.find_all('a')
            # 6.1
            for column in columns:
                # 6/2
                country = column.text
                # Tách lấy phần tên quốc gia (trước dấu phẩy nếu có) // 6.3
                first_part = country.split(",")[0]
                # 6.4
                list_country.append(first_part)
        # Ghi log thành công // 7
        write_log_to_db("SUCCESS", "Lấy danh sách quốc gia thành công", "Craw data")
        # 8
        return list_country
    except Exception as e:
        # Ghi log lỗi nếu xảy ra ngoại lệ // 9
        write_log_to_db("ERROR", f"Lỗi trong phương thức CrawCountry: {e}", "Craw data")
        # 9.1
        return []
# Lấy thông tin bổ sung
def CrawInformation(url):
    # b1
    list_link_country_web = CrawLinkCountry(url_main_web)
    # b2
    if not list_link_country_web:
        # b2.1
        write_log_to_db("ERROR", "Không thể lấy danh sách quốc gia", "Craw data")
        return
    # b3
    for country in list_link_country_web:
        try:
            # b3.1
            url_country = url + country
            # b3.2
            soup = GetPageContent(url_country)
            # b3.3
            if not soup:
                continue
            # b3.4
            qlook_div = soup.find('div', id='qlook')
            # b3.5
            if qlook_div:
                # b3.6
                h2_tag = qlook_div.find('div', class_='h2')
                p_tag = qlook_div.find('p')
                # b3.7
                if h2_tag and p_tag:
                    # b3.8
                    list_temperature.append(h2_tag.text.strip().rstrip('°C').split()[0])
                    list_status.append(p_tag.text.strip())
            else:
                # b3.5.1
                list_temperature.append("No data available")
                list_status.append("No p tag found")
        except Exception as e:
            # b3.9
            write_log_to_db("ERROR", f"Lỗi khi lấy thông tin thời tiết từ URL {url_country}: {e}", "Craw data")

# Phương thức lấy ra thông tin chính về thời tiết của các link và kết hợp với CrawInformation
def CrawDetailedInformation(url):
    try:
        #B3.1 Lấy danh sách link
        list_link_country_web = CrawLinkCountry(url_main_web)
        # B3.2 Lấy tên quốc gia
        list_country = CrawCountry(url_main_web)
        # B3.3 Kiểm tra giá trị 2 biến có null không
        if not list_link_country_web or not list_country:
            write_log_to_db("ERROR", "Không thể lấy danh sách link quốc gia hoặc tên quốc gia", "Craw data")

            return []

        #B3.4 Lấy thông tin bổ sung về thời tiết
        CrawInformation(url_web)
        #B3.5 tạo biến
        list_data_country_weather = []
        i = 0
        #B3.6 duyệt qua link lấy data
        for country in list_link_country_web:
            try:
                #B3.6.1 Truy cập URL của từng quốc gia
                url_country = url + country
                soup = GetPageContent(url_country)
                # B3.6.2 kiểm trả giá trị của soup
                if not soup:
                    write_log_to_db("ERROR", f"Không thể lấy nội dung từ URL: {url_country}", "Craw data")
                    continue
                # B3.6.3 Gán giá trị từ thư viên soup
                title = soup.find('div', class_="bk-focus__qlook")
                table = soup.find("table", class_="table table--left table--inner-borders-rows")

                # B3.6.4 Nếu không tìm thấy bảng, ghi log và bỏ qua quốc gia này
                if not table:
                    write_log_to_db("WARNING", f"Không tìm thấy dữ liệu bảng tại URL: {url_country}", "Craw data")
                    continue
                # B3.6.5 chèn dữ liệu vào output
                output_row = []
                output_row.append(list_country[i])  # Tên quốc gia
                output_row.append(list_temperature[i])  # Nhiệt độ
                output_row.append(list_status[i])  # Trạng thái thời tiết

                # B3.6.6 Lấy dữ liệu chi tiết từ bảng
                j = 0
                # B3.6.7 chạy for để lấy dữ liệu từ table
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

                # B3.6.8 Thêm dữ liệu thời gian hết hạn
                output_row.append("24/12/2999")
                # B3.6.9 add dữ liệu vào output
                list_data_country_weather.append(output_row)
                print(f"Dữ liệu quốc gia {list_country[i]}: {output_row}")
            except Exception as e:
                write_log_to_db("ERROR", f"Lỗi khi xử lý dữ liệu quốc gia {list_country[i]}: {e}", "Craw data")
            i += 1

        write_log_to_db("SUCCESS", "Hoàn tất crawl thông tin chi tiết thời tiết", "Craw data")
        # B3.6.10 trả dữ liệu output
        return list_data_country_weather
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi trong phương thức CrawDetailedInformation: {e}", "Craw data")
        return []

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

            # Ghi dữ liệu
            csv_writer.writerows(data)

        write_log_to_db("SUCCESS", f"Dữ liệu được thêm vào file CSV thành công: {filePath}", "Craw data")
    except Exception as e:
        write_log_to_db("ERROR", f"Lỗi khi thêm dữ liệu vào file CSV: {e}", "Craw data")

# Quá trình chính
def CrawData():
    # 1
    set_values()  # Lấy cấu hình
    # 2
    if not url_web or not url_main_web or not mainFilePath:
        # 2.1
        write_log_to_db("ERROR", "URL hoặc đường dẫn file không hợp lệ", "Craw data")
        # 2.2
        send_email("[ERROR] Craw data", "URL hoặc đường dẫn file không hợp lệ")
        return
    try:
        # 3
        data = CrawDetailedInformation(url_web)
        # 4
        ExportCsv(data, mainFilePath)
    except Exception as e:
        # 5
        write_log_to_db("ERROR", f"Lỗi khi crawl data: {e}", "Craw data")
        send_email("[ERROR] Craw data", f"Lỗi khi crawl data: {e}")
CrawData()
