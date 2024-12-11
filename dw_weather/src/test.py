import pymysql

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

def select_data_control_file_config():
    lines = CrawInformationDB()
    if not lines:
        # write_log_to_db("ERROR", "Không lấy được thông tin cấu hình từ file connect_db.txt", "Craw data")
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

            # Thay vì dùng câu SQL SELECT, bạn gọi một stored procedure
            procedure_name = "GetControlDataConfig"  # Thay 'your_procedure_name' bằng tên thủ tục thực tế
            cursor.callproc(procedure_name)

            # Lấy kết quả trả về nếu thủ tục trả về dữ liệu
            result = cursor.fetchone()

            if result:
                print("SUCCESS", "Lấy thông tin cấu hình từ thủ tục database thành công", "Craw data")
                return result
            else:
                print("ERROR", "Không có dữ liệu trả về từ thủ tục", "Craw data")
                return None
    except Exception as e:
        error_message = f"Lỗi khi gọi thủ tục: {e}"
        print("ERROR", error_message, "Craw data")
        return None
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()
print(select_data_control_file_config())