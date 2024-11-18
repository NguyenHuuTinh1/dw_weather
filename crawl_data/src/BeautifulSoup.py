import bs4
import requests
import csv

# khởi tạo các biến
list_temperature = []
list_status = []

# gắn giá trị từ db control vào cho biến -->chưa code
url_main_web = 'https://www.timeanddate.com/weather/?low=c&sort=1'
url_web = 'https://www.timeanddate.com'
mainFilePath = 'D:\Crawl_Data\crawl_data\.venv'
list_title = 'Nation, Temperature, Weather status, Location, Current Time, Latest Report, Visibility, Pressure, Humidity, Dew Point, Dead Time'
list_title = list_title.split(', ')


# phương thức phụ lấy ra web
def GetPageContent(url):
    page = requests.get(url, headers={"Accept-Language": "en-US"})
    return bs4.BeautifulSoup(page.text, "html.parser")

# phương thức lấy ra link từ trang chủ
def CrawLinkCountry(url):
    soup = GetPageContent(url)
    table = soup.find("table", class_="zebra fw tb-theme")
    list_link_country_web = []
    if table:
        for table_row in table.find_all('tr'):
            columns = table_row.find_all('a')
            for column in columns:
                link = column['href']
                list_link_country_web.append(link)
    return list_link_country_web
# phương thức lấy ra tên các quốc gia gắn với từ link
def CrawCountry(url):
    list_country = []
    soup = GetPageContent(url)
    table = soup.find("table", class_="zebra fw tb-theme")
    if table:
        for table_row in table.find_all('tr'):
            columns = table_row.find_all('a')
            for column in columns:
                country = column.text
                first_part = country.split(",")[0]
                list_country.append(first_part)
    return list_country
# phương thức lấy ra thông tin bổ sung về thời tiết của link
def CrawInformation(url):
    list_link_country_web = CrawLinkCountry(url_main_web)
    for country in list_link_country_web:
        url_country = url + country
        soup = GetPageContent(url_country)
        qlook_div = soup.find('div', id='qlook')
        if qlook_div:
            h2_tag = qlook_div.find('div', class_='h2')
            p_tag = qlook_div.find('p')
            if h2_tag and p_tag:
                h2_content = h2_tag.text.strip().rstrip('°C').split()[0]
                list_temperature.append(h2_content)
                p_content = p_tag.text.strip()
                list_status.append(p_content)
            else:
                h2_content = "No h2 tag found"
                p_content = "No p tag found"
        else:
            h2_content = "No data available"
            p_content = "No p tag found"

# phương thức lấy ra thông tin chính về thời tiết của các link và kết hợp với CrawInformation cho ra list dữ liệu cuối cùng của quá trình crawdata
def CrawDetailedInformation(url):
    # kết noi db
    list_link_country_web = CrawLinkCountry(url_main_web)
    #ve bo sung
    list_country = CrawCountry(url_main_web)
    CrawInformation(url_web)
    list_data_country_weather = []
    i = 0
    for country in list_link_country_web:
        url_country = url + country
        soup = GetPageContent(url_country)
        title = soup.find('div', class_="bk-focus__qlook")
        table = soup.find("table", class_="table table--left table--inner-borders-rows")
        if table:
            output_row = []
            output_row.append(list_country[i])  # Thêm tên quốc gia
            output_row.append(list_temperature[i])
            output_row.append(list_status[i])
            j = 0
            for table_row in table.find_all('tr'):
                list_td = table_row.find_all('td')
                for td in list_td:
                    content = td.text.strip()
                    if j > 2:
                        content = td.text.strip().split()[0]  # Lấy phần tử đầu tiên
                    if j == 5:
                        content = td.text.rstrip('%')  # Loại bỏ ký tự '%'
                    output_row.append(content)
                    j = j+1
            output_row.append("24/12/2999")
            list_data_country_weather.append(output_row)
            print(list_data_country_weather[i])
        i += 1
    return list_data_country_weather

# phương thức xuất dữ liệu ra file csv
def ExportCsv(data, filePath):
    with open('output.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        # csv_writer.writerow(list_title)
        csv_writer.writerows(data)

def CrawData(filePath):
    out = CrawDetailedInformation(url_web)
    ExportCsv(out, filePath)

CrawData(mainFilePath)



