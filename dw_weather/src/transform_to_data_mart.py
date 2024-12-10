import pymysql
from datetime import datetime


def CrawInformationDB():
    # Buoc 1
    lines = []

    try:
        # Buoc 2
        with open(r"E:\dw_weather\dw_weather\src\connect_db.txt", "r", encoding="utf-8") as file:
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



# Method to transform data from fact table to Data Mart
def transform_to_data_mart():
    """
    Transforms data from the fact table into the data mart for analytics.
    The data mart includes aggregated data grouped by country and time.
    """
    # Step 1: Fetch database connection credentials for data_mart
    db_credentials = CrawInformationDB()
    # Ensure the credentials are correctly read and split
    if not db_credentials or len(db_credentials) < 4:
        # If the credentials are missing or incomplete, log an error
        write_log_to_db("ERROR", "Missing database credentials for Data Mart transformation.",
                        "Data Mart Transformation")
        print("error here 1")
        return

    # Use the correct credentials for the data_mart database
    host = db_credentials[8]  # Line 6: Host (localhost)
    user = db_credentials[9]  # Line 7: Username
    password = db_credentials[10]  # Line 8: Password
    data_mart_db = db_credentials[11]  # Line 9: Database name (data_mart)
    data_warehouse_db = 'data_warehouse'  # Name of the database containing fact_table and other tables

    try:
        # Step 2: Connect to the data_mart database for inserting results
        with DatabaseConnection(host, user, password, data_mart_db) as conn:
            with conn.cursor() as cursor:
                # Step 4: Log the start of the transformation
                write_log_to_db("INFO", "Starting Data Mart transformation process.", "Data Mart Transformation")
                print("connect to do")

                # Step 5: Clear the existing Data Mart table to ensure fresh data
                cursor.execute("TRUNCATE TABLE data_mart_weather")
                conn.commit()
                write_log_to_db("INFO", "Cleared existing Data Mart table.", "Data Mart Transformation")

                # Step 7: Log fetching data from the fact table in data_warehouse
                write_log_to_db("INFO", "Fetching data from fact table for aggregation.", "Data Mart Transformation")

                # Step 8: Adjusted SQL query to reference fact_table and other tables in data_warehouse
                cursor.execute(f"""
                    SELECT 
                        c.values_country AS nation,
                        l.values_location AS location,
                        d.year, d.month,
                        AVG(f.temperature) AS avg_temperature,
                        AVG(f.humidity) AS avg_humidity,
                        AVG(f.pressure) AS avg_pressure,
                        AVG(f.visibility) AS avg_visibility,
                        COUNT(*) AS record_count
                    FROM {data_warehouse_db}.fact_table f  -- Specify data_warehouse database for fact_table
                    JOIN {data_warehouse_db}.country c ON f.country_id = c.id  -- Specify data_warehouse for country
                    JOIN {data_warehouse_db}.location l ON f.location_id = l.id  -- Specify data_warehouse for location
                    JOIN {data_warehouse_db}.date_dim d ON f.date_id = d.id  -- Specify data_warehouse for date_dim
                    GROUP BY nation, location, year, month
                """)
                data_mart_records = cursor.fetchall()

                # Step 9: Insert data into data_mart_weather table
                if data_mart_records:
                    insert_query = """
                        INSERT INTO data_mart_weather (
                            nation, location, year, month, avg_temperature, 
                            avg_humidity, avg_pressure, avg_visibility, record_count
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, data_mart_records)
                    conn.commit()

                    # Step 10: Log success
                    write_log_to_db("SUCCESS", f"Inserted {len(data_mart_records)} records into Data Mart.",
                                    "Data Mart Transformation")
                else:
                    # Log if no data found
                    write_log_to_db("INFO", "No data to insert into Data Mart.", "Data Mart Transformation")

    except Exception as e:
        # Step 11: Log any errors encountered
        write_log_to_db("ERROR", f"Error during Data Mart transformation: {e}", "Data Mart Transformation")
        print("hello")



transform_to_data_mart()
