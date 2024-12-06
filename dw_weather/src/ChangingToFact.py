import pymysql
from datetime import datetime


# Method to write logs to the database
def write_log_to_db(status, note, log_date=None):
    """
    Logs events to the database log table.
    :param status: Log status (e.g., "SUCCESS", "ERROR")
    :param note: Description of the log
    :param log_date: Optional log date; uses current time if not provided
    """
    db_credentials = read_db_credentials(r"E:\dw_weather\dw_weather\src\connect_db.txt")
    if not db_credentials:
        print("Cannot write log because no database credentials found.")
        return

    try:
        connection = pymysql.connect(
            host=db_credentials[0],
            user=db_credentials[1],
            password=db_credentials[2],
            database=db_credentials[3]
        )
        if connection.open:
            cursor = connection.cursor()
            sql_query = "INSERT INTO log (status, note, log_date) VALUES (%s, %s, %s)"
            data = (status, note, log_date if log_date else datetime.now())
            cursor.execute(sql_query, data)
            connection.commit()
            print("Log successfully written to the database.")
    except Exception as e:
        print(f"Error writing log: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.open:
            connection.close()


# Method to read database credentials
def read_db_credentials(file_path):
    """
    Reads database connection credentials from a file.
    :param file_path: Path to the credentials file
    :return: List of credentials [host, user, database]
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            return [line.strip() for line in file]
    except Exception as e:
        write_log_to_db("ERROR", f"Failed to read database credentials: {e}")
        return []


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


# Method to transform staging data to fact table
def transform_staging_to_fact():
    """
    Transforms data from staging tables into the fact table,
    including mapping to `date_dim` and `latest_report`.
    """
    db_credentials = read_db_credentials(r"E:\dw_weather\dw_weather\src\connect_db.txt")
    if not db_credentials:
        write_log_to_db("ERROR", "Missing database credentials for staging to fact transformation.")
        return

    try:
        with DatabaseConnection(*db_credentials) as conn:
            with conn.cursor() as cursor:

                # Fetch mappings from lookup tables
                write_log_to_db("INFO", "Fetching lookup table data.")
                cursor.execute("SELECT id, values_country FROM country")
                country_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_location FROM location")
                location_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, values_weather FROM weather_description")
                weather_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, time FROM latesreport")
                latest_report_map = {row[1]: row[0] for row in cursor.fetchall()}

                cursor.execute("SELECT id, date_values FROM date_dim")
                date_map = {row[1]: row[0] for row in cursor.fetchall()}

                # Fetch raw data from the staging table
                write_log_to_db("INFO", "Fetching data from staging table.")
                cursor.execute("""
                    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
                           visibility, pressure, humidity, dew_point, dead_time
                    FROM staging
                """)
                staging_data = cursor.fetchall()

                write_log_to_db("INFO", f"Fetched {len(staging_data)} records from staging table.")

                # Transform data and prepare for fact table insertion
                fact_inserts = []
                for record in staging_data:
                    nation, temperature, weather_status, location, current_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                    country_id = country_map.get(nation)
                    location_id = location_map.get(location)
                    weather_id = weather_map.get(weather_status)
                    report_id = latest_report_map.get(latest_report)

                    # Check or insert into date_dim
                    if current_time not in date_map:
                        day = current_time.day
                        month = current_time.month
                        year = current_time.year
                        cursor.execute(
                            "INSERT INTO date_dim (date_values, day, month, year) VALUES (%s, %s, %s, %s)",
                            (current_time, day, month, year)
                        )
                        conn.commit()
                        cursor.execute("SELECT LAST_INSERT_ID()")
                        date_id = cursor.fetchone()[0]
                        date_map[current_time] = date_id
                    else:
                        date_id = date_map[current_time]

                    # Verify mappings
                    if country_id and location_id and weather_id and report_id and date_id:
                        fact_inserts.append((country_id, location_id, weather_id, date_id, report_id,
                                             temperature, visibility, pressure, humidity, dew_point, dead_time))
                    else:
                        write_log_to_db("WARNING", f"Skipping record due to missing mapping: {record}")

                # Insert transformed data into the fact table
                if fact_inserts:
                    write_log_to_db("INFO", "Inserting data into the fact table.")
                    insert_query = """
                        INSERT INTO fact_table (
                            country_id, location_id, weather_id, date_id, report_id, temperature,
                            visibility, pressure, humidity, dew_point, dead_time
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, fact_inserts)
                    conn.commit()
                    write_log_to_db("SUCCESS", f"Inserted {len(fact_inserts)} records into fact table.")
                else:
                    write_log_to_db("INFO", "No valid data to insert into fact table.")

    except Exception as e:
        write_log_to_db("ERROR", f"Error during staging to fact transformation: {e}")



# Entry point
transform_staging_to_fact()
