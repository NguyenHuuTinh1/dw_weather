import pymysql
from datetime import datetime


def read_db_credentials(file_path):
    """Reads database connection credentials from a file."""
    with open(file_path, "r", encoding="utf-8") as file:
        return [line.strip() for line in file]


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


def transform_staging_to_fact(db_credentials):
    """
    Transforms data from staging tables into the fact table.

    Assumptions:
    - Staging tables: country, location, weather_description, and a staging table for raw weather data.
    - Fact table: weather_fact (e.g., with columns country_id, location_id, weather_id, temperature, humidity, etc.).
    """
    with DatabaseConnection(*db_credentials) as conn:
        with conn.cursor() as cursor:

            # STEP 1: Fetch the data from the staging tables
            print("Fetching staging data...")

            # Fetch country mappings
            cursor.execute("SELECT id, values_country FROM country")
            country_map = {row[1]: row[0] for row in cursor.fetchall()}

            # Fetch location mappings
            cursor.execute("SELECT id, values_location FROM location")
            location_map = {row[1]: row[0] for row in cursor.fetchall()}

            # Fetch weather description mappings
            cursor.execute("SELECT id, values_weather FROM weather_description")
            weather_map = {row[1]: row[0] for row in cursor.fetchall()}

            # Fetch raw staging data (adjust columns as necessary)
            cursor.execute("""
                SELECT nation, temperature, weather_status, location, currentTime, latestReport, visibility, pressure, humidity, dew_point, dead_time
                FROM staging
            """)
            staging_data = cursor.fetchall()

            print(f"Fetched {len(staging_data)} records from the staging table.")

            # STEP 2: Transform and insert into the fact table
            fact_inserts = []

            for record in staging_data:
                nation, temperature, weather_status, location, current_time, latest_report, visibility, pressure, humidity, dew_point, dead_time = record

                # Get the country ID using the 'nation' value
                country_id = country_map.get(nation)
                location_id = location_map.get(location)
                weather_id = weather_map.get(weather_status)

                # Check if all required mappings are available
                if country_id and location_id and weather_id:
                    fact_inserts.append((
                        country_id, location_id, weather_id, temperature, current_time, latest_report, visibility, pressure, humidity, dew_point, dead_time
                    ))
                else:
                    print(f"Skipping record due to missing mapping: {record}")

            # Insert transformed data into the fact table
            if fact_inserts:
                print("Inserting transformed data into the fact table...")
                insert_query = """
                    INSERT INTO fact_table (
                        country_id, location_id, weather_id, temperature, currentTime, latestReport, visibility, pressure, humidity, dew_point, dead_time
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_query, fact_inserts)
                conn.commit()
                print(f"Inserted {len(fact_inserts)} records into the fact table.")
            else:
                print("No data to insert into the fact table.")


if __name__ == "__main__":
    # Load database credentials
    CONNECT_DB_FILE = r"D:\dw_weather\crawl_data\src\connect_db.txt"
    db_credentials = read_db_credentials(CONNECT_DB_FILE)

    # Transform and load data from staging to fact table
    transform_staging_to_fact(db_credentials)
