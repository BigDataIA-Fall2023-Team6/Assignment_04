import sys
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connection parameters
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE")
}

# Start a session
with Session.builder.configs(connection_parameters).create() as session:
    # Check if the correct number of arguments are provided
    if len(sys.argv) < 3:
        print("Usage: python app.py [departure_airport] [arrival_airport]")
        sys.exit(1)

    # Assign command-line arguments to variables
    departure_airport_des = sys.argv[1]
    arrival_airport_des = sys.argv[2]

    # Define the SQL for the UDTF
    get_flight_details_udtf_sql = """
    CREATE OR REPLACE FUNCTION ANALYTICS.get_flight_details(departure_airport_des VARCHAR, arrival_airport_des VARCHAR)
    RETURNS TABLE (FLIGHT_NUMBER NUMBER, DEPARTURE_COUNTRY_CODE VARCHAR, TOTAL_SEATS NUMBER, AIRCRAFT_TYPE VARCHAR, DEPARTURE_AIRPORT VARCHAR, ARRIVAL_AIRPORT VARCHAR, TOTAL_FUEL_BURN FLOAT, TOTAL_CO2_EMISSION FLOAT)
    LANGUAGE SQL
    AS
    $$
        SELECT FLIGHT_NUMBER, DEPARTURE_COUNTRY_CODE, TOTAL_SEATS, AIRCRAFT_TYPE, DEPARTURE_AIRPORT, ARRIVAL_AIRPORT, TOTAL_FUEL_BURN, TOTAL_CO2_EMISSION
        FROM ANALYTICS.FLIGHT_CONSUMPTION_ANALYTICS
        WHERE DEPARTURE_AIRPORT = departure_airport_des
        AND ARRIVAL_AIRPORT = arrival_airport_des
    $$;
    """

    # Execute the UDTF creation SQL
    session.sql(get_flight_details_udtf_sql).collect()

    # Call the UDTF with command-line arguments
    result = session.sql(f"SELECT * FROM TABLE(ANALYTICS.get_flight_details('{departure_airport_des}', '{arrival_airport_des}'))").collect()

    for row in result:
        print(row)