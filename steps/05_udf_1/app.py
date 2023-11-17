

import sys
import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import udf

# Function Definition: Local Python function (for testing logic)
def get_max_emission_flight(arrival_airport: str, departure_airport: str):
    query = f"""
    SELECT
        AIRCRAFT_REGISTRATION_NUMBER,
        ARRIVAL_COUNTRY_CODE,
        ESTIMATED_CO2_TOTAL_TONNES,
        ESTIMATED_FUEL_BURN_TOTAL_TONNES,
        ACTUAL_TOTAL_SEATS,
        IS_OPERATING,
        ARRIVAL_ACTUAL_ONGROUND_LOCAL,
        DEPARTURE_ACTUAL_OFFGROUND_LOCAL
    FROM
        FLIGHT_ANALYTICS.FLIGHT_VIEW
    WHERE
        ARRIVAL_AIRPORT = '{arrival_airport}' AND DEPARTURE_AIRPORT = '{departure_airport}'
    ORDER BY
        ESTIMATED_CO2_TOTAL_TONNES,
        ESTIMATED_FUEL_BURN_TOTAL_TONNES
    """
    return session.sql(query).collect()

# Main Execution
if __name__ == "__main__":
    # Set up the path to include the utils module
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    # Import the utilities and establish a session
    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    # SQL to create the UDF in Snowflake
    create_udf_sql = """
    CREATE OR REPLACE FUNCTION FLIGHT_ANALYTICS.get_emission_data_for_flights_between_airports(arrival_airport_name VARCHAR, departure_airport_name VARCHAR)
    RETURNS TABLE (
        AIRCRAFT_REGISTRATION_NUMBER VARCHAR,
        ARRIVAL_COUNTRY_CODE VARCHAR,
        ESTIMATED_CO2_TOTAL_TONNES FLOAT,
        ESTIMATED_FUEL_BURN_TOTAL_TONNES FLOAT,
        ACTUAL_TOTAL_SEATS INT,
        IS_OPERATING BOOLEAN,
        ARRIVAL_ACTUAL_ONGROUND_LOCAL TIMESTAMP,
        DEPARTURE_ACTUAL_OFFGROUND_LOCAL TIMESTAMP
    )
    LANGUAGE SQL
    AS
    $$
    SELECT
        AIRCRAFT_REGISTRATION_NUMBER,
        ARRIVAL_COUNTRY_CODE,
        ESTIMATED_CO2_TOTAL_TONNES,
        ESTIMATED_FUEL_BURN_TOTAL_TONNES,
        ACTUAL_TOTAL_SEATS,
        IS_OPERATING,
        ARRIVAL_ACTUAL_ONGROUND_LOCAL,
        DEPARTURE_ACTUAL_OFFGROUND_LOCAL
    FROM
        FLIGHT_ANALYTICS.FLIGHT_VIEW
    WHERE
        ARRIVAL_AIRPORT = arrival_airport_name AND DEPARTURE_AIRPORT = departure_airport_name
    ORDER BY
        ESTIMATED_CO2_TOTAL_TONNES,
        ESTIMATED_FUEL_BURN_TOTAL_TONNES
    $$
    """

    # Execute the SQL to create the UDF in Snowflake
    session.sql(create_udf_sql).collect()

    # Test the UDF with an example
    test_result = session.sql("SELECT * FROM TABLE(FLIGHT_ANALYTICS.get_emission_data_for_flights_between_airports('ORD', 'RSW'))").collect()
    print(test_result)

    for row in test_result:
        print(row);

    # Optionally, close the session
    # session.close()
