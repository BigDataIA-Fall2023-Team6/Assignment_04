import sys
import os
from snowflake.snowpark import Session



# my_udf_file.py
def get_efficiency_score(flight_num: str) -> float:
    query = f"SELECT efficiency_score FROM comprehensive_flight_efficiency WHERE FLIGHT_NUMBER = '{flight_num}'"
    result = session.sql(query).collect()
    return result[0][0] if result else None



if __name__ == "__main__":
    # Set up the path to include the utils module
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    # Import the utilities and establish a session
    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    session.sql("USE SCHEMA FLIGHT_ANALYTICS").collect()


    # SQL to create the UDF in Snowflake
    create_udf_sql = """
    CREATE OR REPLACE FUNCTION FLIGHT_ANALYTICS.get_efficiency_score(flight_num VARCHAR)
    RETURNS FLOAT
    LANGUAGE SQL
    AS
    $$
    SELECT avg(efficiency_score) as Average_Efficiency_Score FROM FLIGHT_ANALYTICS.comprehensive_flight_efficiency cfe WHERE cfe.FLIGHT_NUMBER = flight_num
    $$
    """

    # Execute the SQL to create the UDF in Snowflake
    session.sql(create_udf_sql).collect()

    # Test the UDF with an example
    test_result = session.sql("SELECT FLIGHT_ANALYTICS.get_efficiency_score('2737')").collect()
    print(test_result)


    # Optionally, close the session
    # session.close()