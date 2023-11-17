# # from snowflake.snowpark import Session
# # from snowflake.snowpark.functions import udf
# # from snowflake.snowpark.types import StringType, FloatType

# # # Connection parameters
# # connection_parameters = {
# #     "account": "reoyefh-pcb13308",
# #     "user": "RYUK18",
# #     "password": "Ryuk@1997",
# #     "role": "Flight_ROLE",
# #     "warehouse": "Flight_WH",
# #     "database": "Flight_DB"
# # }

# # # Start a session
# # session = Session.builder.configs(connection_parameters).create()

# # # Define the function logic
# # def get_max_emission():
# #     # Define the SQL for the UDF
# #     udf_sql = """
# #         SELECT 
# #             FLIGHT_NUMBER, 
# #             MAX(ESTIMATED_CO2_TOTAL_TONNES) AS max_emission
# #         FROM 
# #             ANALYTICS.MY_FLIGHT_DATA_VIEW
# #         GROUP BY 
# #             FLIGHT_NUMBER
# #         ORDER BY 
# #             max_emission DESC
# #         LIMIT 1
# #     """
# #     return udf_sql

# # # Register the UDF if it does not exist
# # udf_name = "get_max_emission_flight"
# # if not session.sproc.exists(udf_name):
# #     session.sproc.create_or_replace(get_max_emission, return_type=FloatType(), is_permanent=True, name=udf_name)

# # # Now call the UDF
# # result = session.sql(f"SELECT * FROM TABLE({udf_name}())").collect()

# # print("Flight Number with Maximum Emission:", result)

# # # End the session
# # session.close()

# from snowflake.snowpark import Session

# # Connection parameters
# connection_parameters = {
#     "account": "reoyefh-pcb13308",
#     "user": "RYUK18",
#     "password": "Ryuk@1997",
#     "role": "Flight_ROLE",
#     "warehouse": "Flight_WH",
#     "database": "Flight_DB"
# }

# # Start a session
# session = Session.builder.configs(connection_parameters).create()

# # Function name
# udf_name = "get_max_emission_flight"

# # Check if the UDF exists
# udf_exists_query = f"SHOW FUNCTIONS LIKE '{udf_name}'"
# udf_exists_result = session.sql(udf_exists_query).collect()

# # If the UDF does not exist, create it
# if not udf_exists_result:
#     # Define the SQL for the UDF
#     create_udf_sql = f"""
#     CREATE OR REPLACE FUNCTION ANALYTICS.{udf_name}()
#     RETURNS TABLE (flight_number NUMBER(38,0), max_emission FLOAT)
#     LANGUAGE SQL
#     AS
#     $$
#     SELECT 
#         FLIGHT_NUMBER, 
#         MAX(TOTAL_ESTIMATED_CO2) AS max_emission
#     FROM 
#         ANALYTICS.MY_FLIGHT_DATA_VIEW
#     GROUP BY 
#         FLIGHT_NUMBER
#     ORDER BY 
#         max_emission DESC
#     LIMIT 1
#     $$
#     """
#     session.sql(create_udf_sql).collect()

# # Now call the UDF
# result = session.sql(f"SELECT * FROM TABLE(ANALYTICS.{udf_name}())").collect()

# print("Flight Number with Maximum Emission:", result)

# # End the session
# session.close()


# NEW CODE

import sys
from snowflake.snowpark import Session

# Connection parameters
connection_parameters = {
    "account": "reoyefh-pcb13308",
    "user": "RYUK18",
    "password": "Ryuk@1997",
    "role": "Flight_ROLE",
    "warehouse": "Flight_WH",
    "database": "Flight_DB"
}

# Start a session
with Session.builder.configs(connection_parameters).create() as session:
    # Check if the correct number of arguments are provided
    if len(sys.argv) < 3:
        print("Usage: python testapp.py [aircraft_reg_number] [aircraft_type]")
        sys.exit(1)

    # Assign command-line arguments to variables
    aircraft_reg_number = sys.argv[1]
    aircraft_type = sys.argv[2]

    # Define the SQL for the UDF
    get_flight_details_udf_sql = """
    CREATE OR REPLACE FUNCTION ANALYTICS.get_flight_details(aircraft_reg_number VARCHAR, aircraft_type VARCHAR)
    RETURNS TABLE 
    AS
    $$
    SELECT *
    FROM ANALYTICS.FLIGHT_CONSUMPTION_ANALYTICS
    WHERE AIRCRAFT_REGISTRATION_NUMBER = :aircraft_reg_number
      AND AIRCRAFT_TYPE = :aircraft_type;
    $$
    """

    # Check if the UDF exists and create it if not
    udf_exists_result = session.sql(f"SHOW FUNCTIONS LIKE 'ANALYTICS.get_flight_details'").collect()
    if not udf_exists_result:
        session.sql(get_flight_details_udf_sql).collect()

    # Call the UDF with command-line arguments
    result = session.sql(f"SELECT * FROM TABLE(ANALYTICS.get_flight_details('{aircraft_reg_number}', '{aircraft_type}'))").collect()

    for row in result:
        print(row)
