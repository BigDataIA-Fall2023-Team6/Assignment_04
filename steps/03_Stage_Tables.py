def transfer_tables(session):
    database_src = "OAG_FLIGHT_STATUS_DATA_SAMPLE"
    schema_src = "PUBLIC"
    database_dest = "Flight_DB"
    schema_dest = "FLIGHT_STATUS_DATA"
# Import necessary modules from Snowpark
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame

def copy_table_between_schemas_flight_status(session):
    # Define source database, schema, and table
    src_database = "OAG_FLIGHT_STATUS_DATA_SAMPLE"
    src_schema = "PUBLIC"
    src_table = "FLIGHT_STATUS_LATEST_SAMPLE"

    # Define destination database, schema, and table
    dest_database = "Flight_DB"
    dest_schema = "Stage_Data"
    dest_table = "FLIGHT_STATUS_DATA"

    # Set the warehouse size to XLARGE
    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Create a DataFrame for the source table
    source_dataframe = session.table(f'{src_database}.{src_schema}.{src_table}')

    # Write the DataFrame to the destination table
    source_dataframe.write.mode("overwrite").saveAsTable(f'{dest_database}.{dest_schema}.{dest_table}')

    # Set the warehouse size back to XSMALL
    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def copy_table_between_schemas_flight_emission(session):
        # Define source database, schema, and table
    src_database = "OAG_GLOBAL_CONNECTIONS_DATA_SAMPLE"
    src_schema = "PUBLIC"
    src_table = "GLOBAL_SINGLE_CONNECTIONS_SAMPLE"

    # Define destination database, schema, and table
    dest_database = "Flight_DB"
    dest_schema = "Stage_Data"
    dest_table = "FLIGHT_GLOBAL_CONNECTION_DATA"

    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Create a DataFrame for the source table
    source_dataframe = session.table(f'{src_database}.{src_schema}.{src_table}')

    # Write the DataFrame to the destination table
    source_dataframe.write.mode("overwrite").saveAsTable(f'{dest_database}.{dest_schema}.{dest_table}')

    # Set the warehouse size back to XSMALL
    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XSMALL").collect()   

def copy_table_between_schemas_flight_global_connect(session):

    src_database = "OAG_FLIGHT_EMISSIONS_DATA_SAMPLE"
    src_schema = "PUBLIC"
    src_table = "ESTIMATED_EMISSIONS_STATUS_SAMPLE"

    # Define destination database, schema, and table
    dest_database = "Flight_DB"
    dest_schema = "Stage_Data"
    dest_table = "FLIGHT_EMISSION_DATA"
    
    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    # Create a DataFrame for the source table
    source_dataframe = session.table(f'{src_database}.{src_schema}.{src_table}')

    # Write the DataFrame to the destination table
    source_dataframe.write.mode("overwrite").saveAsTable(f'{dest_database}.{dest_schema}.{dest_table}')

    # Set the warehouse size back to XSMALL
    session.sql("ALTER WAREHOUSE Flight_WH SET WAREHOUSE_SIZE = XSMALL").collect()  

# Main function to execute the script
def main():
    # Create a Snowpark session


    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    # Call the function to copy the table
    copy_table_between_schemas_flight_status(session)
    copy_table_between_schemas_flight_emission(session)
    copy_table_between_schemas_flight_global_connect(session)

    # Close the Snowpark session
    session.close()

# Execute the main function if the script is run
if __name__ == "__main__":
    main()