from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def create_flight_view(session):
    session.use_schema('Flight_Analytics')

    flight_status_data = session.table("Flight_Status.Flight_Status_Data")
    flight_emission_data = session.table("Flight_Emission.Flight_Emission_Data")

    # Join the tables on the FLIGHT_NUMBER column
    joined_df = flight_status_data.join(
        flight_emission_data,
        flight_status_data['AIRCRAFT_REGISTRATION_NUMBER'] == flight_emission_data['AIRCRAFT_REGISTRATION_NUMBER'],
        rsuffix='_emission'
    )

    # Select the desired columns from Flight_Status_Data
    selected_cols = [
        "ACTUAL_BUSINESS_CLASS_SEATS",
        "ACTUAL_ECONOMY_CLASS_SEATS",
        "ACTUAL_FIRST_CLASS_SEATS",
        "ACTUAL_PREMIUM_ECONOMY_CLASS_SEATS",
        "ACTUAL_TOTAL_SEATS",
        "AIRCRAFT_REGISTRATION_NUMBER",
        "ARRIVAL_ACTUAL_INGATE_LOCAL",
        "ARRIVAL_ACTUAL_ONGROUND_LOCAL",
        "ARRIVAL_COUNTRY_CODE",
        "ARRIVAL_ESTIMATED_INGATE_LOCAL",
        "ARRIVAL_GATE",
        "ARRIVAL_IATA_AIRPORT_CODE",
        "ARRIVAL_TERMINAL",
        "BAGGAGE",
        "CHECK_IN_COUNTER",
        "DEPARTURE_ACTUAL_OFFGROUND_LOCAL",
        "DEPARTURE_COUNTRY_CODE",
        "FLIGHT_NUMBER",
        "FLIGHT_TYPE",
        "SCHEDULED_ARRIVAL_TIME_LOCAL",
        "SCHEDULED_DEPARTURE_DATE_LOCAL",
        "SCHEDULED_DEPARTURE_TIME_LOCAL",
        "STATUS_KEY"
    ]

    # Add the selected columns from Flight_Emission_Data
    selected_cols += [
        "AIRCRAFT_TYPE",
        "ARRIVAL_AIRPORT",
        "DEPARTURE_AIRPORT",
        "ESTIMATED_CO2_CRUISE_TONNES",
        "ESTIMATED_CO2_TAKEOFF_TONNES",
        "ESTIMATED_CO2_TAXI_IN_TONNES",
        "ESTIMATED_CO2_TAXI_OUT_TONNES",
        "ESTIMATED_CO2_TOTAL_TONNES",
        "ESTIMATED_FUEL_BURN_CRUISE_TONNES",
        "ESTIMATED_FUEL_BURN_TAKEOFF_TONNES",
        "ESTIMATED_FUEL_BURN_TAXI_IN_TONNES",
        "ESTIMATED_FUEL_BURN_TAXI_OUT_TONNES",
        "ESTIMATED_FUEL_BURN_TOTAL_TONNES",
        "IS_OPERATING"
    ]

    # Create or replace the Flight_View
    joined_df.select(selected_cols).create_or_replace_view('Flight_View')

# For local debugging
if __name__ == "__main__":
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_flight_view(session)

    session.close()