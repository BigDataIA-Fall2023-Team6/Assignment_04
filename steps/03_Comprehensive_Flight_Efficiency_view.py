from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, when, minute, unix_timestamp

def comprehensive_Flight_Efficiency_view(session):
    session.use_schema('Flight_Analytics')

        # Load the data
        # Load the data
    flight_status_df = session.table("stage_data.flight_status_data")
    flight_emission_df = session.table("stage_data.flight_emission_data")

    # Calculate the delay duration and efficiency score as separate columns
    delay_duration_minutes = ((unix_timestamp(flight_status_df["DEPARTURE_ACTUAL_OFFGROUND_LOCAL"]) - unix_timestamp(flight_status_df["SCHEDULED_DEPARTURE_TIME_LOCAL"])) / 60)
    efficiency_score = flight_emission_df["ESTIMATED_CO2_TOTAL_TONNES"] / when(delay_duration_minutes == 0, None).otherwise(delay_duration_minutes)

    # Perform the join and select operations, applying the aliases separately
    result_df = flight_status_df.join(
        flight_emission_df,
        flight_status_df["aircraft_registration_number"] == flight_emission_df["aircraft_registration_number"]
    ).select(
        flight_status_df["FLIGHT_NUMBER"].alias("Flight_Number"),
        flight_status_df["DEPARTURE_IATA_AIRPORT_CODE"].alias("departure_airport"),
        flight_status_df["ARRIVAL_IATA_AIRPORT_CODE"].alias("arrival_airport"),
        flight_status_df["SCHEDULED_DEPARTURE_TIME_LOCAL"],
        flight_status_df["DEPARTURE_ACTUAL_OFFGROUND_LOCAL"].alias("actual_departure_time"),
        flight_emission_df["ESTIMATED_CO2_TOTAL_TONNES"].alias("total_co2_emissions"),
        delay_duration_minutes.alias("delay_duration_minutes"),
        when(flight_status_df["FLIGHT_STATE"] == "Cancelled", 1).otherwise(0).alias("is_cancelled"),
        efficiency_score.alias("efficiency_score")
    )

    # Filter out rows where efficiency_score is null
    result_df = result_df.filter(col("efficiency_score").is_not_null())

    # Create the view
    result_df.create_or_replace_view("comprehensive_flight_efficiency")

# For local debugging
if __name__ == "__main__":
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    comprehensive_Flight_Efficiency_view(session)

    session.close()