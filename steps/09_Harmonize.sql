USE ROLE Flight_ROLE;
USE WAREHOUSE Flight_WH;
USE DATABASE Flight_DB;
CREATE OR REPLACE VIEW HARMONIZED.combined_flight_data AS
SELECT
    gc.CONNECTION_ID,
    gc.DEPARTURE_CITY_CODE,
    gc.DEPARTURE_AIRPORT_IATA,
    gc.ARRIVAL_CITY_CODE,
    gc.ARRIVAL_AIRPORT_IATA,
    gc.EFFECTIVE_PERIOD_START_DATE,
    gc.EFFECTIVE_PERIOD_END_DATE,
    gc.DAYS_OF_OPERATION,
    gc.ARRIVAL_INTERVAL_DAYS,
    gc.ELAPSED_TIME,
    CAST(gc.LEG_1_CARRIER_CODE_IATA AS VARCHAR) AS LEG_1_CARRIER_CODE_IATA_gc,
    CAST(gc.LEG_1_FLIGHT_NUMBER AS VARCHAR) AS LEG_1_FLIGHT_NUMBER_gc,
    gc.LEG_1_DEPARTURE_LOCAL_TIME,
    gc.LEG_1_ARRIVAL_LOCAL_TIME,
    CAST(gc.LEG_2_CARRIER_CODE_IATA AS VARCHAR) AS LEG_2_CARRIER_CODE_IATA_gc,
    CAST(gc.LEG_2_FLIGHT_NUMBER AS VARCHAR) AS LEG_2_FLIGHT_NUMBER_gc,
    gc.LEG_2_DEPARTURE_LOCAL_TIME,
    gc.LEG_2_ARRIVAL_LOCAL_TIME,
    ed.ESTIMATED_FUEL_BURN_TOTAL_TONNES,
    ed.ESTIMATED_CO2_TOTAL_TONNES,
    sd.STATUS_KEY,
    sd.SCHEDULED_DEPARTURE_DATE_LOCAL,
    sd.DEPARTURE_ESTIMATED_OFFGROUND_LOCAL,
    sd.ARRIVAL_ESTIMATED_INGATE_LOCAL,
    sd.ACTUAL_TOTAL_SEATS
FROM
    FLIGHT_DB.STAGE_DATA.FLIGHT_GLOBAL_CONNECTION_DATA gc
JOIN
    FLIGHT_DB.STAGE_DATA.FLIGHT_EMISSION_DATA ed ON LEG_1_CARRIER_CODE_IATA_gc = ed.CARRIER_CODE AND TO_CHAR(gc.LEG_1_FLIGHT_NUMBER) = ed.FLIGHT_NUMBER
JOIN
    FLIGHT_DB.STAGE_DATA.FLIGHT_STATUS_DATA sd ON LEG_1_CARRIER_CODE_IATA_gc = sd.IATA_CARRIER_CODE AND TO_CHAR(gc.LEG_1_FLIGHT_NUMBER) = TO_CHAR(sd.FLIGHT_NUMBER)
WHERE
    gc.DEPARTURE_AIRPORT_IATA = ed.DEPARTURE_AIRPORT AND
    gc.ARRIVAL_AIRPORT_IATA = ed.ARRIVAL_AIRPORT AND
    sd.DEPARTURE_IATA_AIRPORT_CODE = gc.DEPARTURE_AIRPORT_IATA AND
    sd.ARRIVAL_IATA_AIRPORT_CODE = gc.ARRIVAL_AIRPORT_IATA;