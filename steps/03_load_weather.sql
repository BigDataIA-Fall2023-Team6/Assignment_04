/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       03_load_weather.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Data sharing/marketplace (instead of ETL)
-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (PrPr, Git integration)


USE ROLE Flight_ROLE;
USE WAREHOUSE Flight_WH;
USE DATABASE Flight_DB;

-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- ----------------------------------------------------------------------------

/*---
But what about data that needs constant updating - like the WEATHER data? We would
need to build a pipeline process to constantly update that data to keep it fresh.

Perhaps a better way to get this external data would be to source it from a trusted
data supplier. Let them manage the data, keeping it accurate and up to date.

Enter the Snowflake Data Cloud...

Weather Source is a leading provider of global weather and climate data and their
OnPoint Product Suite provides businesses with the necessary weather and climate data
to quickly generate meaningful and actionable insights for a wide range of use cases
across industries. Let's connect to the "Weather Source LLC: frostbyte" feed from
Weather Source in the Snowflake Data Marketplace by following these steps:

    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Weather Source LLC: frostbyte" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "FROSTBYTE_WEATHERSOURCE" (all capital letters)
                        -> Grant to "HOL_ROLE"
    
That's it... we don't have to do anything from here to keep this data updated.
The provider will do that for us and data sharing means we are always seeing
whatever they they have published.


-- You can also do it via code if you know the account/share details...
SET WEATHERSOURCE_ACCT_NAME = '*** PUT ACCOUNT NAME HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE_NAME = '*** PUT ACCOUNT SHARE HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE = $WEATHERSOURCE_ACCT_NAME || '.' || $WEATHERSOURCE_SHARE_NAME;

CREATE OR REPLACE DATABASE FROSTBYTE_WEATHERSOURCE
  FROM SHARE IDENTIFIER($WEATHERSOURCE_SHARE);

GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table
--SELECT * FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES LIMIT 100;



-- SELECT
--     fsl.FLIGHT_NUMBER,
--     fsl.SCHEDULED_DEPARTURE_DATE_LOCAL,
--     fsl.DEPARTURE_ACTUAL_OFFGROUND_LOCAL,
--     fsl.ARRIVAL_ACTUAL_ONGROUND_LOCAL,
--     ees.AIRCRAFT_TYPE,
--     ees.ESTIMATED_CO2_TOTAL_TONNES,
--     ees.ESTIMATED_FUEL_BURN_TOTAL_TONNES,
--     gsc.CONNECTION_ID,
--     gsc.ELAPSED_TIME,
--     gsc.FREQUENCY
-- FROM
--     FLIGHT_STATUS.FLIGHT_STATUS_DATA fsl
-- JOIN
--     --FLIGHT_EMISSION.FLIGHT_EMISSION_DATA ees ON fsl.FLIGHT_NUMBER = ees.FLIGHT_NUMBER
--     FLIGHT_EMISSION.FLIGHT_EMISSION_DATA ees ON CAST(fsl.FLIGHT_NUMBER AS VARCHAR) = CAST(ees.FLIGHT_NUMBER AS VARCHAR)
--     AND fsl.SCHEDULED_DEPARTURE_DATE_LOCAL = ees.SCHEDULED_DEPARTURE_DATE
-- JOIN
--     FLIGHT_GLOBAL_CONNECTION.FLIGHT_GLOBAL_CONNECTION_DATA gsc ON ees.DEPARTURE_AIRPORT = gsc.LEG_1_DEPARTURE_AIRPORT_IATA
--     AND ees.ARRIVAL_AIRPORT = gsc.LEG_1_ARRIVAL_AIRPORT_IATA
-- WHERE
--     fsl.IS_OPERATING_CARRIER = TRUE
--     AND ees.IS_OPERATING = TRUE
--     AND gsc.IS_SELF_CONNECTION = 0;

CREATE OR REPLACE VIEW ANALYTICS.my_flight_data_view AS
SELECT
    fsl.FLIGHT_NUMBER,
    fsl.SCHEDULED_DEPARTURE_DATE_LOCAL,
    MIN(fsl.DEPARTURE_ACTUAL_OFFGROUND_LOCAL) AS MIN_DEPARTURE_ACTUAL_OFFGROUND_LOCAL,
    MAX(fsl.ARRIVAL_ACTUAL_ONGROUND_LOCAL) AS MAX_ARRIVAL_ACTUAL_ONGROUND_LOCAL,
    ees.AIRCRAFT_TYPE,
    SUM(ees.ESTIMATED_CO2_TOTAL_TONNES) AS TOTAL_ESTIMATED_CO2,
    SUM(ees.ESTIMATED_FUEL_BURN_TOTAL_TONNES) AS TOTAL_ESTIMATED_FUEL_BURN,
    gsc.CONNECTION_ID,
    AVG(gsc.ELAPSED_TIME) AS AVG_ELAPSED_TIME,
    AVG(gsc.FREQUENCY) AS AVG_FREQUENCY
FROM
    FLIGHT_STATUS.FLIGHT_STATUS_DATA fsl
JOIN
    FLIGHT_EMISSION.FLIGHT_EMISSION_DATA ees ON CAST(fsl.FLIGHT_NUMBER AS VARCHAR) = CAST(ees.FLIGHT_NUMBER AS VARCHAR)
    AND fsl.SCHEDULED_DEPARTURE_DATE_LOCAL = ees.SCHEDULED_DEPARTURE_DATE
JOIN
    FLIGHT_GLOBAL_CONNECTION.FLIGHT_GLOBAL_CONNECTION_DATA gsc ON ees.DEPARTURE_AIRPORT = gsc.LEG_1_DEPARTURE_AIRPORT_IATA
    AND ees.ARRIVAL_AIRPORT = gsc.LEG_1_ARRIVAL_AIRPORT_IATA
WHERE
    fsl.IS_OPERATING_CARRIER = TRUE
    AND ees.IS_OPERATING = TRUE
    AND gsc.IS_SELF_CONNECTION = 0
GROUP BY
    fsl.FLIGHT_NUMBER,
    fsl.SCHEDULED_DEPARTURE_DATE_LOCAL,
    ees.AIRCRAFT_TYPE,
    gsc.CONNECTION_ID;