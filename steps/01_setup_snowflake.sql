/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       01_setup_snowflake.sql
Author:       Jeremiah Hansen
Last Updated: 1/1/2023
-----------------------------------------------------------------------------*/


-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------

-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;

SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE Flight_ROLE;

GRANT ROLE Flight_ROLE TO ROLE SYSADMIN;
GRANT ROLE Flight_ROLE TO USER IDENTIFIER($MY_USER);

GRANT EXECUTE TASK ON ACCOUNT TO ROLE Flight_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE Flight_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE Flight_ROLE;

-- GRANT WRITE PRIVILEGES ON DATABASE OAG_FLIGHT_STATUS_DATA_SAMPLE TO ROLE Flight_ROLE;




-- Databases
CREATE OR REPLACE DATABASE Flight_DB;
GRANT OWNERSHIP ON DATABASE Flight_DB TO ROLE Flight_ROLE;

-- Warehouses
CREATE OR REPLACE WAREHOUSE Flight_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
GRANT OWNERSHIP ON WAREHOUSE Flight_WH TO ROLE Flight_ROLE;



USE ROLE Flight_ROLE;
USE WAREHOUSE Flight_WH;
USE DATABASE Flight_DB;


-- Schemas
CREATE OR REPLACE SCHEMA Flight_Emission;
CREATE OR REPLACE SCHEMA Flight_Global_Connection;
CREATE OR REPLACE SCHEMA Flight_Status;

CREATE OR REPLACE SCHEMA Stage_Data;

CREATE OR REPLACE SCHEMA Flight_Analytics;
CREATE OR REPLACE SCHEMA ANALYTICS;

CREATE STAGE my_udf_stage;





--USE ROLE ACCOUNTADMIN;

--DROP DATABASE Flight_DB;
--DROP WAREHOUSE Flight_WH;
--DROP ROLE Flight_ROLE;

--Drop the weather share

USE DATABASE FLIGHT_DB;
USE WAREHOUSE FLIGHT_WH;

Select * FROM FLIGHT_STATUS.FLIGHT_STATUS_DATA where Flight_Status_Data.BAGGAGE is not null limit 100;




-- Set the context to the target schema
USE SCHEMA Flight_Analytics;

-- Create or replace the Flight_View
--CREATE OR REPLACE VIEW Flight_View AS
SELECT
    fsd.actual_first_class_seats,
    fsd.arrival_terminal,
    fed.aircraft_type,
    fed.carrier_code
FROM
    Stage_Data.Flight_Status_Data fsd
JOIN
    Stage_Data.Flight_Emission_Data fed
ON
    fsd.aircraft_registration_number = fed.aircraft_registration_number
    
    limit 5;



SELECT
AIRCRAFT_REGISTRATION_NUMBER, ARRIVAL_COUNTRY_CODE,ESTIMATED_CO2_TOTAL_TONNES,ESTIMATED_FUEL_BURN_TOTAL_TONNES,
ACTUAL_TOTAL_SEATS, IS_OPERATING, ARRIVAL_ACTUAL_ONGROUND_LOCAL, DEPARTURE_ACTUAL_OFFGROUND_LOCAL

FROM
    FLIGHT_ANALYTICS.FLIGHT_VIEW

where
    ARRIVAL_AIRPORT = 'ORD' and DEPARTURE_AIRPORT = 'RSW'
order by ESTIMATED_CO2_TOTAL_TONNES,ESTIMATED_FUEL_BURN_TOTAL_TONNES;


SELECT
ESTIMATED_CO2_TOTAL_TONNES, ESTIMATED_FUEL_BURN_TOTAL_TONNES,
ARRIVAL_AIRPORT, DEPARTURE_AIRPORT, FLIGHT_NUMBER

FROM
    FLIGHT_ANALYTICS.FLIGHT_VIEW

where ESTIMATED_CO2_TOTAL_TONNES is not null and ESTIMATED_FUEL_BURN_TOTAL_TONNES is not null and DEPARTURE_AIRPORT ='BOM';



select *
FROM TABLE(GET_MAX_EMISSION_FLIGHT('ORD','RSW'));

use FLIGHT_DB;
use schema FLIGHT_ANALYTICS;
select FLIGHT_DB.FLIGHT_ANALYTICS.GET_MAX_EMISSION_FLIGHT('ORD','RSW');


use HOL_DB;

select ANALYTICS.FAHRENHEIT_TO_CELSIUS_UDF(50);


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
    ARRIVAL_AIRPORT = 'ORD' AND DEPARTURE_AIRPORT = 'RSW'
ORDER BY
    ESTIMATED_CO2_TOTAL_TONNES,
    ESTIMATED_FUEL_BURN_TOTAL_TONNES;



CREATE OR REPLACE FUNCTION get_max_emission_flight(arrival_airport varchar, departure_airport varchar)
RETURNS TABLE (
    AIRCRAFT_REGISTRATION_NUMBER varchar,
    ARRIVAL_COUNTRY_CODE varchar,
    ESTIMATED_CO2_TOTAL_TONNES FLOAT,
    ESTIMATED_FUEL_BURN_TOTAL_TONNES FLOAT,
    ACTUAL_TOTAL_SEATS INTEGER,
    IS_OPERATING BOOLEAN,
    ARRIVAL_ACTUAL_ONGROUND_LOCAL TIMESTAMP_NTZ,
    DEPARTURE_ACTUAL_OFFGROUND_LOCAL TIMESTAMP_NTZ
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
        ARRIVAL_AIRPORT is arrival_airport AND DEPARTURE_AIRPORT is departure_airport
$$;



SELECT efficiency_score FROM FLIGHT_ANALYTICS.comprehensive_flight_efficiency
where Flight_Num