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
CREATE OR REPLACE SCHEMA ANALYTICS;




--USE ROLE ACCOUNTADMIN;

--DROP DATABASE Flight_DB;
--DROP WAREHOUSE Flight_WH;
--DROP ROLE Flight_ROLE;

--Drop the weather share
