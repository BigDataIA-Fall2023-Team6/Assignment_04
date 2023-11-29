# Data Engineering Pipelines with Snowpark Python

## Part 1: Individual Data Engineering Pipelines with Snowpark Python github links
[Abhishek](https://github.com/Abhishek-Sand/DataEngineering-SnowPark-Python)
[Dilip](https://github.com/dilip-ramesh-sharma/sfguide-data-engineering-with-snowpark-python)
[Vivek](https://github.com/Hanagojiv/DataEngineering-with-snowpark-python)

## Part 2: Snowflake Text to SQL Application

## Application Links
[Codelabs](https://codelabs-preview.appspot.com/?file_id=1dWJvi6KNL4annlpBsybTe6KxJx4BSZYGVkFCE3aWLXg#0)
[Streamlit]()
## Abstract
This application represents a cutting-edge integration of AI, web technology, and database management, designed to streamline the process of converting natural language queries into SQL commands. Key highlights of the project include:

1. Streamlit Integration:

Utilizes Streamlit, an open-source app framework, to create a responsive and interactive web interface.
Enables easy input of user queries and displays real-time data and SQL query outputs.

2. Natural Language Processing with OpenAI:

Incorporates OpenAI's powerful language models for interpreting and translating natural language inputs into SQL queries.
Enhances user experience by allowing intuitive, conversational interaction with the database.

3. Snowflake for Database Management:

Employs Snowflake, a cloud-based data warehousing platform, for robust and scalable database operations.
Facilitates secure and efficient handling of database queries and transactions.

4. Langchain for Advanced Query Processing:

Uses the Langchain library to bridge the gap between OpenAI's language models and SQL query generation.
Supports complex query processing, ensuring accurate translation of user inputs into SQL syntax.

5. User-Friendly Interface Features:

Interactive selection of database tables and columns for tailored data viewing and manipulation.
Real-time display of SQL queries generated from user inputs, with options to confirm or correct the queries.

6. Session and History Management:

Maintains a chat history within the Streamlit session, allowing users to track their queries and the application's responses.
Offers a user-friendly method for reviewing and modifying past queries.

7. Comprehensive Error Handling:

Implements robust error handling to manage and communicate issues during query processing and execution.
Ensures a smooth user experience by providing clear feedback on query syntax and execution errors.

8. Environment and Dependency Management:

Leverages environment variables and dotenv for secure and flexible configuration management.
Ensures consistent execution environments through meticulous dependency management with requirements.txt.
This project exemplifies the innovative use of AI and cloud technologies to simplify and enhance the experience of database interaction, making it accessible to a wider range of users, from database professionals to those with limited SQL expertise.

## UDF 1: get_max_emission_flight

* The script includes SQL to create a Snowflake UDF (FLIGHT_ANALYTICS.get_emission_data_for_flights_between_airports) that encapsulates the query logic.

* The UDF returns a table with various columns related to flights, such as aircraft registration number, country codes, CO2 emissions, fuel burn, total seats, operational status, and timestamps of arrival and departure.
* Use Case:
This UDF is specifically designed to serve multiple purposes in the aviation industry and environmental research:

  1. Analyze Environmental Impact:
   Assesses CO2 emissions and fuel usage for flights between specified airports, aiding in environmental impact studies.
  2. Optimize Flight Operations:
    Helps airlines and regulatory bodies identify trends and areas for operational efficiency improvements.
  3. Inform Policy and Decision Making:
    Provides critical data for informed decision-making in environmental policy formulation and implementation within the aviation sector.

## UDF 2: get_max_emission_flight

* This script includes SQL to create another crucial Snowflake User-Defined Function (UDF), FLIGHT_ANALYTICS.get_emission_data_for_flights_between_airports, focusing on advanced data analysis in flight operations.
* Use Case:
This UDF is designed to provide multifaceted benefits in both aviation operations and environmental research:
  1. Analyze Environmental Impact:
    Enables detailed assessment of CO2 emissions and fuel consumption for specific flight routes, supporting environmental impact analyses.
  2. Optimize Flight Operations:
    Aids airlines and aviation authorities in identifying key trends and potential for efficiency improvements in flight operations.
  3. Inform Policy and Decision Making:
    Delivers crucial data insights for shaping environmental policies and making informed decisions in the aviation sector.

## UDF 3: Flight Details Retrieval

This utility script is designed to connect to Snowflake and create a User-Defined Table Function (UDTF) for retrieving specific flight details.

  * Features:
    Environment Variable Management: Utilizes dotenv to load environment variables for secure and efficient Snowflake connection configuration.

    Dynamic Snowflake Session: Initiates a Snowflake session dynamically, using connection parameters sourced from environment variables.

  * UDTF Creation and Execution:
    Includes SQL to create a Snowflake UDTF, ANALYTICS.get_flight_details, for querying flight data based on departure and arrival airports.

    The UDTF returns a table with columns like Flight Number, Country Codes, Total Seats, Aircraft Type, Fuel Burn, and CO2 Emissions.
  * Use Case:
    This script caters to data analysts and operational managers in the aviation industry, providing them with a tool to:

    1. Retrieve Detailed Flight Information: 
        Quickly fetches specific flight details, aiding in operational analysis and data verification tasks.
    2. Environmental Impact Analysis: 
        Assists in evaluating the environmental impact of flights by providing key data on fuel burn and CO2 emissions.

## Flight Efficiency View

  1. Efficiency Score Calculation:

  * It computes the delay duration in minutes between the scheduled and actual departure times.
  * The efficiency score is formulated as the ratio of estimated CO2 emissions (in tonnes) to the delay duration in minutes. This calculation is done to assess the environmental efficiency of flights, with a special condition to handle zero delay duration.
  2. Data Joining and Transformation:
  * The script joins the flight status data with the emission data on the aircraft registration number.
  * It selects and aliases specific columns, including flight number, departure and arrival airports, CO2 emissions, delay duration, and a binary indicator for flight cancellations.
  3. Efficiency Score Formulation:

  * The efficiency score is calculated as the ratio of estimated CO2 emissions to the delay duration.
  * The formula for the efficiency score is:
    Efficiency Score =
            Estimated CO2 Emissions (tonnes)
        -----------------------------------
            Delay Duration (minutes)

 
  * A special condition is included: If the delay duration is zero (meaning no delay), the efficiency score is set to None (or null) to avoid division by zero.
  * This score provides a measure of environmental impact per minute of delay. A higher score indicates more emissions per minute of delay, suggesting less environmental efficiency.
  4. Interpretation:
  * Low Efficiency Score: Indicates lower emissions relative to the delay duration, suggesting better environmental performance.
  * High Efficiency Score: Signifies higher emissions for the given delay, indicating poorer environmental efficiency. 

## Architecture Diagram

![Alt Text](images\MicrosoftTeams-image (2).png)

## Project Structure
```text
Project/
├── .devcontainer/
├── .github/
├── images/
│   ├── demo_overview.png
├── steps/
│   ├── 04_udf_1_FlightAnalytics/
│   │   ├── utils/
│   │   ├── .gitignore
│   │   ├── app.py
│   │   ├── app.toml
│   │   ├── local_connection.py
│   │   ├── requirements.txt
│   ├── 05_udf_2_EfficiencyScore/
│   │   ├── utils/
│   │   ├── .gitignore
│   │   ├── app.py
│   │   ├── app.toml
│   │   ├── local_connection.py
│   │   ├── requirements.txt
│   ├── 06_udf_3_EmmisionCalc/
│       ├── .gitignore
│       ├── app.py
│       ├── app.toml
│       ├── ex.env
│       ├── local_connection.py
│       ├── requirements.txt
├── 01_setup_snowflake.sql
├── 02_load_raw.py
├── 03_Stage_Tables.py
├── 04_Flight_Analytics_view.py
├── 05_Comprehensive_Flight_Efficiency_view.py
├── 06_Emission_analytics_view.sql
├── 07_global_connection_view.sql
├── 08_orchestrate_jobs.sql
├── 09_Harmonize.sql
├── 10_deploy_via_cicd.sql
├── 11_teardown.sql
├── streamlit/
│   ├── .streamlit/
│   │   ├── app.py
│   ├── utils/
│   ├── .gitignore
│   ├── LEGAL.md
│   ├── LICENSE
│   ├── README.md
│   ├── deploy_snowpark_apps.py
│   ├── environment.yml
│   ├── requirements.txt
```

### Tools
* 🔧 Streamlit - [link](https://streamlit.io/)
* 🔧 LangChain - [link](https://python.langchain.com/docs/modules/chains/foundational/llm_chain)
* 🔧 OpenAI - [link](https://github.com/openai/openai-python)


## Contribution
*   Abhishek : 34`%` 
*   Dilip : 33`%`
*   Vivek : 33`%`

## Team Members 👥
- Abhishek Sand
  - NUID 002752069
  - Email sand.a@northeastern.edu
- Dilip Sharma
  - NUID 002674474
  - Email sharma.dil@northeastern.edu
- Vivek Basavanth Hanagoji
  - NUID 002762662
  - Email hanagoji.v@northeastern.edu

## Individual Distribution

| **Developer**     |          **Deliverables**                        |
|:-------------:    |:----------------------------------:              |
|      Abhishek     | Langchain Implementation (Text to SQL)           |
|      Abhishek     | Streamlit                                        |
|      Abhishek     | Stramlit Cloud hosting                           |
|      Abhishek     | Git setup and integration                        |
|      Vivek        | UDFs and Views                                   |
|      Vivek        | Architecture Diagram                             |
|      Dilip        | Thematic Story Design                            |
|      Dilip        | UDFs and Views                                   |
|      Dilip        | CodeLabs                                         |
|      Vivek        | Documentation                                    |
