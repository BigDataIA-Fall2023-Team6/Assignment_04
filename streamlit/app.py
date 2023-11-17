import os
import openai
from sqlalchemy import create_engine
import streamlit as st
from langchain.chains import LLMChain
from dotenv import load_dotenv
from langchain.document_loaders import SnowflakeLoader
from snowflake.snowpark import Session
from langchain.chains import create_sql_query_chain
from langchain.llms import OpenAI
from langchain.utilities import SQLDatabase

load_dotenv()

os.environ["OPENAI_API_KEY"] = os.environ.get("API_KEY") # Replace with your actual OpenAI API key
openai.api_key = os.environ.get("API_KEY")

# Initialize session state for chat history
if 'chat_history' not in st.session_state:
    st.session_state['chat_history'] = []

if 'current_query' not in st.session_state:
    st.session_state['current_query'] = ""

st.title("Text to SQL Converter using Snowflake")
conn = st.connection("snowflake")

genre = st.radio(
    "You can Select the Raw Tables to View the Column Names",
    ["FLIGHT_STATUS_DATA", "FLIGHT_GLOBAL_CONNECTION_DATA", "FLIGHT_EMISSION_DATA"],
    index=None,
)
# Perform query without caching.
df = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_STATUS' AND TABLE_NAME = 'FLIGHT_STATUS_DATA';")
df1 = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_GLOBAL_CONNECTION' AND TABLE_NAME = 'FLIGHT_GLOBAL_CONNECTION_DATA';")
df2 = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_EMISSION' AND TABLE_NAME = 'FLIGHT_EMISSION_DATA';")

if genre == 'FLIGHT_STATUS_DATA':
    st.write(df)

elif genre == 'FLIGHT_GLOBAL_CONNECTION_DATA':
    st.write(df1)

elif genre == 'FLIGHT_EMISSION_DATA':
    st.write(df2)

user_input = st.text_input("Enter your question:")

# Display chat history
for entry in st.session_state['chat_history'][::-1]:
    st.text(entry)

OPENAI_API_KEY = os.environ.get("API_KEY")
snowflake_account = os.environ.get("SNOWFLAKE_ACCOUNT")
username = os.environ.get("SNOWFLAKE_USER")
password = os.environ.get("SNOWFLAKE_PASS")
role = os.environ.get("SNOWFLAKE_ROLE")
warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
database= os.environ.get("SNOWFLAKE_DATABASE")
schema = os.environ.get("SNOWFLAKE_SCHEMA")


snowflake_url = f"snowflake://{username}:{password}@{snowflake_account}/{database}/{schema}?warehouse={warehouse}&role={role}"
print(snowflake_url)

engines = create_engine(snowflake_url)
sql_database = SQLDatabase(engines)
print(sql_database.table_info)

llm = OpenAI(temperature=0,model='text-davinci-003',openai_api_key=OPENAI_API_KEY)
database_chain = create_sql_query_chain(llm,sql_database)

connection_parameters = {
            "account": snowflake_account,
            "user": username,
            "password": password,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema
        }
session = Session.builder.configs(connection_parameters).create()
if session:
    print("Session Created Successfully")
if user_input:
    prompt = user_input

    sql_query = database_chain.invoke({"question": prompt})

    if prompt != st.session_state['current_query']:
        st.session_state['current_query'] = prompt
        sql_query = database_chain.invoke({"question": prompt})

        st.session_state['chat_history'].append(f"User: {prompt}")
        st.session_state['chat_history'].append(f"Generated SQL: {sql_query}")

        # Show the generated SQL query and ask for confirmation
        st.write(sql_query)

    # Show the generated SQL query and ask for confirmation
    # st.write(sql_query)
    confirm = st.button("Confirm")
    wrong = st.button("Wrong")

    if confirm:
        st.session_state['chat_history'].append("User confirmed the query.")

        # Process the confirmed query here        
    if wrong:
        corrected_sql = st.text_input("Correct the SQL query:")
        submit_correction = st.button("Submit Correction")
        if submit_correction:
            st.session_state['chat_history'].append(f"User corrected SQL: {corrected_sql}")
            QUERY = corrected_sql
            snowflake_loader = SnowflakeLoader(
                query=QUERY,
                account = os.environ.get("SNOWFLAKE_ACCOUNT"),
                user= os.environ.get("SNOWFLAKE_USER"),
                password=os.environ.get("SNOWFLAKE_PASS"),
                role=os.environ.get("SNOWFLAKE_ROLE"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
                database=os.environ.get("SNOWFLAKE_DATABASE"),
                schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            )
            try:
                snowflake_documents = snowflake_loader.load()
                page_contents = [doc.page_content for doc in snowflake_documents]

                st.dataframe(page_contents)
            except Exception as e:
            # Handle any errors that occur during data loading
                st.error("An error occurred: Seems like the Query is incorrect. Please Review the query and Check the column names and try again.")

            # Process the corrected query here


################### Working Query to Query the Database ###########################################################
    Query_input = st.text_input("Enter your final query:")

    if Query_input:
        QUERY = Query_input
        snowflake_loader = SnowflakeLoader(
            query=QUERY,
            account = os.environ.get("SNOWFLAKE_ACCOUNT"),
            user= os.environ.get("SNOWFLAKE_USER"),
            password=os.environ.get("SNOWFLAKE_PASS"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        )
        try:
            snowflake_documents = snowflake_loader.load()
            page_contents = [doc.page_content for doc in snowflake_documents]

            st.dataframe(page_contents)
        except Exception as e:
        # Handle any errors that occur during data loading
            st.error("An error occurred: Seems like the Query is incorrect. Please Review the query and Check the column names and try again.")
#     def format_document_content(page_content):
#         formatted_content = ""
#         lines = page_content.split('\n')
#         for line in lines:
#             key, value = line.split(':', 1)  # Split only on the first colon
#             value = value.strip() if value else "N/A"
#             formatted_content += f"{key}: {value}\n"
#         return formatted_content
    
# # Use an expander for each document
#     for i, doc in enumerate(snowflake_documents):
#         with st.expander(f"Document {i}"):
#             formatted_content = format_document_content(doc.page_content)
#             st.text(formatted_content)



