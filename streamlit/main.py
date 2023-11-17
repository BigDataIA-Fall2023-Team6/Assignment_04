import streamlit as st
from langchain.chains import LLMChain
from langchain.prompts import load_prompt
from dotenv import load_dotenv

load_dotenv()

# os.environ["OPENAI_API_KEY"] = "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"  # Replace with your actual OpenAI API key
# openai.api_key = os.environ["OPENAI_API_KEY"]

# # Initialize connection.
# st.title("Langchain Text to SQL Conversion using SnowPark-Python")

# conn = st.connection("snowflake")

# genre = st.radio(
#     "Would you like to review the Table Schema",
#     ["FLIGHT_STATUS_DATA", "FLIGHT_GLOBAL_CONNECTION_DATA", "FLIGHT_EMISSION_DATA"],
#     index=None,
# )
# # Perform query without caching.
# df = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_STATUS' AND TABLE_NAME = 'FLIGHT_STATUS_DATA';")
# df1 = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_GLOBAL_CONNECTION' AND TABLE_NAME = 'FLIGHT_GLOBAL_CONNECTION_DATA';")
# df2 = conn.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FLIGHT_EMISSION' AND TABLE_NAME = 'FLIGHT_EMISSION_DATA';")

# if genre == 'FLIGHT_STATUS_DATA':
#     st.write(df)

# elif genre == 'FLIGHT_GLOBAL_CONNECTION_DATA':
#     st.write(df1)

# elif genre == 'FLIGHT_EMISSION_DATA':
#     st.write(df2)

# # User input for the question
# user_input = st.text_input("Enter your question about flight data here")

# # if user_input:
# #     prompt_template = load_prompt("/workspaces/Assignment_04/streamlit/sqlprompt.yml")
# #     llm = OpenAI(temperature=0, model='text-davinci-003')
# #     sql_chain = LLMChain(llm=llm, prompt=prompt_template, verbose=True)

# #     sql_query= sql_chain(user_input)


# # user_input = st.text_input("Enter the question here")

# if user_input:
#     prompt_template = {
#         "input": f"Given Below are the table structure in the Flight_DB in SnowFlake. "
#                  f"In Table: FLIGHT_STATUS_DATA  {df}, Table:FLIGHT_GLOBAL_CONNECTION_DATA {df1} and Table: FLIGHT_EMISSION_DATA {df2}. "
#                  f"Take the User query and respond back with SQL Query. "
#                  f"user question: {user_input}",
#         "output": "and your generated SQL Query:"
#     }
#     llm = OpenAI(temperature=0, model='text-davinci-003')
#     sql_chain = LLMChain(llm=llm, prompt=prompt_template, verbose=True)

#     sql_query = sql_chain(user_input)
#     # result = execute_query(sql_query['text'])
#     # print(result)
#     st.write(sql_query)

# # # Function to generate SQL query using LangChain
def generate_sql_query(question, df, df1, df2):
    prompt_template = {
        "input": f"Given Below are the table structure in the Flight_DB in SnowFlake. "
                 f"In Table: FLIGHT_STATUS_DATA  {df}, Table:FLIGHT_GLOBAL_CONNECTION_DATA {df1} and Table: FLIGHT_EMISSION_DATA {df2}. "
                 f"Take the User query and respond back with SQL Query. "
                 f"user question: {question}",
        "output": "and your generated SQL Query:"
    }
# #     llm = OpenAI(temperature=0, model='text-davinci-003')
# #     response = llm(prompt_template)
# #     return response.choices[0].text.strip()

# # # Streamlit interface
# # st.title("Text to SQL Query Generator")

# # # User input for the question
# # user_input = st.text_input("Enter your question about flight data here")

# # # Generate and display SQL query
# # if user_input:
# #     sql_query = generate_sql_query(user_input, df, df1, df2)
# #     st.text("Generated SQL Query:")
# #     st.write(sql_query)



# # from langchain.prompts import BasePromptTemplate

# # class MyPromptTemplate(BasePromptTemplate):
# #     def format(self, data):
# #         # Implement formatting logic here
# #         # `data` would typically be the user's input or other dynamic information you want to include in the prompt
# #         return {
# #             "input": f"Given Below are the table structure in the Flight_DB in SnowFlake. "
# #                      f"In Table: FLIGHT_STATUS_DATA  {data['df']}, Table:FLIGHT_GLOBAL_CONNECTION_DATA {data['df1']} and Table: FLIGHT_EMISSION_DATA {data['df2']}. "
# #                      f"Take the User query and respond back with SQL Query. "
# #                      f"user question: {data['user_input']}",
# #             "output": "and your generated SQL Query:"
# #         }

# #     def format_prompt(self, data):
# #         # This method should format the prompt as a string
# #         formatted_data = self.format(data)
# #         return f"{formatted_data['input']} {formatted_data['output']}"

# # # Now, use this custom prompt template in your LLMChain
# # user_input = st.text_input("Enter the question here")

# # if user_input:
# #     prompt_template = MyPromptTemplate()
# #     llm = OpenAI(temperature=0, model='text-davinci-003')
# #     sql_chain = LLMChain(llm=llm, prompt=prompt_template, verbose=True)
# #     response = llm.complete(prompt_template)

# #     sql_query = sql_chain(user_input)
# #     # result = execute_query(sql_query['text'])
# #     # print(result)
# #     st.write(sql_query)
import os

import streamlit as st
from langchain.chains import LLMChain  
from langchain.llms import openai
from langchain.prompts import FuncPrompt

# Schema helper functions
def get_schema(df):
  return df[['COLUMN_NAME']].to_dict('records')

def get_prompt(user_input, df_cols, df1_cols, df2_cols):
  return {
    "input": f"Here are the table schemas: {df_cols} {df1_cols} {df2_cols}. Based on this, generate a SQL query for the following user question: {user_input}", 
    "output": "SQL Query:"
  }

os.environ["OPENAI_API_KEY"] = "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"  # Replace with your actual OpenAI API key
openai.api_key = os.environ["OPENAI_API_KEY"]

conn = st.connection("snowflake")

genre = st.radio(
    "Would you like to review the Table Schema",
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

st.title("Text to SQL Converter")
user_input = st.text_input("Enter your question:")


llm = openai(model="code-davinci-002")
prompt = FuncPrompt(get_prompt)
sql_chain = LLMChain(llm=llm, prompt=prompt, verbose=True)
sql_query = sql_chain(user_input, df, df1, df2)  
st.code(sql_query['text'])
