# import os
# import streamlit as st
# import snowflake.connector
# import google.generativeai as genai

# # Configure API key
# genai.configure(api_key=os.getenv("my_api_key"))

# # Function to load Google Gemini model and provide query as response
# def get_gemini_response(question, prompt):
#     model = genai.GenerativeModel('gemini-pro')
#     response = model.generate_content([prompt[0], question])
#     return response.text

# # Function to retrieve query from Snowflake database
# def read_snowflake_query(sql):
#     # Establish connection to Snowflake
#     conn = snowflake.connector.connect(
#         user='PRIYANSHAGARWAL2305',
#         password='Priyansh@2002',
#         account='kc32134.ap-southeast-1',
#         warehouse='COMPUTE_WH',
#         database='INVENTORY_PREDICTION',
#         schema='PUBLIC'
#     )
#     cur = conn.cursor()
#     cur.execute(sql)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     for row in rows:
#         print(row)
#     return rows

# def generate_natural_language_response(question, sql_query, sql_result):
#     # Convert SQL results to a string format
#     result_str = "\n".join([str(row) for row in sql_result])
    
#     # Create a new prompt to generate natural language response
#     nl_prompt = f"""
#     The SQL query generated for the question "{question}" was: {sql_query}
#     The result of the query is: {result_str}
#     Please provide a natural language response that answers the original question based on the query results and MAKE SURE YOU INCLUDE EACH AND EVERY RESULT OF OUTPUT QUERY.
#     It must not be missed.
#     """
    
#     # Get the natural language response from the model
#     model = genai.GenerativeModel('gemini-pro')
#     response = model.generate_content([nl_prompt])
#     return response.text

# # Define your prompt
# prompt = [
#     """
#     You are an expert in converting English questions to SQL query!
#     The SQL table has the name INVENTORY_MANAGEMENT and has the following columns -
#     InvoiceNo, Product_Code, Product_Description, Quantity, Unit_Price, CustomerId,
#     Country, Date, Time, Revenue, Cancelled.

#     The InvoiceNo column is the bill number of every order, multiple products can be on the same
#     bill number.
#     Product_Code column is the unique code for every product.
#     Product_Description column contains name of the product.
#     Quantity column has the number of items of that products bought.
#     Unit_Price column contains the price for the product.
#     CustomerId column is a UniqueID for the customer who bought that product.
#     Country column is the country in which that product was purchased.
#     Date column contains the date (in YYYY-MM-DD format) on which every product was bought.
#     Time column contains the time (in HH:MM:SS format) on ehich the product was bought.
#     Revenue column contains the sales which is nothing but Quantity*Unit_price for the transaction.
#     Cancelled column is the column which decide whether this is an cancelled transaction or not. 
 
#     also the sql should not have ''' in the beginning or end and sql word in the output
#     """
# ]

# # Streamlit App
# st.set_page_config(page_title="I can retrieve any SQL query")
# st.header("Gemini App to retrieve SQL data")

# question = st.text_input("Input:", key='input')
# submit = st.button("Ask the Question")

# # If submit is clicked
# if submit:
#     response = get_gemini_response(question, prompt)
#     print(response)
#     response = read_snowflake_query(response)
#     st.subheader("The Response is: ")
#     for row in response:
#         print(row)
#         st.header(row)

# if submit:
#     sql_query = get_gemini_response(question, prompt)
#     print(sql_query)
#     sql_result = read_snowflake_query(sql_query)
#     natural_language_response = generate_natural_language_response(question, sql_query, sql_result)
#     st.subheader("The Response is: ")
#     st.write(natural_language_response)

import configparser
import streamlit as st
import snowflake.connector
import google.generativeai as genai

config = configparser.ConfigParser()

config.read('config.ini')

API_KEY = config['GoogleAPI']['GOOGLE_API_KEY']

genai.configure(api_key=API_KEY)
 
# Function to load Google Gemini model and provide query as response
def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text

SNOWFLAKE_USER = config['Snowflake']['user']
SNOWFLAKE_PASSWORD = config['Snowflake']['password']
SNOWFLAKE_ACCOUNT = config['Snowflake']['account']
SNOWFLAKE_WAREHOUSE = config['Snowflake']['warehouse']
SNOWFLAKE_DATABASE = config['Snowflake']['database']
SNOWFLAKE_SCHEMA = config['Snowflake']['schema']
SNOWFLAKE_ROLE = config['Snowflake']['role']
 
# Function to retrieve query from Snowflake database
def read_snowflake_query(sql):
    # Establish connection to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role = SNOWFLAKE_ROLE
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
 
    # for row in rows:
        # print(row)
    return rows

def generate_natural_language_response(question, sql_query, sql_result):
    # Convert SQL results to a string format
    result_str = "\n".join([str(row) for row in sql_result])
    
    # Create a new prompt to generate natural language response
    nl_prompt = f"""
    The SQL query generated for the question "{question}" was: {sql_query}
    The result of the query is: {result_str}
    Please provide a natural language response that answers the original question based on the query results and MAKE SURE YOU INCLUDE EACH AND EVERY RESULT OF OUTPUT QUERY.
    It must not be missed.
    """
    
    # Get the natural language response from the model
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([nl_prompt])
    return response.text
 
# Define your prompt
prompt = [
    """
    You are an expert in converting English questions to SQL query!
    The SQL table has the name INVENTORY_MANAGEMENT and has the following columns -
    InvoiceNo, Product_Code, Product_Description, Quantity, Unit_Price, Customer_Id,
    Country, Date, Time, Revenue, Cancelled.
 
    The InvoiceNo column is the bill number of every order, multiple products can be on the same
    bill number.
    Product_Code column is the unique code for every product.
    Product_Description column contains name of the product.
    Quantity column has the number of items of that products bought.
    Unit_Price column contains the price for the product.
    CustomerId column is a UniqueID for the customer who bought that product.
    Country column is the country in which that product was purchased.
    Date column contains the date (in YYYY-MM-DD format) on which every product was bought.
    Time column contains the time (in HH:MM:SS format) on ehich the product was bought.
    Revenue column contains the sales which is nothing but Quantity*Unit_price for the transaction.
    Cancelled column is the column which decide whether this is a cancelled transaction or not. 1 means the order is cancelled, and 0
    means the order is not cancelled.

    For every Product_Code in the dataset, there is mostly only one Product_Description in the dataset. But for some records,
    there can be multiple product description so handle queries related to these colums accordingly.
 
    Please make sure that the sql should not have ''' in the beginning or end and sql word in the output
    because while testing the query, there is high possibility that this query would result in error.

    Make sure that for every query that requires Customer_Id column, don't consider null values for customer Id. This is important
    as Null customer_id is of no use.

    Also, make sure that you use only above provided column names while generating the query and do not get
    biased for any keyword given to you in the user prompt.

    One more important information is that I am running these queries on snowflake, so provides sql queries that are 
    compatible with snowflake.

    There is only one table (INVENTORY_MANAGEMENT) that you have to deal with so apply joins on the table only when it is necessary.
    """ 
]
 
# Streamlit App
st.set_page_config(page_title="I can retrieve any SQL query")
st.header("Gemini App to retrieve SQL data")
 
question = st.text_input("Input:", key='input')
submit = st.button("Ask the Question")

if submit:
    sql_query = get_gemini_response(question, prompt)
    print(sql_query)
    sql_result = read_snowflake_query(sql_query)
    natural_language_response = generate_natural_language_response(question, sql_query, sql_result)
    st.subheader("The Response is: ")
    st.write(natural_language_response)
