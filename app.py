import streamlit as st
import numpy as np
import pandas as pd
import snowflake.connector
import configparser
from datetime import timedelta
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA

config = configparser.ConfigParser()

config.read('config.ini')

SNOWFLAKE_USER = config['Snowflake']['user']
SNOWFLAKE_PASSWORD = config['Snowflake']['password']
SNOWFLAKE_ACCOUNT = config['Snowflake']['account']
SNOWFLAKE_WAREHOUSE = config['Snowflake']['warehouse']
SNOWFLAKE_DATABASE = config['Snowflake']['database']
SNOWFLAKE_SCHEMA = config['Snowflake']['schema']
SNOWFLAKE_ROLE = config['Snowflake']['role']

# Step 1: Load the dataset
@st.cache_data
def load_data():
   
    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER ,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA
    )
   
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM INVENTORY_MANAGEMENT")
    df = cursor.fetch_pandas_all()
    conn.close()

    desc_to_rmv = ['DOT', 'POST', 'S', 'AMAZONFEE', 'BANK CHARGES', 'CRUK']

    df = df[~(df['PRODUCT_CODE'].isin(desc_to_rmv))]

    df = df[~((df['REVENUE'] > 1000) | (df['REVENUE'] < -1000))]

    sales_by_product = df.groupby('PRODUCT_CODE')['REVENUE'].sum()
    sorted_sales = sales_by_product.sort_values(ascending=False)

    # Select the top 100 products
    top_100_products = sorted_sales.head(100)
    freq_items = []
    for i in top_100_products.index:
        freq_items.append(i)

    df = df[df['PRODUCT_CODE'].isin(freq_items)]

    return df

data = load_data()

# def get_most_frequent_descriptions(df):
#     # Group by product and find the most frequent description
#     description_mode = df.groupby('StockCode')['Description'].agg(lambda x: x.mode().iloc[0])
#     description_mode = description_mode.reset_index()
    
#     # Create a list of product-description pairs
#     product_description_pairs = description_mode.apply(lambda row: f"{row['StockCode']} - {row['Description']}", axis=1)
#     return product_description_pairs

# product_description_pairs = get_most_frequent_descriptions(data)


# Step 2: Create the UI
st.title("Inventory Prediction")

# Select a product
products = data['PRODUCT_CODE'].unique()
selected_product = st.selectbox('Select a product', products)

# selected_product = selected_product_description.split(' - ')[0]

# Step 3: Filter the dataset
product_data = data[data['PRODUCT_CODE'] == selected_product]

product_data['DATE'] = pd.to_datetime(product_data['DATE'])
product_data.set_index('DATE', inplace=True)
product_data = product_data.REVENUE.resample('D').sum()

product_data.replace(0, np.nan, inplace=True)
product_data.interpolate(method='linear', inplace=True)

product_data = pd.DataFrame(product_data)
# product_data.reset_index(inplace=True)

model = ARIMA(product_data, order=(10, 2, 8))
results = model.fit()

# Step 6: Make future predictions
future_dates = pd.date_range(start=product_data.index[-1] + timedelta(days=1), periods=30, freq='D')
forecast = results.get_forecast(steps=30)
forecast_index = future_dates
forecast_values = forecast.predicted_mean
forecast_values = np.where(forecast_values < 0, 0, forecast_values)
forecast_ci = forecast.conf_int()

#Actual values plot
plt.figure(figsize=(10,6))
plt.plot(product_data.index, product_data['REVENUE'], label='Actual')
plt.xlabel('Date')
plt.ylabel('Actual Sales')
plt.title(f'Actual sales for {selected_product}')
st.pyplot(plt)

actual_sum = product_data['REVENUE'].sum()

st.write(f"Yearly sales of {selected_product}: {actual_sum}")

# Step 7: Plot the forecasted values
plt.figure(figsize=(10, 6))
plt.plot(forecast_index, forecast_values, label='Forecast')
plt.xlabel('Date')
plt.ylabel('Forecasted Sales')
plt.title(f'Inventory Forecast for {selected_product}')
plt.legend()
st.pyplot(plt)

forecast_sum = round(forecast_values.sum(), 2)

# Display the sum of forecasted values
st.write(f"Sum of forecasted sales of the {selected_product} for the next 30 days: {forecast_sum}")