import pandas as pd
import pymysql
from sqlalchemy import create_engine

#Load data
df = pd.read_csv("Superstore.csv", encoding = 'latin1')

#Check Column Names
print(df.head())
print(df.info())

#Transform Data
df.columns = df.columns.str.lower().str.replace(" ","_")
print(df.columns)

df['order_date'] = pd.to_datetime(df['order_date'])
df['ship_date'] = pd.to_datetime(df['ship_date'])
print(df.isnull().sum())

#Clean Data
df = df.dropna()
df = df.drop_duplicates()
print(df.dtypes)
print(df.head())
print(df.describe())

#Split into Tables
customers = df[['customer_id','customer_name','segment']].drop_duplicates()

products = df[['product_id','product_name','category','sub-category']].drop_duplicates()

location = df[['postal_code','city','state','region','country']].drop_duplicates()

orders = df[['order_id','order_date','ship_date','ship_mode','customer_id','postal_code']].drop_duplicates()

sales = df[['row_id', 'order_id','product_id','sales','quantity','discount','profit']]

print (customers.shape)
print (products.shape)
print (orders.shape)
print (sales.shape)

#Connect Python to MySQL
engine = create_engine ("mysql+pymysql://root:@localhost/superstore_db")

try:
    engine.connect()
    print ("Connected to XAMPP MySQL!")
except Exception as e:
    print ("Error:", e)

#Fix Customer.coumns error to load data into database
customers.colums = customers.columns.str.strip()
print(customers.columns)

products['product_id'] = products['product_id'].str.strip()
products = products.drop_duplicates(subset = ['product_id'])

#Load Tables into Database
try:
    customers.to_sql('customer', engine, if_exists = 'replace', index = False)
    print("customer uploaded")

    products.to_sql('products', engine, if_exists = 'replace', index = False)
    print("products uploaded")

    location.to_sql('location', engine, if_exists = 'replace', index = False)
    print("location uploaded")

    orders.to_sql('orders', engine, if_exists = 'replace', index = False)
    print("orders uploaded")

    sales.to_sql('sales', engine, if_exists = 'replace', index = False)
    print("sales uploaded")

except Exception as e:
    print ("Error:", e)

print("Data uploaded successfully!")