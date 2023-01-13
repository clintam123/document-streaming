from numpy import double
import streamlit as st
from pandas import DataFrame

import numpy as np

import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/', username='root', password='example')
db = client['docstreaming']
collection = db['invoices']

# Below the first chart add a input field for the invoice number
cust_id = st.sidebar.text_input('CustomerID:')
# st.text(inv_no)  # Use this to print out the content of the input field

# if enter has been used on the input field
if cust_id:
    query = {'CustomerID': cust_id}
    # only includes or excludes
    document = collection.find(query, {"_id": 0, "StockCode": 0, "Description": 0, "Quantity": 0, "Country": 0,
                                       "UnitPrice": 0})

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(document)

    # drop duplicates, but keep the first one
    df.drop_duplicates(subset='InvoiceNo', keep='first', inplace=True)

    # Add the table with a headline
    st.header('Output Customer Invoices')
    customer_table = st.dataframe(data=df)

# Below the fist chart add a input field for the invoice number
inv_no = st.sidebar.text_input("InvoiceNo:")
# st.text(inv_no)  # Use this to print out the content of the input field

# if enter has been used on the input field
if inv_no:
    query = {"InvoiceNo": inv_no}
    document = collection.find(query, {"_id": 0, "InvoiceDate": 0, "Country": 0, "CustomerID": 0})

    # create the dataframe
    df = DataFrame(document)

    # reindex it so that the columns are order lexicographically
    reindexed = df.reindex(sorted(df.columns), axis=1)

    # Add the table with a headline
    st.header("Output by Invoice ID")
    invoice_table = st.dataframe(data=reindexed)
