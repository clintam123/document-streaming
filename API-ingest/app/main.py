import json
from datetime import datetime

from fastapi.encoders import jsonable_encoder
from kafka import KafkaProducer
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.responses import JSONResponse

import kafka


# Create class (schema) for the JSON
# Date is ingested as a string and then before writing will be validated
class InvoiceItem(BaseModel):
    InvoiceNo: int
    StockCode: str
    Description: str
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: int
    Country: str


app = FastAPI()


@app.get('/')
async def root():
    return {'message': 'Hello World'}


@app.post("/invoice-item")
async def post_invoice_item(item: InvoiceItem):
    print('Message received')
    try:
        date = datetime.strptime(item.InvoiceDate, '%d/%m/%Y %H:%M')
        print('Found a timestamp:', date)

        # Replace / with - and add seconds
        item.InvoiceDate = date.strftime('%d-%m-%Y %H:%M:%S')

        json_of_item = jsonable_encoder(item)
        print('type', type(json_of_item))
        print('json-item', json_of_item)

        json_as_string = json.dumps(json_of_item)
        print('json-string', json_as_string)

        produce_kafka_string(json_as_string)

        return JSONResponse(content=json_of_item, status_code=201)

    # Will be thrown by datetime if the date does not fit
    # All other value errors are automatically taken care of because of the InvoiceItem Class
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)


def produce_kafka_string(json_string):
    producer = KafkaProducer(bootstrap_servers='kafka:9092', acks=1)

    producer.send('ingestion-topic', bytes(json_string, 'utf-8'))
    producer.flush()
