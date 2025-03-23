from typing import Union
import json
from fastapi import FastAPI,HTTPException
import uvicorn
import logging
from pydantic import BaseModel
import os 

logging.basicConfig(level=logging.INFO,filemode="w",filename="logs.txt",format='%%((message)s%% : %%(name)s%%',datefmt='%m/%d/%Y %I:%M:%S %p')
app = FastAPI()

class Item(BaseModel):
    name: str="abcd"
    price: float
    is_offer: Union[bool, None] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int,goy:str, q: Union[str, None] = None):
    
   try:
      logging.getLoggerClass(__name__)
      logging.info("item added !")
      return {"item_id": item_id, "q": q,"g":goy}
   except HTTPException as exe:
      logging.error(exec)

@app.put("/items/{item_id}")
def read_item(item_id: int,item:Item):
    # logging.getLoggerClass(__name__)
    logging.info("item updated !")
    return {"item_name": item.name,"item_price": item.price}

@app.get("/api/v1/customer")
def get_customers():
  try:

    os.chdir("/home/usmail/sys_fraud/sys_detection_frauds/system_fraud_transactions_detection/data")
    with open("customers.json","r") as file:
     return json.load(file)
    
  except Exception as exec:
     print(os.getcwd())
  

@app.get("/api/v1/transactions")
def get_transactions():
   try:
    os.chdir("/home/usmail/sys_fraud/sys_detection_frauds/system_fraud_transactions_detection/data")

    with open("transactions.json","r") as file:
      return json.load(file) 
   except FileNotFoundError as exec:
      logging.error(exec)    

@app.get("/api/v1/externaldata")
def externaldata():
  try:
    os.chdir("/home/usmail/sys_fraud/sys_detection_frauds/system_fraud_transactions_detection/data")

    with open("external_data.json","r") as file:
      return json.load(file)    
  except FileNotFoundError as exec:
      logging.error(exec)    
      
if __name__=="__main__":
   uvicorn.run(app, host="127.0.0.1", port=8000)

   