from typing import Union
import json
from fastapi import FastAPI,HTTPException
import uvicorn
import logging
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO,filemode="w",filename="logs.txt",format='%%((message)s%% : %%(name)s%%')
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
    with open("customers.json","r") as file:
      return json.load(file)

@app.get("/api/v1/transactions")
def get_transactions():
    with open("../data/transactions.json","r") as file:
      return json.load(file)    

@app.get("/api/v1/externaldata")
def get_transactions():
    with open("../data/external_data.json","r") as file:
      return json.load(file)    
      
if __name__=="__main__":
   uvicorn.run(app, host="127.0.0.1", port=8000)

   