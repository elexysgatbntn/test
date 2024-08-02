import os
import json
from typing import List, Optional
from pydantic import BaseModel, Field
from bson import ObjectId
from pymongo import MongoClient

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError(f'Invalid ObjectId with value: {v}')
        return ObjectId(v)

    @classmethod
    def __get_pydantic_json_schema__(cls, schema):
        schema.update(type="string")

class DocumentModel(BaseModel):
    id: PyObjectId = Field(default_factory=PyObjectId, alias="_id")
    title: str
    content: str
    author: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    embedding: Optional[List[float]] = []

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['mydatabase']
collection = db['mycollection']

def load_json_files(directory: str) -> List[dict]:
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    data = []
    for json_file in json_files:
        with open(os.path.join(directory, json_file), 'r') as file:
            data.append(json.load(file))
    return data

def insert_documents(directory: str):
    documents = load_json_files(directory)
    for doc_data in documents:
        try:
            document = DocumentModel(**doc_data)
            document_dict = document.dict(by_alias=True)
            insert_result = collection.insert_one(document_dict)
            print(f"Document inserted with ID: {insert_result.inserted_id}")
        except Exception as e:
            print(f"Failed to insert document: {e}")

# path to data sa repo mo
json_directory = 'path/to/your/json/files'

insert_documents(json_directory)
