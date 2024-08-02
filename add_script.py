import os
import json
import chardet
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError
from bson import ObjectId
from pymongo import MongoClient

class PyObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, *args):
        if not ObjectId.is_valid(v):
            raise ValueError(f'Invalid ObjectId with value: {v} from cls {cls}')
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
mongo_url = "mongodb+srv://finquest:123@finquest.jupe3n1.mongodb.net/?retryWrites=true&w=majority&appName=finquest"
client = MongoClient(mongo_url)
db = client['finquest']
collection = db['documents']

def detect_encoding(file_path: str) -> str:
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Read the first 10 KB
        result = chardet.detect(raw_data)
        return result['encoding']
    
def read_file_with_encoding(file_path: str) -> str:
    encodings = ['utf-8', 'latin-1', 'cp1252']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                return file.read()
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Unable to read file with supported encodings: {file_path}")


def load_json_files(directory: str) -> List[dict]:
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    data = []
    for json_file in json_files:
        file_path = os.path.join(directory, json_file)
        try:
            file_content = read_file_with_encoding(file_path)
            json_data = json.loads(file_content)
            if 'entries' in json_data:
                data.extend(json_data['entries'])
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file {file_path}: {e}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
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


json_directory = json_directory = r"C:\Users\elexy\OneDrive\Desktop\test\data"


insert_documents(json_directory)
