import os
import json
import chardet
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError
from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime

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
    id: Optional[PyObjectId] = Field(default_factory=PyObjectId, alias="_id")
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

def read_file_with_encoding(file_path: str, encoding: str) -> str:
    with open(file_path, 'r', encoding=encoding) as file:
        return file.read()

def load_json_files(directory: str) -> List[dict]:
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]
    data = []
    for json_file in json_files:
        file_path = os.path.join(directory, json_file)
        try:
            encoding = detect_encoding(file_path)
            file_content = read_file_with_encoding(file_path, encoding)
            json_data = json.loads(file_content)
            if 'entries' in json_data:
                for entry in json_data['entries']:
                    # Ensure 'content' is a string, concatenate if it's a list
                    if isinstance(entry.get('content'), list):
                        entry['content'] = ' '.join(entry['content'])
                    data.append(entry)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file {file_path}: {e}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    return data

def insert_or_update_document(doc_data: dict):
    author = doc_data.get('author')
    if not author:
        print("Author field is required.")
        return
    
    existing_doc = collection.find_one({"author": author})
    current_time = datetime.now().isoformat()

    if existing_doc:
        # Update existing document
        update_data = {
            "title": doc_data.get('title'),
            "content": doc_data.get('content'),
            "updated_at": current_time,
            "embedding": doc_data.get('embedding', [])
        }
        collection.update_one({"author": author}, {"$set": update_data})
        print(f"Document with author '{author}' updated.")
    else:
        # Insert new document
        new_doc = {
            "title": doc_data.get('title'),
            "content": doc_data.get('content'),
            "author": author,
            "created_at": current_time,
            "updated_at": current_time,
            "embedding": doc_data.get('embedding', [])
        }
        collection.insert_one(new_doc)
        print(f"New document with author '{author}' inserted.")

def insert_documents(directory: str):
    documents = load_json_files(directory)
    for doc_data in documents:
        try:
            insert_or_update_document(doc_data)
        except ValidationError as e:
            print(f"Validation error: {e}")
        except Exception as e:
            print(f"Failed to insert/update document: {e}")

# Specify the directory containing JSON files
json_directory = r"C:\Users\elexy\OneDrive\Desktop\test\data"
insert_documents(json_directory)
