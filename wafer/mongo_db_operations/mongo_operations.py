import json
import pandas as pd
from pymongo import MongoClient
from utils.read_params import read_params


class mongo_db_operation:
    """
    Description :   This method is used for all mongodb operations

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.DB_URL = self.config["mongodb"]["url"]

    def get_client(self):
        """
        Method Name :   get_client
        Description :   This method is used for getting the client from MongoDB

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_client.__name__

        try:
            self.client = MongoClient(self.DB_URL)

            return self.client

        except Exception as e:
            raise e
            raise e

    def create_db(self, client, db_name):
        """
        Method Name :   create_db
        Description :   This method is creating a database in MongoDB

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.create_db.__name__

        try:
            db = client[db_name]

            return db

        except Exception as e:
            raise e

    def create_collection(
        self,
        database,
        collection_name,
    ):
        """
        Method Name :   create_collection
        Description :   This method is used for creating a collection in created database

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.create_collection.__name__

        try:
            collection = database[collection_name]

            return collection

        except Exception as e:
            raise e

    def get_collection(self, collection_name, database):
        """
        Method Name :   get_collection
        Description :   This method is used for getting collection from a database

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_collection.__name__

        try:
            collection = self.create_collection(database, collection_name)

            return collection

        except Exception as e:
            raise e

    def get_collection_as_dataframe(self, db_name, collection_name):
        """
        Method Name :   get_collection_as_dataframe
        Description :   This method is used for converting the selected collection to dataframe

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_collection_as_dataframe.__name__

        try:
            client = self.get_client()

            database = self.create_db(client, db_name)

            collection = database.get_collection(name=collection_name)

            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            return df

        except Exception as e:
            raise e

    def insert_dataframe_as_record(
        self,
        data_frame,
        db_name,
        collection_name,
    ):
        """
        Method Name :   insert_dataframe_as_record
        Description :   This method is used for inserting the dataframe in collection as record

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.insert_dataframe_as_record.__name__

        try:
            records = json.loads(data_frame.T.to_json()).values()

            client = self.get_client()

            database = self.create_db(client, db_name)

            collection = database.get_collection(collection_name)

            collection.insert_many(records)

        except Exception as e:
            raise e

    def insert_one_record(self, db_name, collection_name, data):
        try:
            client = self.get_client()

            db = self.create_db(client=client, db_name=db_name)

            collection = db.get_collection(name=collection_name)

            collection.insert_one(data)

        except Exception as e:
            raise e
