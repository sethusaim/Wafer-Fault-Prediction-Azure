import json
import os
import pickle
from io import StringIO

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from utils.logger import app_logger
from utils.read_params import read_params


class blob_operation:
    def __init__(self, db_name, collection_name):
        self.config = read_params()

        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.class_name = self.__class__.__name__

        self.log_writer = app_logger()

        self.model_save_format = self.config["model_utils"]["save_format"]

        self.db_name = db_name

        self.collection_name = collection_name

    def get_container_client(self, container_name):
        method_name = self.get_container_client.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            container_client = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Got container client from connection string",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return container_client

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_blob_service_client(self):
        method_name = self.get_blob_service_client.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Got blob service client from connection string",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return client

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def create_azure_container(self, container_name):
        method_name = self.create_azure_container.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            client = self.get_container_client(
                container_name=container_name,
            )

            if client.exists() is True:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"{container_name} container already exists, skipping the creation of container",
                )

            else:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"{container_name} container does not exists. Creating a new container",
                )

                client.create_container()

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"Created {container_name} container in azure",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def upload_file(self, container_name, file_name, create_container=False):
        method_name = self.upload_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            if create_container is True:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"Create_container is set to {create_container}. Creating a new container with name as {container_name}",
                )

                self.create_azure_container(container_name=container_name)

            else:
                pass

            client = self.get_container_client(
                container_name=container_name,
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Uploading {file_name} file to {container_name} container",
            )

            with open(file=file_name, mode="rb") as f:
                client.upload_blob(data=f, name=file_name)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Uploaded {file_name} file to {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def delete_file(self, container_name, file_name):
        method_name = self.delete_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            client = self.get_container_client(container_name=container_name)

            client.delete_blob(file_name)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Deleted {file_name} file from {container_name} container",
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_object(self, container_name, file_name):

        method_name = self.get_object.__name__

        try:
            client = self.get_container_client(container_name=container_name)

            f = client.download_blob(blob=file_name)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Got {file_name} info from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return f

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def read_object(self, object, decode=True, make_readable=False):
        method_name = self.read_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            func = (
                lambda: object.readall().decode()
                if decode is True
                else object.readall()
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Read {object} object with decode as {decode}",
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return conv_func()

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def read_text(self, container_name, file_name):
        method_name = self.read_text.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            f_obj = self.get_object(container_name=container_name, file_name=file_name)

            content = self.read_object(object=f_obj)

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return content

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def read_json(self, container_name, file_name):
        method_name = self.read_json.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            f_obj = self.get_object(container_name=container_name, file_name=file_name)

            json_content = self.read_object(object=f_obj)

            dic = json.loads(json_content)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Read {file_name} json file from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return dic

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_df_from_object(self, object):
        method_name = self.get_df_from_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            content = self.read_object(object=object, make_readable=True)

            df = pd.read_csv(content)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Got dataframe from {object} object",
            )

            return df

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def read_csv(self, container_name, file_name):
        method_name = self.read_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            f_obj = self.get_object(container_name=container_name, file_name=file_name)

            df = self.get_df_from_object(object=f_obj)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Read {file_name} csv file from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return df

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_blob_url(self, container_name, file_name):
        method_name = self.get_blob_url.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            client = self.get_blob_service_client()

            blob_file = client.get_blob_client(container=container_name, blob=file_name)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Got {file_name} blob from {container_name} container",
            )

            f = blob_file.url

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Got {file_name} blob url",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return f

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def copy_data(self, src_container_name, dest_container_name, src_file, dest_file):
        method_name = self.copy_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            dest_client = self.get_container_client(container_name=dest_container_name)

            src_blob = self.get_blob_url(
                container_name=src_container_name, file_name=src_file
            )

            dest_blob = dest_client.get_blob_client(blob=dest_file)

            dest_blob.start_copy_from_url(src_blob)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Copied {src_file} file from {src_container_name} container to {dest_file} file from {dest_container_name}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def move_data(self, src_container_name, dest_container_name, src_file, dest_file):
        method_name = self.move_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            self.copy_data(
                src_container_name=src_container_name,
                dest_container_name=dest_container_name,
                src_file=src_file,
                dest_file=dest_file,
            )

            self.delete_file(container_name=src_container_name, file_name=src_file)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Moved {src_file} file from {src_container_name} container to {dest_container_name} container,with {dest_file} file as name",
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def load_model(self, container_name, model_name):
        method_name = self.load_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            model_file = model_name + self.model_save_format

            f_obj = self.get_object(
                container_name=container_name, file_name=model_file
            )

            model_content = self.read_object(object=f_obj, decode=False)

            model = pickle.loads(model_content)

            self.log_writer.log(
                db_name=self.class_name,
                collection_name=self.collection_name,
                log_info=f"Loaded {model_name} model from {container_name} container",
            )

            return model

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )
