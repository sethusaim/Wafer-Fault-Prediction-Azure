import json
import os
import pickle
from io import StringIO

import pandas as pd
from azure.storage.blob import BlobServiceClient, ContainerClient
from utils.logger import App_Logger
from utils.model_utils import get_model_name
from utils.read_params import read_params


class Blob_Operation:
    def __init__(self):
        self.config = read_params()

        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.class_name = self.__class__.__name__

        self.log_writer = App_Logger()

        self.model_save_format = self.config["model_utils"]["save_format"]

    def get_container_client(self, db_name, collection_name, container_name):
        method_name = self.get_container_client.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            container_client = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info="Got container client from connection string",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return container_client

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def get_blob_service_client(self, db_name, collection_name):
        method_name = self.get_blob_service_client.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            client = BlobServiceClient.from_connection_string(
                conn_str=self.connection_string
            )

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info="Got blob service client from connection string",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return client

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def load_file(self, container_name, file, db_name, collection_name):
        method_name = self.load_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            client = self.get_blob_service_client(
                db_name=db_name, collection_name=collection_name
            )

            blob_client = client.get_blob_client(container=container_name, blob=file)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info="Got blob client from blob service client",
            )

            f = blob_client.exists()

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"{file} file exists is {f}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return f

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def upload_file(
        self, db_name, collection_name, container_name, src_file, dest_file, remove=True
    ):
        method_name = self.upload_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            client = self.get_container_client(
                container_name=container_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Uploading {src_file} file to {container_name} container",
            )

            with open(file=src_file, mode="rb") as f:
                client.upload_blob(data=f, name=dest_file)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Uploaded {src_file} file to {container_name} container",
            )

            if remove is True:
                self.log_writer.log(
                    db_name=db_name,
                    collection_name=collection_name,
                    log_info=f"Option remove is set {remove}..deleting the {src_file} file",
                )

                os.remove(src_file)

                self.log_writer.log(
                    db_name=db_name,
                    collection_name=collection_name,
                    log_info=f"Removed the local copy of {src_file}",
                )

                self.log_writer.start_log(
                    key="exit",
                    class_name=self.class_name,
                    method_name=method_name,
                    db_name=db_name,
                    collection_name=collection_name,
                )

            else:
                self.log_writer.log(
                    db_name=db_name,
                    collection_name=collection_name,
                    log_info=f"Option remove is set {remove}, not deleting the file",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def delete_file(self, db_name, collection_name, container_name, file_name):
        method_name = self.delete_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            client = self.get_container_client(
                container_name=container_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            client.delete_blob(file_name)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Deleted {file_name} file from {container_name} container",
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def get_object(self, db_name, collection_name, container_name, file_name):
        method_name = self.get_object.__name__

        try:
            client = self.get_container_client(
                container_name=container_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            f = client.download_blob(blob=file_name)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Got {file_name} info from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return f

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def read_object(
        self, db_name, collection_name, object, decode=True, make_readable=False
    ):
        method_name = self.read_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            func = (
                lambda: object.readall().decode()
                if decode is True
                else object.readall()
            )

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Read {object} object with decode as {decode}",
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return conv_func()

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def read_text(self, db_name, collection_name, container_name, file_name):
        method_name = self.read_text.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            f_obj = self.get_object(
                container_name=container_name,
                file_name=file_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            content = self.read_object(
                object=f_obj, db_name=db_name, collection_name=collection_name
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return content

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def read_json(self, db_name, collection_name, container_name, file_name):
        method_name = self.read_json.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            f_obj = self.get_object(
                container_name=container_name,
                file_name=file_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            json_content = self.read_object(
                object=f_obj, db_name=db_name, collection_name=collection_name
            )

            dic = json.loads(json_content)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Read {file_name} json file from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return dic

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def get_df_from_object(self, db_name, collection_name, object):
        method_name = self.get_df_from_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            content = self.read_object(
                object=object,
                make_readable=True,
                db_name=db_name,
                collection_name=collection_name,
            )

            df = pd.read_csv(content)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Got dataframe from {object} object",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return df

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def get_files_from_folder(
        self, db_name, collection_name, container_name, folder_name
    ):
        try:
            client = self.get_container_client(
                container_name=container_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            folder = folder_name + "/"

            blob_list = client.list_blobs(name_starts_with=folder)

            f_name_lst = [f.name for f in blob_list]

            return f_name_lst

        except Exception as e:
            raise e

    def read_csv(
        self, db_name, collection_name, container_name, file_name, folder=False
    ):
        method_name = self.read_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            if folder is True:
                files = self.get_files_from_folder(
                    container_name=container_name,
                    folder_name=file_name,
                    db_name=db_name,
                    collection_name=collection_name,
                )

                lst = [
                    (
                        self.read_csv(
                            container_name=container_name,
                            file_name=f,
                            db_name=db_name,
                            collection_name=collection_name,
                        ),
                        f,
                        f.split("/")[-1],
                    )
                    for f in files
                ]

                self.log_writer.log(
                    db_name=db_name,
                    collection_name=collection_name,
                    log_info="Got list of tuples consisting of dataframe,file name and abs file name",
                )

                self.log_writer.exception_log(
                    error=e,
                    class_name=self.class_name,
                    method_name=method_name,
                    db_name=db_name,
                    collection_name=collection_name,
                )

                return lst

            else:
                csv_obj = self.get_object(
                    container_name=container_name,
                    file_name=file_name,
                    db_name=db_name,
                    collection_name=collection_name,
                )

                df = self.get_df_from_object(
                    object=csv_obj, db_name=db_name, collection_name=collection_name
                )

                self.log_writer.log(
                    db_name=db_name,
                    collection_name=collection_name,
                    log_info=f"Read {file_name} csv file from {container_name} container",
                )

                self.log_writer.exception_log(
                    error=e,
                    class_name=self.class_name,
                    method_name=method_name,
                    db_name=db_name,
                    collection_name=collection_name,
                )

                return df

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def get_blob_url(self, db_name, collection_name, container_name, file_name):
        method_name = self.get_blob_url.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            client = self.get_blob_service_client(
                db_name=db_name, collection_name=collection_name
            )

            blob_file = client.get_blob_client(container=container_name, blob=file_name)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Got {file_name} blob from {container_name} container",
            )

            f = blob_file.url

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Got {file_name} blob url",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return f

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def copy_data(
        self,
        db_name,
        collection_name,
        src_container_name,
        dest_container_name,
        src_file,
        dest_file,
    ):
        method_name = self.copy_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            dest_client = self.get_container_client(
                container_name=dest_container_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            src_blob = self.get_blob_url(
                container_name=src_container_name,
                file_name=src_file,
                db_name=db_name,
                collection_name=collection_name,
            )

            dest_blob = dest_client.get_blob_client(blob=dest_file)

            dest_blob.start_copy_from_url(src_blob)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Copied {src_file} file from {src_container_name} container to {dest_file} file from {dest_container_name}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def move_data(
        self,
        db_name,
        collection_name,
        src_container_name,
        dest_container_name,
        src_file,
        dest_file,
    ):
        method_name = self.move_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            self.copy_data(
                src_container_name=src_container_name,
                dest_container_name=dest_container_name,
                src_file=src_file,
                dest_file=dest_file,
                db_name=db_name,
                collection_name=collection_name,
            )

            self.delete_file(
                container_name=src_container_name,
                file_name=src_file,
                db_name=db_name,
                collection_name=collection_name,
            )

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Moved {src_file} file from {src_container_name} container to {dest_container_name} container,with {dest_file} file as name",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def load_model(
        self, db_name, collection_name, container_name, model_name, model_dir=None
    ):
        method_name = self.load_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            func = (
                lambda: model_name + self.model_save_format
                if model_dir is None
                else model_dir + model_name + self.model_save_format
            )

            model_file = func()

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Got {model_file} as model file",
            )

            f_obj = self.get_object(
                container_name=container_name,
                file_name=model_file,
                db_name=db_name,
                collection_name=collection_name,
            )

            model_content = self.read_object(
                object=f_obj,
                decode=False,
                db_name=db_name,
                collection_name=collection_name,
            )

            model = pickle.loads(model_content)

            self.log_writer.log(
                db_name=self.class_name,
                collection_name=collection_name,
                log_info=f"Loaded {model_name} model from {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            return model

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def save_model(
        self, db_name, collection_name, container_name, model, idx, model_dir
    ):
        method_name = self.save_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            model_name = get_model_name(
                model=model, db_name=db_name, collection_name=collection_name
            )

            func = (
                lambda: model_name + self.model_save_format
                if model_name == "KMeans"
                else model_name + str(idx) + self.model_save_format
            )

            model_file = func()

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Local copy of {model_name} model file name is created",
            )

            dir_func = (
                lambda: model_dir + "/" + model_file
                if model_dir is not None
                else model_file
            )

            container_model_file = dir_func()

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Container location of {model_name} model file name is created ",
            )

            with open(file=model_file, mode="wb") as f:
                pickle.dump(model, f)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Saved local copy of {model_name} model",
            )

            self.upload_file(
                container_name=container_name,
                src_file=model_file,
                dest_file=container_model_file,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

    def upload_df_as_csv(
        self,
        db_name,
        collection_name,
        container_name,
        dataframe,
        file_name,
        container_file_name,
    ):
        method_name = self.upload_df_as_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        try:
            dataframe.to_csv(file_name, index=None, header=True)

            self.log_writer.log(
                db_name=db_name,
                collection_name=collection_name,
                log_info=f"Created a local copy of dataframe with name {file_name}",
            )

            self.upload_file(
                container_name=container_name,
                file_name=container_file_name,
                db_name=db_name,
                collection_name=collection_name,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=db_name,
                collection_name=collection_name,
            )
