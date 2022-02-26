from utils.logger import app_logger
from utils.read_params import read_params
from wafer.blob_storage_operations.blob_operations import blob_operation
from wafer.mongo_db_operations.mongo_operations import mongo_db_operation


class db_operation_train:
    """
    Description :    This class shall be used for handling all the db operations

    Version     :    1.2
    Revisions   :    moved setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.db_name = self.config["db_log"]["db_train_log"]

        self.train_data_container = self.config["blob_container"][
            "wafer_train_data_container"
        ]

        self.train_export_csv_file = self.config["export_csv_file"]["train"]

        self.good_data_train_dir = self.config["data"]["train"]["good_data_dir"]

        self.input_files_container = self.config["blob_container"][
            "input_files_container"
        ]

        self.train_db_insert_log = self.config["train_db_log"]["db_insert"]

        self.train_export_csv_log = self.config["train_db_log"]["export_csv"]

        self.blob = blob_operation()

        self.db_op = mongo_db_operation()

        self.log_writer = app_logger()

    def insert_good_data_as_record(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   insert_good_data_as_record
        Description :   This method inserts the good data in MongoDB as collection

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.insert_good_data_as_record.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.train_db_insert_log,
        )

        try:
            lst = self.blob.read_csv(
                db_name=self.db_name,
                collection_name=self.train_db_insert_log,
                container_name=self.train_data_container,
                file_name=self.good_data_train_dir,
                folder=True,
            )

            for idx, f in enumerate(lst):
                df = f[idx][1]

                file = f[idx][2]

                if file.endswith(".csv"):
                    self.db_op.insert_dataframe_as_record(
                        data_frame=df,
                        db_name=good_data_db_name,
                        collection_name=good_data_collection_name,
                    )

                else:
                    pass

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.train_db_insert_log,
                    log_info="Inserted dataframe as collection record in mongodb",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_db_insert_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_db_insert_log,
            )

    def export_collection_to_csv(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   export_collection_to_csv

        Description :   This method extracts the inserted data to csv file, which will be used for training
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.export_collection_to_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.train_export_csv_log,
        )

        try:
            df = self.db_op.get_collection_as_dataframe(
                db_name=good_data_db_name,
                collection_name=good_data_collection_name,
            )

            self.blob.upload_df_as_csv(
                data_frame=df,
                file_name=self.train_export_csv_file,
                container=self.input_files_container,
                dest_file_name=self.train_export_csv_file,
                db_name=self.db_name,
                collection_name=self.train_export_csv_log,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_export_csv_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_export_csv_log,
            )
