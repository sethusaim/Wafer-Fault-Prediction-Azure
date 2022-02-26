from utils.logger import app_logger
from utils.read_params import read_params
from wafer.blob_storage_operations.blob_operations import blob_operation


class data_getter_train:
    """
    Description :   This class shall be used for obtaining the df from the source for training
    Version     :   1.2
    Revisions   :   Moved to setup to cloud
    """

    def __init__(self, db_name, collection_name):
        self.config = read_params()

        self.db_name = db_name

        self.collection_name = collection_name

        self.train_csv_file = self.config["export_csv_file"]["train"]

        self.input_files = self.config["train_container"]["input_files"]

        self.blob = blob_operation()

        self.log_writer = app_logger()

        self.class_name = self.__class__.__name__

    def get_data(self):
        """
        Method Name :   get_data
        Description :   This method reads the data from the source
        Output      :   A pandas dataframe
        On failure  :   Raise Exception
        Written by  :   iNeuron Intelligence
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            df = self.blob.read_csv(
                container_name=self.input_files,
                file_name=self.train_csv_file,
                db_name=self.db_name,
                collection_name=self.collection_name,
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
