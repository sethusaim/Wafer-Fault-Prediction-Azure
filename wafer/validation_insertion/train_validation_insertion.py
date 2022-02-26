from utils.logger import app_logger
from utils.read_params import read_params
from wafer.data_transform.data_transformation_train import data_transform_train
from wafer.data_type_valid.data_type_valid_train import db_operation_train
from wafer.raw_data_validation.train_data_validation import raw_train_data_validation


class Train_Validation:
    """
    Description :   This class is used for validating all the training batch files

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self, container_name):
        self.raw_data = raw_train_data_validation(
            raw_data_container_name=container_name
        )

        self.data_transform = data_transform_train()

        self.db_operation = db_operation_train()

        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.db_name = self.config["db_log"]["train"]

        self.train_main_log = self.config["train_db_log"]["train_main"]

        self.good_data_db_name = self.config["mongodb"]["train"]["db"]

        self.good_data_collection_name = self.config["mongodb"]["train"]["collection"]

        self.log_writer = app_logger()

    def training_validation(self):
        """
        Method Name :   training_validation
        Description :   This method is used for validating the training batch files

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.training_validation.__name__

        try:
            self.log_writer.start_log(
                key="start",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_main_log,
            )

            (
                LengthOfDateStampInFile,
                LengthOfTimeStampInFile,
                column_names,
                noofcolumns,
            ) = self.raw_data.values_from_schema()

            regex = self.raw_data.get_regex_pattern()

            self.raw_data.validate_raw_file_name(
                regex, LengthOfDateStampInFile, LengthOfTimeStampInFile
            )

            self.raw_data.validate_col_length(NumberofColumns=noofcolumns)

            self.raw_data.validate_missing_values_in_col()

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.train_main_log,
                log_info="Raw Data Validation Completed !!",
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.train_main_log,
                log_info="Starting Data Transformation",
            )

            self.data_transform.rename_target_column()

            self.data_transform.replace_missing_with_null()

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.train_main_log,
                log_info="Data Transformation completed !!",
            )

            self.db_operation.insert_good_data_as_record(
                db_name=self.good_data_db_name,
                collection_name=self.good_data_collection_name,
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.train_main_log,
                log_info="Data type validation Operation completed !!",
            )

            self.db_operation.export_collection_to_csv(
                db_name=self.good_data_db_name,
                collection_name=self.good_data_collection_name,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_main_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.train_main_log,
            )
