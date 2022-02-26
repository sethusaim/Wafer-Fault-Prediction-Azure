import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer
from utils.logger import app_logger
from utils.read_params import read_params
from wafer.blob_storage_operations.blob_operations import blob_operation


class preprocessor:
    """
    Description :   This class shall be used to clean and transform the data before training
    Written by  :   iNeuron Intelligence
    Version     :   1.2
    Revisions   :   Moved to setup to cloud
    """

    def __init__(self, db_name, collection_name):
        self.db_name = db_name

        self.collection_name = collection_name

        self.config = read_params()

        self.blob = blob_operation()

        self.input_files_container = self.config["blob_container"][
            "input_files_container"
        ]

        self.null_values_file = self.config["null_values_csv_file"]

        self.knn_n_neighbors = self.config["knn_imputer"]["n_neighbors"]

        self.knn_weights = self.config["knn_imputer"]["weights"]

        self.log_writer = app_logger()

        self.class_name = self.__class__.__name__

    def remove_columns(self, data, columns):
        """
        Method Name :   remove_columns
        Description :   This method removes the given columns from a pandas dataframe
        Output      :   A pandas dataframe after the removing the specified columns
        On failure  :   Raise Exception
        Written by  :   iNeuron Intelligence
        Version     :   1.2
        Revisions   :   Modified code based on the params.yaml file
        """
        method_name = self.remove_columns.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        self.data = data

        self.columns = columns

        try:
            self.useful_data = self.data.drop(labels=self.columns, axis=1)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Column removal Successful",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return self.useful_data

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def separate_label_feature(self, data, label_column_name):
        """
        Method name :   separate_label_feature
        Description :   This method separates the features and a label columns
        Output      :   Returns two separate dataframe, one containing features and other containing labels
        On failure  :   Raise Exception

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.separate_label_feature.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            self.X = data.drop(labels=label_column_name, axis=1)

            self.Y = data[label_column_name]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Label Separation Successful",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return self.X, self.Y

        except Exception as e:
            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Label Separation Unsuccessful",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def is_null_present(self, data):
        """
        Method name :   is_null_present
        Description :   This method checks whether there are null values present in the pandas
                        dataframe or not
        Output      :   Returns a boolean value. True if null is present in the dataframe, False they are
                        not present
        On failure  :   1.1
        Revisions   :   moved setup to cloud
        """
        method_name = self.is_null_present.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        self.null_present = False

        try:
            self.null_counts = data.isna().sum()

            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break

            if self.null_present:
                null_df = pd.DataFrame()

                null_df["columns"] = data.columns

                null_df["missing values count"] = np.asarray(data.isna().sum())

                self.blob.upload_df_as_csv(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    container_name=self.input_files_container,
                    dataframe=null_df,
                    file_name=self.null_values_file,
                    container_file_name=self.null_values_file,
                )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Finding missing values is a success.Data written to the null values file",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return self.null_present

        except Exception as e:
            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Finding missing values failed",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def impute_missing_values(self, data):
        """
        Method Name :   impute_missing_values
        Desrciption :   This method  replaces all the missing values in th dataframe using KNN imputer
        Output      :   A dataframe which has all missing values imputed
        On failure  :   Raise Exception
        Written by  :   iNeuron Intelligence

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.impute_missing_values.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        self.data = data

        try:
            imputer = KNNImputer(
                n_neighbors=self.knn_n_neighbors,
                weights=self.knn_weights,
                missing_values=np.nan,
            )

            self.new_array = imputer.fit_transform(self.data)

            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Imputing missing values Successful",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return self.new_data

        except Exception as e:
            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Imputing missing values failed",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_columns_with_zero_std_deviation(self, data):
        """
        Method Name :   get_columns_with_zero_std_deviation
        Description :   This method replaces all the missing values in the dataframe using KNN imputer
        Output      :   a dataframe which has all missing values imputed
        On failure  :   Raise Exception
        Written by  :   iNeuron Intelligence
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_columns_with_zero_std_deviation.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        self.data_n = data.describe()

        try:
            self.col_to_drop = [x for x in data.columns if self.data_n[x]["std"] == 0]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Column search for Standard Deviation of Zero Successful.",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return self.col_to_drop

        except Exception as e:
            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info="Column search for Standard Deviation of Zero Failed.",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )
