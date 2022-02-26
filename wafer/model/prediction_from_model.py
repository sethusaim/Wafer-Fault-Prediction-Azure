import pandas as pd
from utils.logger import app_logger
from utils.read_params import read_params
from wafer.data_ingestion.data_loader_prediction import data_getter_pred
from wafer.data_preprocessing.preprocessing import preprocessor
from wafer.blob_storage_operations.blob_operations import blob_operation


class Prediction:
    """
    Description :   This class shall be used for loading the production model

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.db_name = self.config["db_log"]["pred"]

        self.pred_log = self.config["pred_db_log"]["pred_main"]

        self.model_container = self.config["train_container"]["wafer_model_container"]

        self.input_files = self.config["train_container"]["inputs_files_container"]

        self.prod_model_dir = self.config["models_dir"]["prod"]

        self.pred_output_file = self.config["pred_output_file"]

        self.log_writer = app_logger()

        self.blob = blob_operation()

        self.data_getter_pred = data_getter_pred(table_name=self.pred_log)

        self.preprocessor = preprocessor(table_name=self.pred_log)

        self.class_name = self.__class__.__name__

    def delete_pred_file(self):
        """
        Method Name :   delete_pred_file
        Description :   This method is used for deleting the existing prediction batch file

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.delete_pred_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            f = self.blob.load_file(
                container_name=self.input_files,
                file=self.pred_output_file,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

            if f is True:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    log_info="Found existing prediction batch file. Deleting it.",
                )

                self.blob.delete_file(
                    container_name=self.input_files,
                    file=self.pred_output_file,
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                )

            else:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    log_info="Previous prediction file is not found, not deleting it",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )
        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

    def find_correct_model_file(self, cluster_number, container_name):
        """
        Method Name :   find_correct_model_file
        Description :   This method is used for finding the correct model file during prediction

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.find_correct_model_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            list_of_files = self.blob.get_files_from_folder(
                db_name=self.db_name,
                collection_name=self.pred_log,
                container_name=container_name,
                folder_name=self.prod_model_dir,
            )

            for file in list_of_files:
                try:
                    if file.index(str(cluster_number)) != -1:
                        model_name = file

                except:
                    continue

            model_name = model_name.split(".")[0]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_log,
                log_info=f"Got {model_name} from {self.prod_model_dir} folder in {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

            return model_name

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

    def predict_from_model(self):
        """
        Method Name :   predict_from_model
        Description :   This method is used for loading from prod model dir of blob container and use them for prediction

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.predict_from_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            self.delete_pred_file()

            data = self.data_getter_pred.get_data()

            is_null_present = self.preprocessor.is_null_present(data)

            if is_null_present:
                data = self.preprocessor.impute_missing_values(data)

            cols_to_drop = self.preprocessor.get_columns_with_zero_std_deviation(data)

            data = self.preprocessor.remove_columns(data, cols_to_drop)

            kmeans = self.blob.load_model(
                db_name=self.db_name,
                collection_name=self.pred_log,
                container_name=self.model_container,
                model_name="KMeans",
                model_dir=self.prod_model_dir,
            )

            clusters = kmeans.predict(data.drop(["Wafer"], axis=1))

            data["clusters"] = clusters

            clusters = data["clusters"].unique()

            for i in clusters:
                cluster_data = data[data["clusters"] == i]

                wafer_names = list(cluster_data["Wafer"])

                cluster_data = data.drop(labels=["Wafer"], axis=1)

                cluster_data = cluster_data.drop(["clusters"], axis=1)

                crt_model_name = self.find_correct_model_file(
                    cluster_number=i,
                    container_name=self.model_container,
                )

                model = self.blob.load_model(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    container_name=self.model_container,
                    model_name=crt_model_name,
                    model_dir=self.prod_model_dir,
                )

                result = list(model.predict(cluster_data))

                result = pd.DataFrame(
                    list(zip(wafer_names, result)), columns=["Wafer", "Prediction"]
                )

                self.blob.upload_df_as_csv(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    container_file_name=self.input_files,
                    dataframe=result,
                    file_name=self.pred_output_file,
                )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_log,
                log_info=f"End of Prediction",
            )

            return (
                self.input_files,
                self.pred_output_file,
                result.head().to_json(orient="records"),
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )
