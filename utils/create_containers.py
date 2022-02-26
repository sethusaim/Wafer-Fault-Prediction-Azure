import os
from azure.storage.blob import ContainerClient
from utils.read_params import read_params


class create_log_container:
    def __init__(self):
        self.config = read_params()

        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.train_containers = list(self.config["train_db_log"].values())

        self.pred_containers = list(self.config["pred_db_log"].values())

        self.class_name = self.__class__.__name__

    def create_azure_container(self, container_name):
        method_name = self.create_azure_container.__name__

        try:
            client = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )

            if client.exists() is True:
                pass

            else:
                client.create_container()

        except Exception as e:
            error_msg = f"Exception occured in Class : {self.class_name}, Method : {method_name}, Error : {str(e)}"

            raise Exception(error_msg)

    def create_log_container(self):
        """
        Method Name :   create_log_container
        Description :   This method is used for creating the container for log

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.create_log_container.__name__

        try:
            for container in self.train_containers:
                self.create_azure_container(container_name=container)

        except Exception as e:
            error_msg = f"Exception occured in Class : {self.class_name}, Method : {method_name}, Error : {str(e)}"

            raise Exception(error_msg)

    def generate_log_containers(self, type):
        """
        Method Name :   generate_container
        Description :   This method is used for creating the container for log

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.generate_log_containers.__name__

        try:
            func = (
                lambda: self.train_containers
                if type == "train"
                else self.pred_containers
            )

            containers = func()

            for container in containers:
                self.create_log_container(container_name=container)

        except Exception as e:
            error_msg = f"Exception occured in Class : {self.class_name}, Method : {method_name}, Error : {str(e)}"

            raise Exception(error_msg)
