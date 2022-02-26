import os
from azure.storage.blob import ContainerClient
from utils.read_params import read_params


class Azure_Container:
    def __init__(self):
        self.config = read_params()

        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.containers = list(self.config["container"].values())

        self.class_name = self.__class__.__name__

    def create_container(self, container_name):
        method_name = self.create_container.__name__

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

    def generate_containers(self, type):
        """
        Method Name :   generate_container
        Description :   This method is used for creating the container for log

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.generate_containers.__name__

        try:
            for container in self.containers:
                self.create_container(container_name=container)

        except Exception as e:
            error_msg = f"Exception occured in Class : {self.class_name}, Method : {method_name}, Error : {str(e)}"

            raise Exception(error_msg)
