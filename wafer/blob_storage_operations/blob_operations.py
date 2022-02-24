import os
from azure.storage.blob import ContainerClient


class blob_operation:
    def __init__(self):
        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.class_name = self.__class__.__name__

    def create_azure_container(self, container_name):
        try:
            container_client = ContainerClient.from_connection_string(
                conn_str=self.connection_string,
                container_name=container_name,
            )

            container_client.create_container()

        except Exception as e:
            raise e
