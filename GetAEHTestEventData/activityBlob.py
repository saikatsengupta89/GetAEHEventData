import logging
from azure.storage.filedatalake import DataLakeServiceClient

class activityBlob:

    def initialize_storage_account(storage_account_name, storage_account_key):
        
        try:
            global service_client
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=storage_account_key)
            # print(service_client)
        except Exception as e:
            logging.info("Initialize storage Exception: ", e)

    def list_directory_contents(container_name):
        try:
            file_system_client = service_client.get_file_system_client(file_system=container_name)
            paths = file_system_client.get_paths(path="custom")
            for path in paths:
                print(path.name + '\n')

        except Exception as e:
            logging.info("List Directory Exception: ", e)

    def upload_file_to_directory(dataset, container_name, location_rdh, sequence_number):
        try:
            file_system_client = service_client.get_file_system_client(file_system=container_name)
            directory_client = file_system_client.get_directory_client(location_rdh)
            file_client = directory_client.create_file(f"test_data_{sequence_number}.json")

            file_contents = str.encode(dataset)

            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))

        except Exception as e:
            logging.info("Upload File Exception: ", e)