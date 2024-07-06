# def read_data_from_hdfs():
#     # Replace with the actual hostname or service name of your Hadoop container
#     client = InsecureClient('http://namenode:50070')

#     # HDFS file path from where you want to read the file
#     hdfs_file_path = f'/hadoop/dfs/data/consommation-annuelle-residentielle_{date}.json'

#      # Download the file from HDFS to a local temporary path
#      local_file_path = f'/opt/airflow/dags/data/in/consommation-annuelle-residentielle_{date}.json'
#       client.download(
#            hdfs_file_path, local_file_path, overwrite=True)

#        # Read the contents of the file and log it
#        with open(local_file_path, 'r') as file:
#             data = json.load(file)
#             logger.info(f"Data read from HDFS: {data}")

#         return data
