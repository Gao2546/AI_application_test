# import os

# # Replace 'my_dag_id' with the actual DAG ID
# dag_id = "my_dag_id"

# # Use the os.system command to trigger the DAG via CLI
# os.system(f"airflow dags trigger {dag_id}")

import requests
spark_master = "spark://spark:7077"

# Replace these with your Airflow URL and DAG ID
airflow_url = "http://localhost:8085/api/v1/dags/spark-test/dagRuns"
dag_id = "spark-test"

# Optional: Configuration parameters for the DAG run
config = {
    "conf": {
        "spark.master":spark_master
    }
}
config = None

# Make the POST request to trigger the DAG
response = requests.post(airflow_url, json=config, auth=('airflow', 'airflow'))

# Check the response
if response.status_code == 200:
    print("DAG triggered successfully!")
else:
    print(f"Failed to trigger DAG: {response.text}")