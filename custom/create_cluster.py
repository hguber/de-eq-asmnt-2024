from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import re

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def spark_processing(*args, **kwargs):
    """
    Creates a dataproc cluster, submits a job, and deletes the cluster. 
    """
    #year = kwargs['year']
    #month = kwargs['month']
    #bucket_name = kwargs['bucket']
    #object_key = kwargs['name']

    # Create the cluster client.
    print('hello')
    c_o = {"api_endpoint" : kwargs['region'] +"-dataproc.googleapis.com:443"}
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint" : kwargs['region'] +"-dataproc.googleapis.com:443"}
    )
    # Create the cluster config.
    cluster = {
        "project_id": kwargs['project_id'] ,
        "cluster_name": kwargs['cluster_name'],
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }
    print('reached here')
    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": kwargs['project_id'], "region": kwargs['region'], "cluster": cluster}
    )
    result = operation.result()
    print(result)

    print("Cluster created successfully: ", result.cluster_name)

    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint" : kwargs['region'] +"-dataproc.googleapis.com:443"}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": kwargs['cluster_name']},
        "pyspark_job": {"main_python_file_uri": 'gs://eq_spark_scripts/eq_spark_script.py',
                        "jar_file_uris" : ['gs://eq_event_jars/lib/gcs-connector-hadoop3-2.2.5.jar', 
                                           'gs://eq_event_jars/lib/spark-bigquery-with-dependencies_2.12-0.23.2.jar']},
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": kwargs['project_id'], "region": kwargs['region'], "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}")

    operation = cluster_client.delete_cluster(
        request={
            "project_id": kwargs['project_id'],
            "region": kwargs['region'],
            "cluster_name": kwargs['cluster_name'],
        }
    )
    operation.result()

    print("Cluster " +kwargs['cluster_name'] + " successfully deleted.")


