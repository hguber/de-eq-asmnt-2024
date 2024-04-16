from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage

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

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": kwargs['project_id'], "region": kwargs['region'], "cluster": cluster}
    )
    result = operation.result()

    print("Cluster created successfully: ", result.cluster_name)

    operation = cluster_client.delete_cluster(
        request={
            "project_id": kwargs['project_id'],
            "region": kwargs['region'],
            "cluster_name": kwargs['cluster_name'],
        }
    )
    operation.result()

    print("Cluster " + cluster_name + " successfully deleted.")


