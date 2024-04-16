if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint" : kwargs['region'] +"-dataproc.googleapis.com:443"}
        
    operation = cluster_client.delete_cluster(
        request={
            "project_id": kwargs['project_id'],
            "region": kwargs['region'],
            "cluster_name": kwargs['cluster_na'],
        }
    )
    operation.result()

    print("Cluster {} successfully deleted.".format(cluster_name))

    return {}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
