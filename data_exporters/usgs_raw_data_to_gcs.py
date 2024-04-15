from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
#f'eq_events/eq_events_{year}/eq_events_{year}_{month}.csv'
    year = str(df['datetime'].iloc[0])[0:4]
    month = str(df['datetime'].iloc[0])[5:7]
    bucket_name = 'de-eq-asmnt-2024-raw-bucket'
    object_key = 'eq_events/eq_events_' + year + '/' + 'eq_events_' + year + '_' + month + '.csv'
    #'de-eq-asmnt-2024-raw-bucket/eq_events/eq_events_2024'
    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )
