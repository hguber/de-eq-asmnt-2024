import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    event_dtypes = {
        'event_id' : str,
        'location' : str,
        'latitude' : float,
        'longitude' : float,
        'depth' : float,
        'magnitude' : float,
        'significance' : pd.Int64Dtype(),
        'alert' : str, 
        'url' : str,
        'eventtype' : str,
        'country' : str,
    }

    df = data.astype(event_dtypes)
    df['date'] = df['datetime'].dt.date
    df['timestamp'] = df['datetime'].dt.time
    df = df.drop(['url','eventtype'], axis=1)

    conditions = [
        (df['magnitude'] >= 1) & (df['magnitude'] < 3),
        (df['magnitude'] >= 3) & (df['magnitude'] < 4),
        (df['magnitude'] >= 4) & (df['magnitude'] < 5),
        (df['magnitude'] >= 5) & (df['magnitude'] < 6),
        (df['magnitude'] >= 6) & (df['magnitude'] < 7),
        (df['magnitude'] >= 7) & (df['magnitude'] < 8),
        (df['magnitude'] >= 8)]

    values = ['micro', 'minor', 'light', 'moderate', 'strong', 'major', 'great']

    df['level'] = np.select(conditions, values)
    return df.drop_duplicates()

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_duplicates(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output) == len(output.drop_duplicates()), 'There are duplicate rows in the dataset'
