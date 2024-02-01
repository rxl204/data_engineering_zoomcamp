import io
import pandas as pd
import requests
from pandas import DataFrame

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(**kwargs) -> DataFrame:
    dfs = []
    months = [10, 11, 12]
    for m in months:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{m}.csv.gz'
        df = pd.read_csv(url, compression='gzip')
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    return result_df

@test
def test_output(result_df) -> None:
    """
    Template code for testing the output of the block.
    """
    assert result_df is not None, 'The output is undefined'
    print(result_df.shape)
