import pandas as pd
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(f'Rows with zero passengers: {data["passenger_count"].isin([0]).sum()}')
    # Convert 'lpep_pickup_datetime' to datetime if it's not already
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    # filter passenger count is greater than 0 and the trip distance is greater than zero
    data = data[(data['passenger_count']>0)&(data['trip_distance']>0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    return data

# Function to check if a string is in snake case
def is_snake_case(s):
    return s.islower() and '_' in s


@test
def test_output(output, *args) -> None:
    print(output.shape)
    print(output['VendorID'].unique())
    ## Find columns not in snake case
    columns_not_in_snake_case = [col for col in output.columns if not is_snake_case(col)]
    print(columns_not_in_snake_case)
    # List comprehension to rename columns to snake case
    output.columns = [col if is_snake_case(col) else re.sub(r'([a-z])([A-Z])', r'\1_\2', col).lower() for col in output.columns]
    print(output.columns)

    assert output['passenger_count'].isin([0]).sum()==0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum()==0, 'There are rides with zero trip distance'
    assert 'vendor_id' in output.columns, "'vendor_id' is not present in the DataFrame columns."
