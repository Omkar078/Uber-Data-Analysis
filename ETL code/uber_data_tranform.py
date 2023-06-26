import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
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
    # Specify your transformation logic here
    
    # datetime table
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    Datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop = True)
    Datetime_dim['pick_hour'] = df['tpep_pickup_datetime'].dt.hour
    Datetime_dim['pick_day'] = df['tpep_pickup_datetime'].dt.day
    Datetime_dim['pick_month'] = df['tpep_pickup_datetime'].dt.month
    Datetime_dim['pick_year'] = df['tpep_pickup_datetime'].dt.year
    Datetime_dim['pick_weekday'] = df['tpep_pickup_datetime'].dt.weekday
    Datetime_dim['drop_hour'] = df['tpep_dropoff_datetime'].dt.hour
    Datetime_dim['drop_day'] = df['tpep_dropoff_datetime'].dt.day
    Datetime_dim['drop_month'] = df['tpep_dropoff_datetime'].dt.month
    Datetime_dim['drop_year'] = df['tpep_dropoff_datetime'].dt.year
    Datetime_dim['drop_hour'] = df['tpep_dropoff_datetime'].dt.hour
    Datetime_dim['drop_weekday'] = df['tpep_dropoff_datetime'].dt.weekday
    Datetime_dim['datetime_id']  = Datetime_dim.index
    Datetime_dim = Datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                             'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    # pick location table

    pickloc_dim = df[['pickup_longitude','pickup_latitude']].reset_index(drop = True)
    pickloc_dim['pick_location_id'] = pickloc_dim.index
    pickloc_dim = pickloc_dim[['pick_location_id', 'pickup_longitude', 'pickup_latitude']]

    # drop location table

    droploc_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop = True)
    droploc_dim['drop_location_id'] = droploc_dim.index
    droploc_dim = droploc_dim[['drop_location_id', 'dropoff_longitude', 'dropoff_latitude']]

    # passenger count table

    passcount_dim = df[['passenger_count']].reset_index(drop = True)
    passcount_dim['passenger_count_id'] = passcount_dim.index
    passcount_dim = passcount_dim[['passenger_count_id', 'passenger_count']]

    # trip distance table

    tripdist_dim = df[['trip_distance']].reset_index(drop = True)
    tripdist_dim['trip_distance_id'] = tripdist_dim.index
    tripdist_dim = tripdist_dim[['trip_distance_id', 'trip_distance']]

    # rate code table

    ratecode_type = {
    1 : "Standard rate",
    2 : "JFK",
    3 : "Newark",
    4 : "Nassau or Westchester",
    5 : "Negotiated fare",
    6 :  "Group ride",
    }
    ratecode_dim = df[['RatecodeID']].reset_index(drop = True)
    ratecode_dim['rate_code_id'] = ratecode_dim.index
    ratecode_dim['rate_code_name'] = ratecode_dim['RatecodeID'].map(ratecode_type)
    ratecode_dim = ratecode_dim[['rate_code_id','RatecodeID', 'rate_code_name']]

    # payment type table

    paytype_name = {
    1 : "Credit card",
    2 : "Cash",
    3 : "No charge",
    4 : "Dispute",
    5 : "Unknown",
    6 : "Voided trip"
    }
    paytype_dim = df[['payment_type']].reset_index(drop = True)
    paytype_dim['payment_type_id'] = paytype_dim.index
    paytype_dim['payment_type_name'] = paytype_dim['payment_type'].map(paytype_name)
    paytype_dim = paytype_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

    # fact table

    fact_table = df.merge(passcount_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(tripdist_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(ratecode_dim, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickloc_dim, left_on='trip_id', right_on='pick_location_id') \
             .merge(droploc_dim, left_on='trip_id', right_on='drop_location_id')\
             .merge(Datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(paytype_dim, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pick_location_id', 'drop_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]

    print(fact_table)
    return 'success'


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

