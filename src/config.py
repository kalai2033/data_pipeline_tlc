DEFAULT_LOGGING_PATH = '../tests/pipeline.log'
DEFAULT_LOGGING_FORMAT = '%(asctime)s : %(levelname)s : %(name)s : %(message)s'
DATASET_PATH = '../data/yellow_tripdata_2021-**.csv'
SCHEMA = {'trip_distance': 'float64', 'PULocationID': 'uint16', 'DOLocationID': 'uint16',
          'store_and_fwd_flag': 'str', 'fare_amount': 'float64', 'extra': 'float64', 'mta_tax': 'float64',
          'tip_amount': 'float64', 'tolls_amount': 'float64', 'improvement_surcharge': 'float64',
          'total_amount': 'float64', 'congestion_surcharge': 'float64', 'RatecodeID': 'uint8',
          'VendorID': 'uint8', 'passenger_count': 'uint8', 'payment_type': 'uint8'}
DATE_FIELDS = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
DATASET_YEAR = [2021]
