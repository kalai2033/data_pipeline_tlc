import logging
import os

import dask.dataframe as dd
from dask.diagnostics import ProgressBar

import config


def loader(file):
    try:
        logger.info('start loading the data into dask dataframe')
        data_df = dd.read_csv(file,
                              dtype={'RatecodeID': 'float64', 'VendorID': 'float64', 'passenger_count': 'float64',
                                     'payment_type': 'float64'}, converters={'store_and_fwd_flag': convert_dtype},
                              parse_dates=config.DATE_FIELDS)
        logger.info('Finished loading the data into dask dataframe')
        return data_df
    except FileNotFoundError:
        logger.error('File not found: ' + file)
        print('File not found!!', file)


def preprocess(df):
    logger.info('start preprocess the dataframe')
    df = df[(df['VendorID'].isin([1, 2])) & (df['tpep_pickup_datetime'] != 0) & (df['tpep_dropoff_datetime'] != 0)
            & (df['passenger_count'] > 0) & (df['trip_distance'] > 0) & (df['RatecodeID'].isin([1, 2, 3, 4, 5, 6]))
            & (df['store_and_fwd_flag'].isin(['Y', 'N'])) & (df['PULocationID'] > 0) & (df['DOLocationID'] > 0)
            & (df['payment_type'].isin([1, 2, 3, 4, 5, 6])) & (df['total_amount'] > 0)
            & df['tpep_pickup_datetime'].dt.year.isin(config.DATASET_YEAR)]
    df = df.dropna().astype(config.SCHEMA)
    logger.info('Finished preprocessing the dataframe')
    return df


def convert_dtype(x):
    if not x:
        return ''
    try:
        return str(x)
    except ValueError:
        return ''


def avg_trip_length(df, month, year):
    return df[(df.tpep_pickup_datetime.dt.month == month) &
              (df.tpep_pickup_datetime.dt.year == year)]['trip_distance'].mean().compute()


def rolling_mean(df):
    rolling_df = df.compute()
    rolling_df = rolling_df.groupby(by=[rolling_df.tpep_pickup_datetime.dt.year,
                    rolling_df.tpep_pickup_datetime.dt.month])['trip_distance'].rolling(45).mean().to_frame(name='Rolling_mean')
    return rolling_df.droplevel(0).reset_index()['Rolling_mean']


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(os.path.join(os.path.dirname(__file__), config.DEFAULT_LOGGING_PATH))
    formatter = logging.Formatter(config.DEFAULT_LOGGING_FORMAT)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    progressbar = ProgressBar()
    logger.info('An info message')
    progressbar.register()
    filename = os.path.join(os.path.dirname(__file__), config.DATASET_PATH)
    ddf = loader(filename)
    ddf = preprocess(ddf)
    print(f'The average trip length for January is ' + str(avg_trip_length(ddf, 1, 2021)))
    print('The 45 rolling mean is ')
    print(rolling_mean(ddf))
    logger.info('An end message')
    progressbar.unregister()
