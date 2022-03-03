import logging
import os
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import calendar
import yaml

with open(os.path.join(os.path.dirname(__file__), '../config.yaml')) as c:
    constants = yaml.load(c, Loader=yaml.FullLoader)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(os.path.join(os.path.dirname(__file__), constants['DEFAULT_LOGGING_PATH']))
formatter = logging.Formatter(constants['DEFAULT_LOGGING_FORMAT'])
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def loader(file_or_url):
    """
    Accepts csv file or s3 url of NYC TLC dataset and returns dask dataframe
    :param file_or_url: path of the csv file or the s3 URL
    :return: a dask dataframe
    """
    try:
        logger.info('start loading the data into dask dataframe')
        data_df = dd.read_csv(file_or_url,
                              dtype={'RatecodeID': 'float64', 'VendorID': 'float64', 'passenger_count': 'float64',
                                     'payment_type': 'float64'}, converters={'store_and_fwd_flag': convert_dtype},
                              parse_dates=constants['DATE_FIELDS'])
        logger.info('Finished loading the data into dask dataframe')
        return data_df
    except FileNotFoundError:
        logger.exception('File not found: ' + file_or_url)
        raise
    except ValueError:
        logger.exception('Invalid url: ' + file_or_url)
        raise
    except Exception as e:
        logger.exception('Dataset import failed: ' + str(e))


def preprocess(df):
    """
    Accepts the loaded dask dataframe and returns the preprocessed dask dataframe
    """
    try:
        logger.info('start preprocess the dataframe')
        df = df[(df['VendorID'].isin([1, 2])) & (df['tpep_pickup_datetime'] != 0) & (df['tpep_dropoff_datetime'] != 0)
                & (df['passenger_count'] > 0) & (df['trip_distance'] > 0) & (df['RatecodeID'].isin([1, 2, 3, 4, 5, 6]))
                & (df['store_and_fwd_flag'].isin(['Y', 'N'])) & (df['PULocationID'] > 0) & (df['DOLocationID'] > 0)
                & (df['payment_type'].isin([1, 2, 3, 4, 5, 6])) & (df['total_amount'] > 0)
                & df['tpep_pickup_datetime'].dt.year.isin(constants['DATASET_YEAR'])]
        df = df.dropna().astype(constants['SCHEMA'])
        logger.info('Finished preprocessing the dataframe')
        return df
    except Exception as e:
        logger.exception('Preprocessing failed: ' + str(e))


def convert_dtype(x):
    """
    Converter function for avoiding mixed dtype in columns warning while loading the input csv
    with dask dataframe read_csv.
    """
    if not x:
        return ''
    try:
        return str(x)
    except Exception:
        return ''


def avg_trip_length(df, trip_month, trip_year):
    try:
        logging.info('calculate average trip length')
        return df[(df.tpep_pickup_datetime.dt.month == trip_month) &
                  (df.tpep_pickup_datetime.dt.year == trip_year)]['trip_distance'].mean().compute()
    except Exception as e:
        logger.exception('Calculation of average trip length failed: ' + str(e))


def rolling_mean(df):
    try:
        logging.info('calculation of rolling mean started')
        rolling_df = df.compute()
        rolling_df = rolling_df.groupby(by=[rolling_df.tpep_pickup_datetime.dt.year,
                                            rolling_df.tpep_pickup_datetime.dt.month])['trip_distance'].rolling(
            45).mean().to_frame(name='Rolling_mean')
        logging.info('Rolling mean calculation finished')
        return rolling_df.droplevel(0).reset_index()['Rolling_mean']

    except Exception as e:
        logger.exception('Calculation of rolling mean failed: ' + str(e))


if __name__ == "__main__":
    logger.info('Initialize the data pipeline')
    progressbar = ProgressBar()
    progressbar.register()
    months = list(calendar.month_name)[1:]
    for year in constants['DATASET_YEAR']:
        for index, month in enumerate(constants['DATASET_MONTH']):
            s3_url = constants['DATASET_URL']
            url = f'{s3_url}_{year}-{month}.csv'
            dask_df = loader(url)
            dask_df = preprocess(dask_df)
            print(f'The avg trip length for {months[index]} {year} is ' + str(avg_trip_length(dask_df, index+1, year)))
            print('The 45 rolling mean is ' + str(rolling_mean(dask_df)))
            # comment/remove the break statement to calculate average trip length for all the months
            break
    progressbar.unregister()
    logging.info('Data pipeline finished processing')




