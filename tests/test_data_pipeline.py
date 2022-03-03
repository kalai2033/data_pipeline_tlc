import unittest
import logging
from src import data_pipeline as dp
from settings import constants
import pathlib as pl
import os
import dask.dataframe as dd

logger = logging.getLogger(__name__)
logger.level = logging.INFO


class TestDataPipeline(unittest.TestCase):

    def setUp(self):
        logger.info('setUp')
        self.valid_input = dp.loader(os.path.join(os.path.dirname(__file__), constants.TEST_DATA_FILENAME))
        self.invalid_path = 'non_existent_file.csv'
        self.preprocessed_df = dp.preprocess(self.valid_input)
        self.sample_df = self.preprocessed_df.sample(frac=0.000001).compute()
        self.avg_trip = dp.avg_trip_length(self.preprocessed_df, 1, 2021)
        self.rolling_mean = dp.rolling_mean(self.preprocessed_df)

    def tearDown(self):
        logger.info('tearDown')

    def test_loader(self):
        path = pl.Path(os.path.join(os.path.dirname(__file__), constants.TEST_DATA_FILENAME))
        self.assertTrue(path.is_file())
        with self.assertRaises(FileNotFoundError):
            dp.loader(self.invalid_path)
        self.assertIsInstance(self.valid_input, dd.DataFrame)
        self.assertIsNotNone(self.valid_input)

    def test_preprocess(self):
        self.assertIsNotNone(self.preprocessed_df)
        self.assertIsInstance(self.preprocessed_df, dd.DataFrame)
        self.assertEqual(len(self.preprocessed_df.columns), 18)
        self.assertEqual(len(constants.DATASET_YEAR), len(set(self.sample_df.tpep_dropoff_datetime.dt.year)))

    def test_avg_trip_length(self):
        self.assertIsNotNone(self.avg_trip)
        self.assertIsInstance(self.avg_trip, float)
        self.assertEqual((round(self.avg_trip, 3)), 2.742)

    def test_rolling_mean(self):
        self.assertIsNotNone(self.rolling_mean)


if __name__ == '__main__':
    unittest.main()
