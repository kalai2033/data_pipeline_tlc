import unittest
import logging
from src import data_pipeline as dp
import pathlib as pl
import os
import dask.dataframe as dd
import yaml

with open(os.path.join(os.path.dirname(__file__), '../config.yaml')) as c:
    config = yaml.load(c, Loader=yaml.FullLoader)
logger = logging.getLogger(__name__)
logger.level = logging.INFO


class TestDataPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """
        Initialize the inputs for the test cases with the setup class
        """
        logger.info('setUp')
        cls.valid_path = os.path.join(os.path.dirname(__file__), config['TEST_DATASET_PATH'])
        cls.valid_input = dp.loader(cls.valid_path)
        cls.invalid_path = 'non_existent_file.csv'
        cls.preprocessed_df = dp.preprocess(cls.valid_input)
        cls.avg_trip = dp.avg_trip_length(cls.preprocessed_df, 1, 2021)
        cls.rolling_mean = dp.rolling_mean(cls.preprocessed_df, 1, 2021)

    @classmethod
    def tearDownClass(cls):
        logger.info('tearDown')

    def test_loader(self):
        logger.info('test for loader started')
        path = pl.Path(self.valid_path)
        self.assertTrue(path.is_file())
        with self.assertRaises(FileNotFoundError):
            dp.loader(self.invalid_path)
        self.assertIsInstance(self.valid_input, dd.DataFrame)
        self.assertIsNotNone(self.valid_input)
        logger.info('test for loader finished')

    def test_preprocess(self):
        logger.info('test for preprocess started')
        self.assertIsNotNone(self.preprocessed_df)
        self.assertIsInstance(self.preprocessed_df, dd.DataFrame)
        self.assertEqual(len(self.preprocessed_df.columns), 18)
        logger.info('test for preprocess finished')

    def test_avg_trip_length(self):
        logger.info('test for avg_trip_length started')
        self.assertIsNotNone(self.avg_trip)
        self.assertIsInstance(self.avg_trip, float)
        self.assertEqual((round(self.avg_trip, 3)), 3.342)
        logger.info('test for avg_trip_length finished')

    def test_rolling_mean(self):
        logger.info('test for rolling mean started')
        self.assertIsNotNone(self.rolling_mean)
        logger.info('test for rolling mean finished')


if __name__ == '__main__':
    unittest.main()
