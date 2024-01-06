import unittest
import web_scraping
from DataProcessor import DataProcessor_Kmeans
import DataFrameAnalyzer
import MachineLearning_class
import requests
from unittest.mock import patch
import pandas as pd
import numpy as np


class TestClassRM(unittest.TestCase):

    def setUp(self):
        self.test_obj_real_madrid_2000 = web_scraping.WebScraping_fbref('Real-Madrid', '53a2f082', 2000)
        self.test_obj_real_madrid_2022 = web_scraping.WebScraping_fbref('Real-Madrid', '53a2f082', 2022, conv_json=False)
        self.test_obj_real_madrid_woman_2022 = web_scraping.WebScraping_fbref('Real-Madrid-Women', '54582b93', 2022)
        pass

    def tearDown(self):
        pass

    def test_object(self):
        self.assertEqual(self.test_obj_real_madrid_2000.link
                         ,
                         'https://fbref.com/en/squads/53a2f082/2000-2001/all_comps/Real-Madrid-Stats-All-Competitions')
        self.assertEqual(self.test_obj_real_madrid_2022.link
                         ,
                         'https://fbref.com/en/squads/53a2f082/2022-2023/all_comps/Real-Madrid-Stats-All-Competitions')
        self.assertEqual(self.test_obj_real_madrid_woman_2022.link
                         ,
                         'https://fbref.com/en/squads/54582b93/2022-2023/all_comps/Real-Madrid-Women-Stats-All'
                         '-Competitions')

    def test_gender(self):
        self.test_obj_real_madrid_2022.gender_dataframe(self.test_obj_real_madrid_2022.link)
        self.test_obj_real_madrid_woman_2022.gender_dataframe(self.test_obj_real_madrid_woman_2022.link)
        self.assertEqual(self.test_obj_real_madrid_2022.gender, 'Male')
        self.assertEqual(self.test_obj_real_madrid_woman_2022.gender, 'Female')

    def test_competition_df(self):
        obj = self.test_obj_real_madrid_2022
        self.assertIsInstance(obj.competition_df(obj.link), pd.core.frame.DataFrame)

    def test_all_competition_df(self):
        obj = self.test_obj_real_madrid_2022
        len_table = len(obj.all_competition_df(obj.link))
        self.assertGreater(len_table, 0)


class TestDataProcessor(unittest.TestCase):

    def setUp(self) -> None:
        self.test_obj_data_male_wide = DataProcessor_Kmeans('Data_files/male.csv','wide')
        self.test_obj_data_male_narrow = DataProcessor_Kmeans('Data_files/male.csv', 'narrow')
        self.test_obj_data_female_wide_1 = DataProcessor_Kmeans('Data_files/female.csv', 'wide')
        self.test_obj_data_female_wide_2 = DataProcessor_Kmeans('Data_files/female.csv', 'wide')
        self.test_obj_data_female_narrow = DataProcessor_Kmeans('Data_files/female.csv', 'narrow')

    def test_columns_notin_data(self):
        self.test_obj_data_female_wide_1.drop_unnecessary_columns()
        test_obj_before = self.test_obj_data_female_wide_1.data
        self.test_obj_data_female_wide_2.preprocess_and_encode()
        for value in ['Unnamed: 0', 'Player', 'Nation', 'Matches']:
            self.assertNotIn(value, test_obj_before.columns)

    def test_number_columns_change(self):
        self.test_obj_data_female_wide_1.drop_unnecessary_columns()
        test_obj_before = self.test_obj_data_female_wide_1.data
        self.test_obj_data_female_wide_2.preprocess_and_encode()
        test_obj_after = self.test_obj_data_female_wide_2.data
        self.assertLess(test_obj_after.shape[1], test_obj_before.shape[1])

    def test_all_numerical_columns(self):
        self.test_obj_data_female_wide_1.preprocess_and_encode()
        test_obj = self.test_obj_data_female_wide_1.data
        allowed_types = ('int32', 'int64', 'float64')
        # Check data types for each column
        for col in test_obj.columns:
            col_type = test_obj[col].dtype
            self.assertIn(col_type, allowed_types, f"Column {col} has unexpected data type: {col_type}")


if __name__ == '__main__':
    unittest.main()
