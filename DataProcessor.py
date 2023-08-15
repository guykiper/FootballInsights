import pandas as pd
from sklearn.preprocessing import LabelEncoder


class DataPreprocessor:
    def __init__(self, file_path, data_type='wide'):
        self.data = pd.read_csv(file_path)
        self.data_type = data_type
        self.pos_encoder = LabelEncoder()
        self.team_encoder = LabelEncoder()

    def drop_unnecessary_columns(self):
        columns_to_drop = ['Unnamed: 0', 'Player', 'Nation',
                           'Matches']
        self.data.drop(columns=columns_to_drop, inplace=True)

    def filter_by_playing_time(self, threshold=10):
        self.data = self.data[self.data['Playing Time - Starts'] >= threshold]

    def fill_playing_time_mp(self, row):
        if pd.isnull(row['Playing Time - MP']):
            return row['MP']
        return row['Playing Time - MP']

    def preprocess_data(self):
        self.drop_unnecessary_columns()
        self.filter_by_playing_time()
        self.data['Playing Time - MP'] = self.data.apply(self.fill_playing_time_mp, axis=1)
        self.data.drop(columns=['MP'], inplace=True)

    def encode_categorical_columns(self):
        self.data['Pos'] = self.pos_encoder.fit_transform(self.data['Pos'])
        self.data['team'] = self.team_encoder.fit_transform(self.data['team'])

    def preprocess_and_encode(self):
        self.preprocess_data()
        self.encode_categorical_columns()

        # Additional code to handle missing values and separate the data
        precetnt_41_missing_columns = ['Per 90 Minutes - xG', 'Per 90 Minutes - xAG', 'Per 90 Minutes - xG+xAG',
                                       'Per 90 Minutes - npxG', 'Per 90 Minutes - npxG+xAG']
        mask = self.data[precetnt_41_missing_columns].isnull().any(axis=1)

        if self.data_type == 'narrow':
            self.data = self.data[mask]
        elif self.data_type == 'wide':
            self.data = self.data[~mask]
            self.data = self.data.dropna(subset=['Age', 'Pos'])
