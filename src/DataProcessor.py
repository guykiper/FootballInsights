from sklearn.preprocessing import LabelEncoder
import os
import random
import shutil
import numpy as np
from tqdm import tqdm
import requests
import pandas as pd
import cv2
from io import BytesIO
from sklearn.model_selection import train_test_split
from PIL import Image
import pycountry
from geopy.geocoders import Nominatim
import hashlib

IMAGE_SIZE = (224, 224)

dict_country_code_iso_alpha_3 = {
    'HON': 'HND',
    'URU': 'URY',
    'GRE': 'GRC',
    'ZAM': 'ZMB',
    'CHA': 'TCD',
    'ALG': 'DZA',
    'SIN': 'SGP',
    'VIE': 'VNM',
    'VIN': 'VCT',
    'MAD': 'MDG',  # Madagascar
    'SUD': 'SDN',
    'NIR': 'GBR',
    'SCO': 'GBR',
    'GER': 'DEU',
    'ARU': 'ABW',
    'POR': 'PRT',
    'GUI': 'GIN',
    'FIJ': 'FJI',
    'SMA': 'SXM',  # Sint Maarten
    'SRI': 'LKA',
    'DEN': 'DNK',
    'TRI': 'TTO',
    'ENG': 'GBR',
    'BUL': 'BGR',
    'SKN': 'KNA',
    'RSA': 'ZAF',
    'HAI': 'HTI',
    'GUA': 'GTM',
    'VAN': 'VUT',
    'TAN': 'TZA',
    'ZIM': 'ZWE',
    'SUI': 'CHE',
    'LIB': 'LBN',
    'GRN': 'GRD',
    'PAR': 'PRY',
    'NED': 'NLD',
    'PUR': 'PRI',
    'SMN': 'SMR',
    'BER': 'BMU',
    'EQG': 'GNQ',
    'PHI': 'PHL',
    'SEY': 'SYC',
    'BOT': 'BWA',
    'CHI': 'CHL',
    'ANG': 'AGO',
    'CRC': 'CRI',
    'TOG': 'TGO',
    'MTN': 'MRT',
    'WAL': 'GBR',
    'CRO': 'HRV',
    'PLE': 'PSE',
    'KSA': 'SAU',
    'MRI': 'MUS',
    'NIG': 'NGA',
    'BHU': 'BTN',
    'CTA': 'CAF',  # Central African Republic
    'NEP': 'NPL',
    'GAM': 'GMB',
    'LES': 'LSO',
    'MAS': 'MYS',
    'CGO': 'COG',
    'TPE': 'TWN'
}


class DataProcessor_Kmeans:
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

    @staticmethod
    def fill_playing_time_mp(row):
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


class ImageClassifierEnsemble_DataPreprocessing:
    IMAGE_SIZE = (224, 224)
    n_estimators = 10

    def __init__(self, data_directory, output_directory, test_size=0.2, min_images_threshold=16):
        self.data_directory = data_directory
        self.output_directory = output_directory
        self.test_size = test_size
        self.min_images_threshold = min_images_threshold
        self.class_names = []  # Initialize the class names attribute

    @staticmethod
    def create_directory():
        # Create a directory to store the images
        output_directory = "Data_files/downloaded_images"
        os.makedirs(output_directory, exist_ok=True)
        df = pd.read_csv('../Data_files/backup_new.csv')
        for index, row in df.iterrows():
            image_url = row['link_image_club']
            club_name = row['club_name']
            gender = row['gender']
            club_code = row['club_code']
            country = row['country']  # Assuming you have a 'country' column in the CSV

            # Create a subdirectory for each country if it doesn't already exist
            country_directory = os.path.join(output_directory, country)
            os.makedirs(country_directory, exist_ok=True)

            try:
                response = requests.get(image_url)
                if response.status_code == 200:
                    image_data = BytesIO(response.content)
                    img = Image.open(image_data)

                    # Create the filename using club_name, gender, and club_code
                    image_filename = f"{club_name}_{gender}_{club_code}.png"
                    image_filepath = os.path.join(country_directory, image_filename)

                    img.save(image_filepath)
                    print(f"Image {index} saved as {image_filepath}.")
                else:
                    print(f"Failed to download image {index} (Status code: {response.status_code}).")
            except Exception as e:
                print(f"Error downloading image {index}: {str(e)}")

    def prepare_data(self):
        os.makedirs(self.output_directory, exist_ok=True)
        data_images = []

        for country_folder in os.listdir(self.data_directory):
            country_path = os.path.join(self.data_directory, country_folder)

            if os.path.isdir(country_path):
                country_label = country_folder
                num_images = len([f for f in os.listdir(country_path) if
                                  f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'))])

                if num_images >= self.min_images_threshold:
                    self.class_names.append(country_label)  # Add class names
                    for image_file in os.listdir(country_path):
                        image_path = os.path.join(country_path, image_file)
                        data_images.append((image_path, country_label))

        random.shuffle(data_images)
        train_data, test_data = train_test_split(data_images, test_size=self.test_size, random_state=42)
        train_directory = os.path.join(self.output_directory, "train")
        test_directory = os.path.join(self.output_directory, "test")

        os.makedirs(train_directory, exist_ok=True)
        os.makedirs(test_directory, exist_ok=True)

        for image_path, label in train_data:
            destination = os.path.join(train_directory, label)
            os.makedirs(destination, exist_ok=True)
            shutil.copy(image_path, destination)

        for image_path, label in test_data:
            destination = os.path.join(test_directory, label)
            os.makedirs(destination, exist_ok=True)
            shutil.copy(image_path, destination)

        print("Data preparation and splitting complete")

    def load_and_shuffle_data(self):
        datasets = ['Data_files/prepared_data/train', 'Data_files/prepared_data/test']
        output = []

        for dataset in datasets:
            images = []
            labels = []
            print("Loading {}".format(dataset))

            for folder in os.listdir(dataset):
                label = self.class_names.index(folder)  # Use class names to get label
                for file in tqdm(os.listdir(os.path.join(dataset, folder))):
                    img_path = os.path.join(os.path.join(dataset, folder), file)
                    image = cv2.imread(img_path)
                    try:
                        image = cv2.resize(image, IMAGE_SIZE)
                        images.append(image)
                        labels.append(label)

                    except:
                        # print('error')
                        # print(image)
                        # print(file)
                        # print(folder)
                        print(image, file, folder)

            images = np.array(images, dtype='float32')
            labels = np.array(labels, dtype='int32')
            output.append((images, labels))

        return output

    @staticmethod
    def display_random_image(class_names, images, labels):
        index = np.random.randint(images.shape[0])
        image = images[index]
        print('Image #{} : '.format(index) + class_names[labels[index]])
        cv2.imshow('image', image)
        cv2.waitKey(0)


class Dataprocessor_transform:

    def __init__(self, df, gender='male'):

        """
        Initializes the Dataprocessor_transform class.

        Parameters:
        - link_df (str): File path or URL to the CSV file containing the input data.
        - gender (str): Gender information (default is 'male').

        Attributes:
        - df (pd.DataFrame): DataFrame containing the input data.
        - gender (str): Gender information associated with the instance.
        """

        self.df = df
        self.file_teams = pd.read_csv('../Data_files/csv files/backup_new.csv')
        self.team_code_table = pd.DataFrame.empty
        self.gender = gender
        self.df_location = pd.DataFrame.empty
        self.pos_transform()
        self.add_columns()
        self.club_name_transform()
        self.add_lat_long()
        self.opponent_total = self.df[self.df['Player'] == 'Opponent Total']  # c1f3aa251af6ebec
        self.squad_total = self.df[self.df['Player'] == 'Squad Total']  # 198adbcff3305cf0
        self.add_unique_id()
        self.drop_columns()

    # def change_nation_code(self):

    def add_country_names(self, save_link, column_name='Nation'):
        """
        Adds a full country name column to the DataFrame based on country code shortcuts.

        Parameters:
        - save_link (str): File path or URL to save the modified DataFrame as a new CSV file.
        - column_name (str): Name of the column containing country code shortcuts (default is 'Nation').

        Returns:
        - list: A list of unique tuples, each containing a country's full name and its corresponding country code.

        This method processes the DataFrame by mapping country code shortcuts to their full names using the pycountry library.
        It then adds a new column 'country_full_name' to the DataFrame and saves the result to a new CSV file specified by save_link.
        The method returns a list of unique pairs of country names and their corresponding country code shortcuts.

        Note:
        - If a country code is not found, 'null' is used as a placeholder for both the country code and full name.
        - The code 'KVX' is treated as a special case, mapping to 'null' with the country name 'Kosovo'.
        """

        df_code_country = pd.DataFrame()
        countries = {}
        for country in pycountry.countries:
            countries[country.alpha_3] = country.name

        names = []
        code_alpha_3_iso = []
        for code in self.df[column_name]:
            if code in countries:
                code_alpha_3_iso.append(code)
                # Extracting the portion to the left of the first comma (if any)
                if ',' in countries[code]:
                    names.append(countries[code].split(',')[0])
                else:
                    names.append(countries[code])
            elif code is np.nan:
                code_alpha_3_iso.append('null')
                names.append('null')
            elif code == 'KVX':
                code_alpha_3_iso.append('null')
                names.append('Kosovo')
            else:
                code_alpha_3_iso.append(dict_country_code_iso_alpha_3[code])
                names.append(countries[dict_country_code_iso_alpha_3[code]])
        original_new = list(set((zip(list(self.df[column_name]), code_alpha_3_iso))))
        df_code_country['original_code'] = [x[0] for x in original_new]
        df_code_country['new_code'] = [x[1] for x in original_new]

        df_code_country.to_csv(save_link)
        self.df['country_full_name'] = names
        self.df['Nation'] = code_alpha_3_iso
        return list(set(list(zip(names, code_alpha_3_iso))))

    def add_lat_long(self, column_name='Nation'):

        """
        Adds latitude and longitude information to the DataFrame based on country names.

        Parameters:
        - save_link (str): File path or URL to save the DataFrame with latitude and longitude information as a new CSV file.
        - column_name (str): Name of the column containing country names (default is 'Nation').

        Returns:
        None

        This method utilizes the `add_country_names` method to obtain country names and codes,
        and then uses the geopy library to retrieve latitude and longitude information for each country.
        The resulting DataFrame, including country names, country codes, latitude, and longitude,
        is saved to a new CSV file specified by save_link.

        Note:
        - If geolocation information is not available for a country, NaN values are assigned to the latitude and longitude.
        """

        # Call add_country_names to get country names and codes
        country_code = self.add_country_names('../Data_files/csv files/code_country_' + self.gender + '.csv',
                                              column_name)

        # Initialize geolocator
        geolocator = Nominatim(user_agent="my_app", timeout=5)

        # Initialize DataFrame for location information
        self.df_location = pd.DataFrame(columns=['country_name', 'country_code', 'latitude', 'longitude'])
        lat = []
        lon = []
        countries = [x[0] for x in country_code]
        countries_code = [x[1] for x in country_code]

        # Retrieve latitude and longitude for each country
        for country in countries:
            location = geolocator.geocode(country)
            if location:
                lat.append(location.latitude)
                lon.append(location.longitude)
            else:
                lat.append(np.nan)
                lon.append(np.nan)
        # Populate DataFrame with location information
        self.df_location['country_name'] = countries
        self.df_location['country_code'] = countries_code
        self.df_location['latitude'] = lat
        self.df_location['longitude'] = lon

        # Save the DataFrame to a new CSV file
        # self.df_location.to_csv(save_link)

    def pos_transform(self):
        cleaned = []
        main_pos = []
        secondary_pos = []

        for p in self.df['Pos']:
            split = str(p).split(",")
            main_pos.append(split[0])
            if len(split) > 1:
                secondary_pos.append(split[1])
            else:
                secondary_pos.append(None)

            key = tuple(sorted(split))
            cleaned.append("-".join(key))
        self.df['Main_Pos'] = main_pos
        self.df['Secondary_Pos'] = secondary_pos
        self.df['Pos'] = cleaned

    def club_name_transform(self):
        if self.gender == 'male':
            club_names = []
            res_list = []

            for last, name in list(zip(self.df['team'].str.split('-').str[-1], self.df['team'])):
                if last == 'male':
                    list_name = name.split('-')
                    list_name[-1] = 'male'
                    female_name = '-'.join(list_name)
                    club_names.append(female_name)
                else:
                    club_names.append(name + '-male')
        else:
            club_names = []
            res_list = []

            for last, name in list(zip(self.df['team'].str.split('-').str[-1], self.df['team'])):
                if last == 'Women':
                    list_name = name.split('-')
                    list_name[-1] = 'female'
                    female_name = '-'.join(list_name)
                    club_names.append(female_name)
                else:
                    club_names.append(name + '-female')

        self.df['team_new_name'] = club_names
            # Add female for those without

    def drop_columns(self):
        self.df.drop(self.df.columns[self.df.columns.str.startswith('Unnamed')], axis=1, inplace=True)
        self.df.drop(self.df[self.df['Player'] == 'Opponent Total'].index, inplace=True)
        self.df.drop(self.df[self.df['Player'] == 'Squad Total'].index, inplace=True)

    def add_columns(self):
        self.df['Gender'] = self.gender
        year = list(pd.read_csv('../Data_files/csv files/backup_new.csv')['year'])[0]
        self.df['season'] = year

    def add_unique_id(self):

        def generate_id(row):
            relevant_data = row['Player'] + row['Nation'] + row['Gender'] + row['Pos']
            hashed = hashlib.sha256(relevant_data.encode())
            return hashed.hexdigest()[:16]

        id_col = "ID"
        self.df[id_col] = self.df.apply(generate_id, axis=1)

    def save_df_csv(self, link_save):
        self.df.to_csv(link_save)


if __name__ == "__main__":
    gender_name = 'female'
    df_male = pd.read_csv("../Data_files/csv files/" + gender_name + ".csv")
    df_male.drop(df_male.columns[df_male.columns.str.startswith('country_full_name')], axis=1, inplace=True)
    data = Dataprocessor_transform(df_male, gender_name)
    # data.pos_transform()

    # data.add_lat_long('../Data_files/csv files/locaiton_info_'+gender+'.csv', "Nation")
    # data.Pos_column_transform()
    # data.save_df_csv("Data_files/csv files/"+gender+".csv")

    # gender = 'female'
    # data = Dataprocessor_transform("../Data_files/csv files/" + gender + ".csv", gender)
    # data.add_lat_long('../Data_files/csv files/locaiton_info_' + gender + '.csv', "Nation")
    # data.Pos_column_transform()
    # data.save_df_csv("../Data_files/csv files/" + gender + ".csv")

    # data.marge_male_female()
