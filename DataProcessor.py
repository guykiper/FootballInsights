import pandas as pd
from sklearn.preprocessing import LabelEncoder
import os
import random
import shutil
import numpy as np
from tqdm import tqdm
import requests
import pandas as pd
import cv2
import tensorflow as tf
from io import BytesIO
from sklearn.model_selection import train_test_split
from PIL import Image
IMAGE_SIZE = (224, 224)





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



class ImageClassifierEnsemble_DataPreprocessing:
    IMAGE_SIZE = (224, 224)
    n_estimators = 10

    def __init__(self, data_directory, output_directory, test_size=0.2, min_images_threshold=16):
        self.data_directory = data_directory
        self.output_directory = output_directory
        self.test_size = test_size
        self.min_images_threshold = min_images_threshold
        self.class_names = []  # Initialize the class names attribute

    def create_directory(self):
        # Create a directory to store the images
        output_directory = "Data_files/downloaded_images"
        os.makedirs(output_directory, exist_ok=True)
        df = pd.read_csv('Data_files/backup_new.csv')
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
        data = []

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
                        data.append((image_path, country_label))

        random.shuffle(data)
        train_data, test_data = train_test_split(data, test_size=self.test_size, random_state=42)
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

    def display_random_image(self, class_names, images, labels):
        index = np.random.randint(images.shape[0])
        image = images[index]
        print('Image #{} : '.format(index) + class_names[labels[index]])
        cv2.imshow('image', image)
        cv2.waitKey(0)
