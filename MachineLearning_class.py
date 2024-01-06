import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
import DataFrameAnalyzer
from DataProcessor import DataProcessor_Kmeans
import warnings
import os
import random
import shutil
import numpy as np
from tqdm import tqdm
import requests
import pandas as pd
import cv2
import tensorflow as tf
from sklearn.metrics import accuracy_score
from keras.applications.vgg16 import VGG16
from DataProcessor import ImageClassifierEnsemble_DataPreprocessing
n_estimators = 10
IMAGE_SIZE = (224, 224)
warnings.filterwarnings("ignore")
import subprocess
subprocess.check_output(["dir"], shell=True)



class KMeansCluster:
    def __init__(self, data):
        self.data = data
        self.scaler = StandardScaler()
        self.scaled_data = self.scaler.fit_transform(self.data)

    def perform_clustering(self, num_clusters=5):
        kmeans = KMeans(n_clusters=num_clusters, random_state=42)
        self.data['Cluster'] = kmeans.fit_predict(self.scaled_data)

    def get_clustered_data(self):
        return self.data


    # def pca_precentation(self):
    #     kmeans = KMeans(n_clusters=2, random_state=0)
    #     kmeans.fit(self.data)
    #     centers = kmeans.cluster_centers_
    #     pca = PCA(n_components=3)
    #     projected = pca.fit_transform(self.data)
    #     x = projected[:, 0]
    #     y = projected[:, 1]
    #     plt.figure(figsize=(7, 5))
    #     plt.scatter(x, y)
    #     plt.title('Visualize the clustering in 2D')
    #     plt.show()
    #     centers_pca = pca.transform(centers)
    #     centers_X = centers_pca[:, 0]
    #     centers_Y = centers_pca[:, 1]
    #
    #     cluster_label = kmeans.fit_predict(self.data)
    #     colors = cm.nipy_spectral(cluster_label.astype(float) / 2)
    #     plt.figure(figsize=(7, 5))
    #     plt.scatter(x, y, color=colors)
    #     plt.scatter(centers_X, centers_Y,
    #                 color='pink', s=150)
    #     plt.title('K means clusters and its centroids')
    #     plt.show()

class ImageClassifierEnsemble_Model:

    n_estimators = 10

    def __init__(self, train_images, train_labels, test_images, test_labels):
        self.train_images = train_images
        self.train_labels = train_labels
        self.test_images = test_images
        self.test_labels = test_labels

    def ensemble_models(self):
        model = VGG16(weights='imagenet', include_top=False)
        train_features = model.predict(self.train_images)
        test_features = model.predict(self.test_images)
        n_train, x, y, z = train_features.shape
        n_test, x, y, z = test_features.shape
        numFeatures = x * y * z

        np.random.seed(seed=1997)
        max_samples = 0.8
        max_samples *= n_train
        max_samples = int(max_samples)
        models = list()
        random = np.random.randint(50, 100, size=n_estimators)

        for i in range(n_estimators):
            model = tf.keras.Sequential([tf.keras.layers.Flatten(input_shape=(x, y, z)),
                                         tf.keras.layers.Dense(random[i], activation=tf.nn.relu),
                                         tf.keras.layers.Dense(26, activation=tf.nn.softmax)])
            model.compile(optimizer='adam', loss='sparse_categorical_crossentropy', metrics=['accuracy'])
            models.append(model)

        histories = []

        for i in range(n_estimators):
            train_idx = np.random.choice(len(train_features), size=max_samples)
            histories.append(
                models[i].fit(train_features[train_idx], self.train_labels[train_idx], batch_size=128, epochs=10,
                              validation_split=0.1))

        predictions = []
        for i in range(n_estimators):
            predictions.append(models[i].predict(test_features))

        predictions = np.array(predictions)
        predictions = predictions.sum(axis=0)
        pred_labels = predictions.argmax(axis=1)
        accuracy = accuracy_score(self.test_labels, pred_labels)

        for i, model in enumerate(models):
            model.save(f"model_{i}.h5")

        return accuracy

    def load_ensemble_models(self, directory="Data_files/Ensemble_image_models"):
        if not os.path.exists(directory):
            raise ValueError(f"The directory {directory} does not exist.")

        models = []
        for i in range(n_estimators):
            model_path = os.path.join(directory, f"model_{i}.h5")
            if not os.path.exists(model_path):
                raise ValueError(f"Model file {model_path} does not exist.")

            model = tf.keras.models.load_model(model_path)
            models.append(model)
        return models

    def predict_ensemble(self, images):
        loaded_models = self.load_ensemble_models()
        predictions = []

        for model in loaded_models:
            model_predictions = model.predict(images)
            predictions.append(model_predictions)

        ensemble_predictions = np.mean(predictions, axis=0)
        final_labels = np.argmax(ensemble_predictions, axis=1)

        return final_labels


if __name__ == "__main__":

    choose = input('what do you wnat to run? k - kmeans, i image classifier:   ')
    ##Kmeans

    if choose == 'k':
        # Create an instance of DataProcessor
        # Create an instance of the DataPreprocessor class
        preprocessor_wide = DataProcessor_Kmeans('Data_files/female.csv')
        # Preprocess and encode the data (use 'wide' or 'narrow' as desired)
        preprocessor_wide.preprocess_and_encode()
        encoded_data_wide = preprocessor_wide.data

        preprocessor_narrow = DataProcessor_Kmeans('Data_files/female.csv', 'narrow')
        preprocessor_narrow.preprocess_and_encode()
        encoded_data_narrow = preprocessor_narrow.data

        kmeans_cluster = KMeansCluster(encoded_data_wide)
        DataFrameAnalyzer.DataFrameAnalyzer(encoded_data_wide).elbow_method()
        # Perform clustering with a specified number of clusters
        num_clusters = 4
        kmeans_cluster.perform_clustering(num_clusters)
        # Get the clustered data
        clustered_data = kmeans_cluster.get_clustered_data()

        # Now 'clustered_data' contains the original data with an additional 'Cluster' column indicating the cluster assignments

    else:
        #Image Classifier

        data_directory = "Data_files/downloaded_images"
        output_directory = "Data_files/prepared_data"

        data_prep = ImageClassifierEnsemble_DataPreprocessing(data_directory, output_directory)
        data_prep.prepare_data()

        class_names = data_prep.class_names  # Access the class names attribute
        train_data, test_data = data_prep.load_and_shuffle_data()

        # Display a random image
        data_prep.display_random_image(class_names, train_data[0], train_data[1])
        data_model = ImageClassifierEnsemble_Model(train_data, train_data, test_data, test_data)
        # Ensemble models and calculate accuracy
        accuracy = data_model.ensemble_models()
        print("Accuracy: {}".format(accuracy))

        # loaded_models = data_prep.load_ensemble_models()
        base_path = "Data_files/prepared_data/train/Belgium Football Clubs"
        image_filename = "Antwerp_Male_c2e6b53b.png"
        image_path = os.path.join(base_path, image_filename)
        image = cv2.imread(image_path)
        image = cv2.resize(image, IMAGE_SIZE)
        predicted_labels = data_model.predict_ensemble(image)