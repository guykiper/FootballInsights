import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
import DataFrameAnalyzer
import DataProcessor
import warnings

warnings.filterwarnings("ignore")



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


if __name__ == "__main__":
    # Create an instance of DataProcessor
    # Create an instance of the DataPreprocessor class
    preprocessor_wide = DataProcessor.DataPreprocessor('female.csv')
    # Preprocess and encode the data (use 'wide' or 'narrow' as desired)
    preprocessor_wide.preprocess_and_encode()
    encoded_data_wide = preprocessor_wide.data

    preprocessor_narrow = DataProcessor.DataPreprocessor('female.csv', 'narrow')
    preprocessor_narrow.preprocess_and_encode()
    encoded_data_narrow = preprocessor_narrow.data

    kmeans_cluster = KMeansCluster(encoded_data_wide)
    DataFrameAnalyzer.DataFrameAnalyzer(encoded_data_wide).elbow_method()
    # Perform clustering with a specified number of clusters
    num_clusters = 4
    kmeans_cluster.perform_clustering(num_clusters)

    # Get the clustered data
    clustered_data = kmeans_cluster.get_clustered_data()


    print('finish')
    # Now 'clustered_data' contains the original data with an additional 'Cluster' column indicating the cluster assignments
