from bs4 import BeautifulSoup
import time
import pandas as pd
import requests
from io import BytesIO
from PIL import Image
import os



"""
The ImageScraper class is designed to facilitate the extraction of image URLs from web pages linked in a CSV file.
It reads a CSV file containing data with links and provides a method (scrape_images) to scrape images for each row in the CSV,
returning a list of image URLs for each row.
This class simplifies the process of collecting images from multiple web pages in a structured and organized manner.
"""
class ImageScraper:
    def __init__(self, csv_file):
        """
        Initialize the ImageScraper class with the path to the CSV file.

        Args:
            csv_file (str): Path to the CSV file containing data with links.
        """
        self.delay_time = 3
        self.csv_file = csv_file
        self.data = pd.read_csv(csv_file)

    def scrape_images(self):
        """
        Scrape images for each row in the CSV file and return a list of image URLs for each row.

        Returns:
            list: A list of lists containing image URLs for each row.
        """
        image_lists = []
        counter = 0
        for index, row in self.data.iterrows():
            link = row['link']
            images = self.extract_images_from_link(link)
            counter += 1
            print(counter)
            print(images)
            image_lists.append(images)
        self.data['team_image_link'] = image_lists
        self.data.to_csv(self.csv_file, index=False)
        return image_lists

    def extract_images_from_link(self, link):
        """
        Extract image URLs from a given web page link.

        Args:
            link (str): URL of the web page to scrape images from.

        Returns:
            list: A list of image URLs found on the web page.
        """
        time.sleep(self.delay_time)
        try:
            response = requests.get(link)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Find the <meta> tag with property="og:image"
                meta_tag = soup.find('meta', attrs={'property': 'og:image'})
                if meta_tag:
                    image_url = meta_tag.get('content')

        except Exception as e:
            print(f"Error scraping images from {link}: {str(e)}")

        return image_url

    @staticmethod
    def create_folder_image():
        df = pd.read_csv("backup_new.csv")

        # Create a directory to store the images
        output_directory = "downloaded_images"
        os.makedirs(output_directory, exist_ok=True)

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


if __name__ == "__main__":
    csv_file = 'backup.csv'
    image_scraper = ImageScraper(csv_file)
    image_lists = image_scraper.scrape_images()

    for index, images in enumerate(image_lists):
        print(f"Images for row {index} - {image_scraper.data.iloc[index]['club_name']}:")
        for image in images:
            print(image)
