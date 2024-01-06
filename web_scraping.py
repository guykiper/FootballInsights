import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import time
from collections import defaultdict
from Class_ElasticSearch import Elasticsearch_conn
from io import BytesIO
from PIL import Image
import os

class WebScraping_fbref:
    def __init__(self, team_name, code_club, year, conv_json=True):
        self.code_club = code_club
        self.team_name = team_name
        self.year = year
        self.conv_json = conv_json
        self.delay_time = 3
        self.link = self.create_url_season()
        self.gender = ''
        self.gender_dataframe(self.link)

    def create_url_season(self):
        """
        :return: a dictionary , the key is the year and the value is the url site of the season
        """
        fbref_site = 'https://fbref.com/en/squads/' + str(self.code_club) + '/'
        fbref_site_end = '/all_comps/' + self.team_name + '-Stats-All-Competitions'

        # https://fbref.com/en/squads/53a2f082/2000-2001/all_comps/Real-Madrid-Stats-All-Competitions
        try:
            season_url = (fbref_site + str(self.year) + '-' + str(self.year + 1) + fbref_site_end)
        except Exception as e:
            print(f"An error occurred: {e}")
            season_url = "not found"
        return season_url

    @staticmethod
    def get_link_report_match(url):
        # TODO: currently this function is not working
        """
        :param url: url for the season
        :return: list with link to each game in the season
        """
        counter_help = 0
        list_links = []
        page = requests.get(url)
        # time.sleep(self.delay_time)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        for link_inner in links:
            counter_help += 1
            if ('href' in link_inner.attrs) and ('matches' in str(link_inner.attrs['href'])) and \
                    ('La-Liga' in str(link_inner.attrs['href'])) and counter_help % 2 != 0:
                list_links.append('https://fbref.com' + str(link_inner.attrs['href']))
                # time.sleep(self.delay_time)
        return pd.Series(list_links)

    # def try_get_link_report_match(self, url):
    #     # TODO: currently this function is not working
    #     """
    #     :param url: url for the season
    #     :return: list with link to each game in the season
    #     """
    #     list_links = []
    #     page = requests.get(url)
    #     # time.sleep(self.dealy_time)
    #     soup = BeautifulSoup(page.content, 'html.parser')
    #     elements = soup.find_all(class_="left group_start")
    #     for element in elements:
    #         try:
    #             link_inner = element.find('a')['href']
    #             list_links.append('https://fbref.com' + str(link_inner))
    #             # time.sleep(self.dealy_time)
    #         except:
    #             continue
    #     return pd.Series(list_links)

    def gender_dataframe(self, url):
        """
        :param url: the url that contanite the dataframe, there are teams of males and females.
        :return: creating a method that will ditermane if the data is about male or female
        """
        time.sleep(self.delay_time)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('strong')
        res_title = [ele.text.strip() for ele in links]
        for i, text in enumerate(res_title):
            if text == 'Gender':
                try:
                    self.gender = str(links[i].nextSibling)[2:]
                except Exception as e:
                    print(f"An error occurred: {e}")
                    self.gender = ''
            else:
                continue

    def competition_df(self, url, comp='La Liga'):
        """
        :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
        :param comp:the compation of real madrid : La Liga,Champions Lg, Copa del Rey, Supercopa de Espana
        :return: return the data frame of the comp that were chosen from the url
        """
        time.sleep(self.delay_time)
        dfs = pd.read_html(url)
        for df in dfs:
            if 'Comp' in list(df.columns):
                season_df = df
                break
            else:
                continue
        # season_df.drop('Notes', inplace=True, axis=1)# remove column 'Notes'
        # season_df = season_df[season_df['Comp'] == comp]
        season_df = season_df.loc[season_df['Comp'] == comp].copy()
        # edit the column 'Match Report' with the link of the match report
        # season_df['Match Report'] = self.get_link_report_match(url)
        season_df.loc[:, 'Match Report'] = self.get_link_report_match(url)
        if self.conv_json:
            return season_df.to_json(orient='records')
        else:
            return season_df

    @staticmethod
    def nation_transformation(dataframe):
        if "Nation" in dataframe.columns and dataframe["Nation"].dtype == "object":
            dataframe["Nation"] = dataframe["Nation"].str.extract(r'([A-Z]+)')
        return dataframe

    @staticmethod
    def age_transformation(dataframe):
        """
        the function convert the column of age from the format of string to numerical value,
        we get the age from the format of "age in years - number of days after the last birthday"
        for example "34-121" -> 34
        :param dataframe:
        :return: dataframe
        """
        # Copy the original DataFrame to avoid modifying the input DataFrame
        transformed_df = dataframe.copy()

        # Check if 'age' column exists in the DataFrame
        if 'Age' in transformed_df.columns:
            if transformed_df['Age'].dtype == 'object' or transformed_df['Age'].dtype == 'str':

                # Split the 'age' column into two parts: age in years and number of days
                age_parts = transformed_df['Age'].str[:2]
                transformed_df['Age'] = age_parts.astype(int)
                return transformed_df

            else:
                return transformed_df
        else:
            return transformed_df

    @staticmethod
    def rename_unnamed_columns(dataframe):
        """
        Function to rename columns starting with "Unnamed" based on the text appearing after the "/" character.

        :param dataframe: Input DataFrame.
        :return: DataFrame with renamed columns.
        """

        renamed_columns = []
        for column in dataframe.columns:
            if column[0].startswith("Unnamed"):
                new_column_name = column[1]
                renamed_columns.append(new_column_name)
            else:
                renamed_columns.append(column)

        renamed_dataframe = dataframe.copy()
        renamed_dataframe.columns = renamed_columns

        return renamed_dataframe

    # def all_competition_df(self, url):
    #
    #     """
    #     :param conv_json: boolean operator for the decision of what the outpur will be
    #     :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
    #     :return: return the data frame of the comp that were chosen from the url
    #     """
    #     time.sleep(self.dealy_time)
    #     # response = self.session.get(url)
    #     # response.raise_for_status()
    #     dfs = pd.read_html(url)
    #     if self.conv_json:
    #         results = []
    #         for df_comp in dfs:
    #             df_comp = self.rename_unnamed_columns(df_comp)
    #             transformed_df = self.age_transformation(df_comp)
    #             transformed_df = self.nation_transformation(transformed_df)
    #             json_data = transformed_df.to_json(orient='records')
    #             parsed_data = json.loads(json_data)
    #             results.append(parsed_data)
    #         return results
    #         # return [json.loads(self.age_transformation(df_comp).to_json(orient='records')) for df_comp in dfs]
    #     else:
    #         return dfs
    #
    def all_competition_df(self, url):

        """
        :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
        :return: return the data frame of the comp that were chosen from the url
        """
        try:
            time.sleep(self.delay_time)
            dfs = pd.read_html(url)
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            links = soup.find_all('h2')
            res_title = [ele.text.strip() for ele in links if len(ele.contents) == 2]

        except Exception as e:
            print(f"An error occurred: {e}")
            # when there is an error in reading the tables from the web page we return this
            return [(" ", pd.DataFrame())]
        results = []

        for df_comp in dfs:
            df_comp = self.rename_unnamed_columns(df_comp)
            df_comp = self.age_transformation(df_comp)
            df_comp = self.nation_transformation(df_comp)
            df_comp['team'] = self.team_name
            new_column_name = []
            for col in df_comp.columns:
                if type(col) == str:
                    new_column_name.append(col)
                elif type(col) == tuple:
                    new_column_name.append(str(col[0] + " - " + col[1]))
            df_comp.columns = new_column_name
            if self.conv_json:
                json_data = df_comp.to_json(orient='records')
                parsed_data = json.loads(json_data)
                results.append(parsed_data)
            else:
                results.append(df_comp)

        combined_list = [(x, y) for x, y in zip(res_title, results)]
        return combined_list

        # return [json.loads(self.age_transformation(df_comp).to_json(orient='records')) for df_comp in dfs]

    #
    def list_df_player_madrid(self, season):
        """
        :param season: the df of the season, example: competition_df(url_season_2019)
        :return: dataframe of players for each game in the season
        """
        list_home_away = [0 if venue == 'Home' else 1 for venue in list(season['Venue'])]
        list_link_venue = zip(season['Match Report'], list_home_away)
        test = set(list(list_link_venue))
        list_df_players = []
        for link_inner, venue in test:
            try:
                df_match_player = pd.read_html(link_inner)
                # list_df_players.append(df_match_player[venue])
                list_df_players.append(df_match_player)
                time.sleep(self.delay_time)
                # print(len(list_df_players))
            except Exception as e:
                print(f"An error occurred: {e}")
                continue
        return list_df_players

    @staticmethod
    def read_table_html(link_inner):
        """
        :param link_inner: the link of a website
        :return: a list of data frame from the page of the website
        """

        return pd.read_html(link_inner)

    def list_of_clubs(self, link_inner):
        """
        :param link_inner: the link will be a link to a country that in the website contain the list of clubs,
         for example  https://fbref.com/en/country/clubs/FRA/France-Football-Clubs
        :return: return a list of all the clubs name
        """
        try:
            return list(self.read_table_html(link_inner)[0]['Squad'])
        except Exception as e:
            print(f"An error occurred: {e}")
            return []

    @staticmethod
    def df_to_json(dataframe, path_name):
        """
        the goal of the function is to write the data frame to other file, for example to json file
        :param dataframe: the dataframe you want to load
        :param path_name: the path name which could be the file name and the file type: example - output.json
        :return: the function create the file and load to it the data from the dataframe
        """
        dataframe.to_json(path_name, orient="records")

    def all_clubs_name(self):
        """
        the goal of this function is to create a file with all the clubs name in each country and the code club
        :return: dictonary with all the information
        """
        time.sleep(self.delay_time)
        url = "https://fbref.com/en/squads/"
        response = requests.get(url)
        # Create a BeautifulSoup object to parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        # Find all <a> tags that contain "clubs" in the text
        link_elements = soup.find_all("a")
        # Extract the links and their corresponding text
        dict_club = defaultdict(list)  # {key - country name(text): value - list of tuple -(club name, link) }
        links_country = []  # a list of tuples that the first value is the link the to the clubs in this country and
        # the seconed value is the name of the country
        for element in link_elements:
            text = element.text.strip()
            list_text = text.split()
            if len(list_text) == 0:
                continue
            if list_text[-1] == 'Clubs' and len(list_text) > 1:
                link_inner = 'https://fbref.com/' + element["href"]
                links_country.append((link_inner, text))

        for country_link in links_country:
            print(country_link[1])
            time.sleep(self.delay_time + 2)
            response = requests.get(country_link[0])
            soup = BeautifulSoup(response.content, "html.parser")
            link_elements_club = soup.find_all("a")

            for club_iter in self.list_of_clubs(country_link[0]):
                try:
                    for element in link_elements_club:
                        text_club = element.text.strip()
                        # list_text = text.split()
                        if text_club == club_iter:
                            inner_link = 'https://fbref.com/' + element["href"]
                            code_club = element["href"].split('/')[3]
                            club_name = ' '.join(element["href"].split("/")[-1].split("-")[:-3])
                            dict_club[country_link[1]].append((club_name, inner_link, code_club))
                    self.df_to_json(dict_club, "club_code.json")
                except Exception as e:
                    # Print the error message and the club name that caused the error
                    print(f"Error: {e} - {club_iter}")
                    dict_club[country_link[1]].append(("", ""))
        return dict_club

    def df_club_info(self, input_file, output_file):

        with open(input_file, "r") as file:
            data = file.read()
        # Convert JSON data to dictionary
        dictonary_club = json.loads(data)
        counter = 0
        df_result = pd.DataFrame(columns=['country', 'club_name', 'club_code', 'link', 'gender', 'year', 'counter'])
        self.year = 2022
        for country in dictonary_club.keys():
            for club in dictonary_club[country]:
                print(df_result)
                counter = counter + 1
                self.team_name = club[0].replace(" ", "-")
                self.code_club = club[2]
                # web_screp = WebScraping_fbref("Real-Madrid", "53a2f082", conv_json=False)
                # web_screp = WebScraping_fbref(club_name_link, club_code, year, conv_json=False)
                link = self.create_url_season()
                self.gender_dataframe(link)
                df_games = self.all_competition_df(link)
                if counter % 10 == 0:
                    df_result.to_csv(output_file)
                if df_games[0][1].empty:

                    continue
                elif 'Player' in df_games[0][1].columns:
                    # new_row = pd.DataFrame([country, web_screp.team_name, web_screp.code_club, link, web_screp.gender],
                    # index=df_result.columns)
                    if self.code_club in list(df_result['club_code']):
                        continue
                    else:
                        df_result = df_result.append({'country': country, 'club_name': self.team_name,
                                                      'club_code': self.code_club, 'link': link,
                                                      'gender': self.gender, 'year': self.year,
                                                      'counter': counter}, ignore_index=True)
                    # df_result = df_result.append(pd.Series([web_screp.team_name, web_screp.code_club, link],
                    # index=df_result.columns),ignore_index=True)

                else:
                    print(counter)

    # def United_Player_dataframe(self, dataframe_list):
    #     """
    #     the goal of this function is to unite all the dataframe of the players from a club to one dataframe
    #     :param dataframe_list: list of all the dataframe from a specific club
    #     :return: one united dataframe
    #     """


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
        df = pd.read_csv("Data_files/backup_new.csv")

        # Create a directory to store the images
        output_directory = "Data_files/downloaded_images"
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


if __name__ == '__main__':

    #webscraping_fbref

    connection = Elasticsearch_conn(
        elastic_password="",
        elastic_username="",
        elastic_index_name="real_madrid_2022",

    )

    # web = WebScraping_fbref("", "", 2022, conv_json=False)
    # # web.df_club_info("club_code.json", "club_2022.csv")
    #
    # with open("club_code.json", "r") as file:
    #     data = file.read()
    # # Convert JSON data to dictionary
    # dictonary_club = json.loads(data)
    # counter = 0
    # df_result = pd.DataFrame(columns=['country', 'club_name', 'club_code', 'link', 'gender', 'year','counter'])
    # year = 2022
    # for country in dictonary_club.keys():
    #     for club in dictonary_club[country]:
    #         counter = counter + 1
    #         print(counter)
    #         if counter <1488:
    #             continue
    #         club_name_link = club[0].replace(" ", "-")
    #         club_code = club[2]
    #         # web_screp = WebScraping_fbref("Real-Madrid", "53a2f082", conv_json=False)
    #         web_screp = WebScraping_fbref(club_name_link, club_code, year, conv_json=False)
    #
    #         link = web_screp.create_url_season()
    #         web_screp.gender_dataframe(link)
    #         df_games = web_screp.all_competition_df(link)
    #         if counter % 10 == 0:
    #             print('read to csv')
    #             df_result.to_csv('information_clubs_1.csv')
    #         if df_games[0][1].empty:
    #             print('df is empty')
    #
    #             continue
    #         elif 'Player' in df_games[0][1].columns:
    #             print('first df containe player')
    #             # new_row = pd.DataFrame([country, web_screp.team_name, web_screp.code_club, link, web_screp.gender],
    #             # index=df_result.columns)
    #             if web_screp.code_club in list(df_result['club_code']):
    #                 continue
    #             else:
    #                 df_result = df_result.append({'country': country, 'club_name': web_screp.team_name,
    #                                               'club_code': web_screp.code_club, 'link': link,
    #                                               'gender': web_screp.gender, 'year': web_screp.year,
    #                                               'counter': counter}, ignore_index=True)
    #             # df_result = df_result.append(pd.Series([web_screp.team_name, web_screp.code_club, link],
    #             # index=df_result.columns),ignore_index=True)
    #
    #         else:
    # #
    #             continue

    #
    web_screp = WebScraping_fbref("Real-Madrid", '53a2f082', 2022, conv_json=False)
    print(type(web_screp.competition_df(web_screp.link)))
    # # print(web_screp.create_url_season(2022))
    # #load to elasticsearch
    # link = web_screp.create_url_season(2022)
    # df_games = web_screp.all_competition_df(link)
    # print(len(df_games))
    # # print(df_games[0])
    # x = web_screp.all_clubs_name()
    # connection.load_data(df_games[0])
    # list_player = web_screp.list_df_player_madrid(df_games)
    # x =
    # web_screp.df_to_json(dataframe=df_games[0], path_name="output.json")


    #image scraper
    csv_file = 'Data_files/backup.csv'
    image_scraper = ImageScraper(csv_file)
    image_lists = image_scraper.scrape_images()

    for index, images in enumerate(image_lists):
        print(f"Images for row {index} - {image_scraper.data.iloc[index]['club_name']}:")
        for image in images:
            print(image)