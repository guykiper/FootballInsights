import pandas as pd
from bs4 import BeautifulSoup
import requests
import regex
import lxml
import json
import time
from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry
from urllib3.util import Retry
from collections import defaultdict
from Class_ElasticSearch import Elasticsearch_conn
class WebScraping_fbref:

    def __init__(self, team_name,code_club, conv_json=True):
        self.code_club = code_club
        self.team_name = team_name
        self.fbref_site = 'https://fbref.com/en/squads/'+str(code_club)+'/'
        self.gender = ''
        # TODO # if we want to make in more general and not only for Real
        # Madrid we will need to change self.fbref_site_end
        #https://fbref.com/en/squads/53a2f082/2000-2001/all_comps/Real-Madrid-Stats-All-Competitions
        self.fbref_site_end = '/all_comps/'+team_name+'-Stats-All-Competitions'
        # self.fbref_site_end = '/Real-Madrid-Stats'
        self.conv_json = conv_json
        self.dealy_time = 1
        # self.session = requests.Session()
        # self.retry_strategy = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        # self.adapter = HTTPAdapter(max_retries=self.retry_strategy)
        # self.session.mount('http://', self.adapter)
        # self.session.mount('https://', self.adapter)

    def create_url_season(self, year):
        """
        :param year: the year which the season start in
        :return: a dictionary , the key is the year and the value is the url site of the season
        """
        #https://fbref.com/en/squads/53a2f082/2000-2001/all_comps/Real-Madrid-Stats-All-Competitions
        try:
            season_url = (self.fbref_site + str(year) + '-' + str(year + 1) + self.fbref_site_end)
        except:
            season_url = "not found"
        return season_url

    def get_link_report_match(self, url):
        # TODO: currently this function is not working
        """
        :param url: url for the season
        :return: list with link to each game in the season
        """
        counter = 0
        list_links = []
        page = requests.get(url)
        # time.sleep(self.dealy_time)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('a')
        for link in links:
            counter += 1
            if ('href' in link.attrs) and ('matches' in str(link.attrs['href'])) and \
                    ('La-Liga' in str(link.attrs['href'])) and counter % 2 != 0:
                list_links.append('https://fbref.com' + str(link.attrs['href']))
                # time.sleep(self.dealy_time)
        return pd.Series(list_links)

    def try_get_link_report_match(self, url):
        # TODO: currently this function is not working
        """
        :param url: url for the season
        :return: list with link to each game in the season
        """
        counter = 0
        list_links = []
        page = requests.get(url)
        # time.sleep(self.dealy_time)
        soup = BeautifulSoup(page.content, 'html.parser')
        elements = soup.find_all(class_="left group_start")
        for element in elements:
            try:
                link = element.find('a')['href']
                list_links.append('https://fbref.com' + str(link))
                # time.sleep(self.dealy_time)
            except:
                continue
        return pd.Series(list_links)

    def gender_dataframe(self,url):
        """
        :param url: the url that contanite the dataframe, there are teams of males and females.
        :return: creating a method that will ditermane if the data is about male or female
        """
        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')
        links = soup.find_all('strong')
        res_title = [ele.text.strip() for ele in links]
        for i, text in enumerate(res_title):
            if text == 'Gender':
                try:
                    self.gender = str(links[i].nextSibling)
                except:
                    self.gender = ''
            else:
                continue

    def compation_df(self, url, comp='La Liga'):
        """
        :param conv_json: boolean operator for the decision of what the outpur will be
        :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
        :param comp:the compation of real madrid : La Liga,Champions Lg,Copa del Rey,Supercopa de Espana
        :return: return the data frame of the comp that were chosen from the url
        """
        time.sleep(self.dealy_time)
        # response = self.session.get(url)
        # response.raise_for_status()
        dfs = pd.read_html(url)
        season_df = dfs[1]
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

    def Naition_transformation(self,dataframe):
        if "Nation" in dataframe.columns and dataframe["Nation"].dtype == "object":
            dataframe["Nation"] = dataframe["Nation"].str.extract(r'([A-Z]+)')
        return dataframe
    def age_transformaiton(self, dataframe):
        '''
        the function convert the column of age from the format of string to numerical value,
        we get the age from the format of "age in years - number of days after the last birthday"
        for example "34-121" -> 34
        :param dataframe:
        :return: dataframe
        '''
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

    def rename_unnamed_columns(self, dataframe):
        '''
        Function to rename columns starting with "Unnamed" based on the text appearing after the "/" character.

        :param dataframe: Input DataFrame.
        :return: DataFrame with renamed columns.
        '''

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

    # def all_compation_df(self, url):
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
    #             transformed_df = self.age_transformaiton(df_comp)
    #             transformed_df = self.Naition_transformation(transformed_df)
    #             json_data = transformed_df.to_json(orient='records')
    #             parsed_data = json.loads(json_data)
    #             results.append(parsed_data)
    #         return results
    #         # return [json.loads(self.age_transformaiton(df_comp).to_json(orient='records')) for df_comp in dfs]
    #     else:
    #         return dfs
    #
    def all_compation_df(self, url):

        """
        :param conv_json: boolean operator for the decision of what the outpur will be
        :param url: get url for a specific season ,example:create_url_all_seasons()['2019-2020']
        :return: return the data frame of the comp that were chosen from the url
        """
        time.sleep(self.dealy_time)
        # response = self.session.get(url)
        # response.raise_for_status()
        try:
            dfs = pd.read_html(url)
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            links = soup.find_all('h2')
            res_title = [ele.text.strip() for ele in links if len(ele.contents) == 2]

        except:
            # when there is an error in reading the tables from the web page we return this
            return [(" ", pd.DataFrame())]
        results = []

        for df_comp in dfs:
            df_comp = self.rename_unnamed_columns(df_comp)
            df_comp = self.age_transformaiton(df_comp)
            df_comp = self.Naition_transformation(df_comp)
            df_comp['team'] = self.team_name
            if self.conv_json == True:
                json_data = df_comp.to_json(orient='records')
                parsed_data = json.loads(json_data)
                results.append(parsed_data)
            else:
                results.append(df_comp)

        combined_list = [(x, y) for x, y in zip(res_title, results)]
        return combined_list

            # return [json.loads(self.age_transformaiton(df_comp).to_json(orient='records')) for df_comp in dfs]

    #
    def list_df_player_madrid(self, season):
        """
        :param season: the df of the season, example: compation_df(url_season_2019)
        :return: dataframe of players for each game in the season
        """
        list_home_away = [0 if venue == 'Home' else 1 for venue in list(season['Venue'])]
        list_link_venue = zip(season['Match Report'], list_home_away)
        test = set(list(list_link_venue))
        list_df_players = []
        for tup in test:
            link, venue = tup
            try:
                df_match_player = pd.read_html(link)
                # list_df_players.append(df_match_player[venue])
                list_df_players.append(df_match_player)
                time.sleep(self.dealy_time)
                # print(len(list_df_players))
            except:
                continue
        return list_df_players

    def Read_table_html(self, link):
        """
        :param link: the link of a website
        :return: retrun a list of data frame from the page of the website
        """

        return pd.read_html(link)

    def list_of_clubs(self, link):
        """
        :param link: the link will be a link to a country that in the websit containe the list of clubs, for example  https://fbref.com/en/country/clubs/FRA/France-Football-Clubs
        :return: return a list of all the clubs name
        """
        try:
            return list(self.Read_table_html(link)[0]['Squad'])
        except:
            return []




    def DF_to_json(self, dataframe,path_name):
        """
        the goal of the function is to write the data frame to other file, for example to json file
        :param dataframe: the dataframe you want to load
        :param path_name: the path name which could be the file name and the file type: example - output.json
        :return: the fucniton create the file and load to it the data from the datafarme
        """
        json_str = json.dumps(dataframe)
        # file_path = "output.json"
        with open(path_name, "w") as json_file:
            json_file.write(json_str)

    def all_clubs_name(self, gender='Male'):
        """
        the goal of this function is to create a file with all the clubs name in each country and the code club
        :return: dictonary with all the information
        """
        url = "https://fbref.com/en/squads/"
        response = requests.get(url)
        # Create a BeautifulSoup object to parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        # Find all <a> tags that contain "clubs" in the text
        link_elements = soup.find_all("a")
        # Extract the links and their corresponding text
        dict_club = defaultdict(list) # {key - country name(text): value - list of tuple -(club name, link) }
        links_country = []  # a list of tuples that the first value is the link the to the clubs in this country and the seconed value is the name of the country
        for element in link_elements:
            text = element.text.strip()
            list_text = text.split()
            if len(list_text) == 0:
                continue
            if list_text[-1] == 'Clubs' and len(list_text) > 1:
                link = 'https://fbref.com/' + element["href"]
                links_country.append((link, text))

        for country_link in links_country:
            print(country_link[1])
            time.sleep(self.dealy_time+5)
            response = requests.get(country_link[0])
            soup = BeautifulSoup(response.content, "html.parser")
            link_elements_club = soup.find_all("a")

            for club in self.list_of_clubs(country_link[0]):
                try:
                    for element in link_elements_club:
                        text_club = element.text.strip()
                        # list_text = text.split()
                        if text_club == club :
                            link = 'https://fbref.com/' + element["href"]
                            code_club = element["href"].split('/')[3]
                            club_name = ' '.join(element["href"].split("/")[-1].split("-")[:-3])
                            dict_club[country_link[1]].append((club_name, link, code_club))
                    self.DF_to_json(dict_club, "club_code.json")

                except:

                    dict_club[country_link[1]].append(("", ""))

        return dict_club


    # def United_Player_dataframe(self, dataframe_list):
    #     """
    #     the goal of this function is to unite all the dataframe of the players from a club to one dataframe
    #     :param dataframe_list: list of all the dataframe from a specific club
    #     :return: one united dataframe
    #     """


if __name__ == '__main__':
    connection = Elasticsearch_conn(
        elastic_password="",
        elastic_username="",
        elastic_index_name="real_madrid_2022",

    )

    with open("club_code.json", "r") as file:
        json_data = file.read()
    # Convert JSON data to dictionary
    data = json.loads(json_data)
    counter = 0
    df_result = pd.DataFrame(columns=['club_name', 'club_code', 'link'])
    for country in data.keys():
        for club in data[country]:
            counter = counter +1
            club_name_link = club[0].replace(" ", "-")
            club_code = club[2]
            # web_screp = WebScraping_fbref("Real-Madrid", "53a2f082", conv_json=False)
            web_screp = WebScraping_fbref(club_name_link, club_code, conv_json=False)
            link = web_screp.create_url_season(2022)
            web_screp.gender(link)
            df_games = web_screp.all_compation_df(link)
            if df_games[0][1].empty:
                print(counter)
                print(link)

                continue
            elif 'Player' in df_games[0].columns:
                print(counter)
                print(web_screp.team_name)
                print(link)
                print(df_games[0])
                new_row = pd.Series([web_screp.team_name, web_screp.code_club, link], index=df_result.columns)
                df_result = pd.concat([df_result, new_row], ignore_index=True)
                # df_result = df_result.append(pd.Series([web_screp.team_name, web_screp.code_club, link],index=df_result.columns),ignore_index=True)

            else:
                print(counter)
    #
    #             continue

    #
    # web_screp = WebScraping_fbref("Real-Madrid",'53a2f082', conv_json=False)
    # # print(web_screp.create_url_season(2022))
    # #load to elasticsearch
    # link = web_screp.create_url_season(2022)
    # df_games = web_screp.all_compation_df(link)
    # print(len(df_games))
    # # print(df_games[0])

    # x = web_screp.all_clubs_name()
    # connection.load_data(df_games[0])
    # list_player = web_screp.list_df_player_madrid(df_games)
    # x =
    # web_screp.DF_to_json(dataframe=df_games[0], path_name="output.json")







