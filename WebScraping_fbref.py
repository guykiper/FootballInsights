import pandas as pd
from bs4 import BeautifulSoup
import requests
import regex


def create_url_season(year):
    '''
    :param year: the year which the seasno start in
    :return: a dictionary , the key is the year and the value is the url site of the season
    '''
    fbref_site = 'https://fbref.com/en/squads/53a2f082/'
    fbref_site_end = '/Real-Madrid-Stats'
    dict_season_url = {}
    dict_season_url[str(year) + '-' + str(year + 1)] = (fbref_site + str(year) + '-' + str(year + 1) + fbref_site_end)
    return (dict_season_url)



if __name__ == '__main__':
    print(create_url_season(2000))