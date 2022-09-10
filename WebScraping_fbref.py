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


def get_link_report_match(url):
    '''
    :param url: url for the season
    :return: list with link to each game in the season
    '''
    counter = 0
    list_links =[]
    page = requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    links = soup.find_all('a')
    for link in links:
        counter += 1
        if ('href' in link.attrs) and ('matches' in str(link.attrs['href'])) and \
                ('La-Liga' in str(link.attrs['href'])) and counter%2!=0 :
            list_links.append('https://fbref.com'+str(link.attrs['href']))
    return list_links




if __name__ == '__main__':
    #print(create_url_season(2000))
    print(get_link_report_match(create_url_season(2000)['2000-2001']))