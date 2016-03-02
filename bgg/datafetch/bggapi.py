import re
import time

import requests
from bs4 import BeautifulSoup

from bgg.util.retry import retry
from lxml import etree

# Number of top games to scrape
N_TOP_GAMES = 5
# The number of games per page in the BGG rankings
N_GAMES_PER_PAGE = 100
# Number of games details to download in a signle API call
API_CHUNK_SIZE = 100
# Default number of seconds to sleep between API calls
API_SLEEP = 120

BGG_RANKING_URL = 'http://www.boardgamegeek.com/browse/boardgame/page/'
BGG_API_URL = "http://www.boardgamegeek.com/xmlapi2/thing?type=boardgame&id={}&stats=1"


def get_bgg_game_ids(n_games=N_TOP_GAMES):
    """Download the IDs of the 'n_games' highest ranked games from BGG.

    :param n_games: Number of games to download from BGG rankings
    """

    # number of pages to scrape
    npages = (n_games - 1) // N_GAMES_PER_PAGE + 1

    @retry(3)
    def __get_page(url):
        page = requests.get(url, timeout=120)
        soup = BeautifulSoup(page.content, 'html.parser')
        # items are found by 'id=results_objectname*' attribute in 'div' tag
        game_ids = []
        for ii in range(1, N_GAMES_PER_PAGE + 1):
            # Get the tag of the ii'th game on this page
            item = soup.find('div', {'id': 'results_objectname' + str(ii)})
            # ID can be found in link href
            href = item.a.attrs['href']
            game_id = re.search(r'/boardgame/(.*)/', href).groups()[0]
            game_ids.append(game_id)
        return game_ids

    all_ids = []
    for pp in range(1, npages + 1):
        print("Reading page {} / {}".format(pp, npages))
        cur_url = BGG_RANKING_URL + str(pp)
        cur_ids = __get_page(cur_url)
        all_ids.extend(cur_ids)

    return all_ids[:n_games]


def get_bgg_game_details(df, sleep=API_SLEEP):
    """Queries the BGG API for details about games.

    :param df: DataFrame with games, must have a 'game_id' column
    :param int sleep: Number of seconds to sleep between API calls
    :rtype: pd.DataFrame
    """
    n_pages = (len(df.index) - 1) // API_CHUNK_SIZE + 1
    for ii in range(1, n_pages + 1):
        print("Gettings stats, chunk {} / {}".format(ii, n_pages))

        start_row = (ii - 1) * API_CHUNK_SIZE
        end_row = ii * API_CHUNK_SIZE - 1
        selection = df['id'].ix[start_row:end_row]
        url = BGG_API_URL.format(','.join(selection.astype(str)))

        result = requests.get(url, timeout=60)
        elem = etree.fromstring(result.content)

        items = elem.iterchildren()
        for jj, item in enumerate(items):
            row = start_row + jj
            df.ix[row, 'name'] = [nm.attrib['value']
                                  for nm in item.findall('name')
                                  if nm.attrib['type'] == 'primary'][0]
            df.ix[row, 'year'] = item.find('yearpublished').attrib['value']
            df.ix[row, 'minplayers'] = item.find('minplayers').attrib['value']
            df.ix[row, 'maxplayers'] = item.find('maxplayers').attrib['value']
            df.ix[row, 'minplaytime'] = item.find('minplaytime').attrib['value']
            df.ix[row, 'maxplaytime'] = item.find('maxplaytime').attrib['value']

            # Use regex to deal with cases like '21 and up'.
            age_str = item.find('minage').attrib['value']
            df.ix[row, 'minage'] = re.findall(r'\d+', age_str)[0]

            df.ix[row, 'boardgamecategory'] = \
                ','.join(x.attrib['value'] for x in item.findall('link')
                         if x.attrib['type'] == 'boardgamecategory')
            df.ix[row, 'boardgamemechanic'] = \
                ','.join(x.attrib['value'] for x in item.findall('link')
                         if x.attrib['type'] == 'boardgamemechanic')
            df.ix[row, 'boardgamedesigner'] = \
                ','.join(x.attrib['value'] for x in item.findall('link')
                         if x.attrib['type'] == 'boardgamedesigner')
            df.ix[row, 'boardgameartist'] = \
                ','.join(x.attrib['value'] for x in item.findall('link')
                         if x.attrib['type'] == 'boardgameartist')

            # Statistics
            df.ix[row, 'usersrated'] = item.find('statistics').find('ratings')\
                .find('usersrated').attrib['value']
            df.ix[row, 'average'] = item.find('statistics').find('ratings')\
                .find('average').attrib['value']
            df.ix[row, 'bayesaverage'] = item.find('statistics').find('ratings')\
                .find('bayesaverage').attrib['value']
            df.ix[row, 'stddev'] = item.find('statistics').find('ratings')\
                .find('stddev').attrib['value']
            df.ix[row, 'averageweight'] = item.find('statistics').find('ratings')\
                .find('averageweight').attrib['value']

        time.sleep(sleep)

    # Convert numerical columns to proper dtypes.
    dtypes = {
        'year': int,
        'minplayers': int,
        'maxplayers': int,
        'minplaytime': int,
        'maxplaytime': int,
        'minage': int,
        'usersrated': int,
        'average': float,
        'bayesaverage': float,
        'stddev': float,
        'averageweight': float,
    }
    for col, dtype in dtypes.items():
        df[col] = df[col].astype(dtype)

    return df
