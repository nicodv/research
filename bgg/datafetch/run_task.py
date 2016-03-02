#!/usr/bin/env python
import sys
import argparse

from bgg.datafetch.bggapi import get_bgg_game_ids, get_bgg_game_details
from bgg.dao.gamesdao import GamesDAO


def update_game_ids():
    """Download top games from BGG and stores their IDs in database."""
    top_games = get_bgg_game_ids()
    with GamesDAO() as dao:
        dao.update_ids(top_games)


def update_games(new_only=False):
    """Update game data in database.

    :param bool new_only: Whether or not to only update games that seem
        to be new (i.e., that have an ID but no name).
    """
    with GamesDAO() as dao:
        games = dao.load_games()
        if new_only:
            games = games[games['name'] == '']
            sleep = 0
        else:
            sleep = 60
        updated_games = get_bgg_game_details(games, sleep=sleep)
        dao.save_games(updated_games)


def main(task):
    if task == 'update_new_games':
        update_games(new_only=True)
    elif task == 'update_all_games':
        update_games(new_only=False)
    else:
        eval(task + '()')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('task', help='Task to run')
    args = parser.parse_args()

    sys.exit(main(args.task))
