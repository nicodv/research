import pandas as pd
import psycopg2

from bgg.util.retry import retry
from bgg.util import read_local_cfg

CFG = read_local_cfg()


class GamesDAO(object):

    @retry(3)
    def __init__(self):

        # define our connection string
        conn_string = 'host={} port={}, dbname={} user={} password={}'.format(
            CFG.get('DB_HOST', 'localhost'),
            CFG.get('DB_PORT', 5432),
            CFG['DB_NAME'],
            CFG['DB_USER'],
            CFG['DB_PASS']
        )

        # make connection, which is kept open until class instance is closed
        try:
            self.conn = psycopg2.connect(conn_string)
        except Exception:
            print("Database connection could not be made.")
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # make sure database connection is closed
        self.conn.commit()
        self.conn.close()

    def execute_sql(self, sql, args=None, fetch=None, many=False):
        """Execute an SQL statement.

        :param str sql: SQL string to execute
        :param list args: Data to feed to the cursor.execute(many) statement
        :param fetch: In case of a read statement; 'one' for fetchone, 'all' for fetchall
        :type fetch: None or str
        :param bool many: Whether to use 'execute' or 'executemany'
        """
        # 'with' statement takes care of commits/rollbacks, and
        # automatically closes cursor
        with self.conn.cursor() as cur:
            if args is not None:
                if many:
                    cur.executemany(sql, args)
                else:
                    cur.execute(sql, args)
            else:
                assert many is False
                cur.execute(sql)

            # save columns that were read, for later use
            if cur.description:
                # noinspection PyAttributeOutsideInit
                self.readColumns = [col_desc[0] for col_desc in cur.description]

            # in case we are fetching things, do this and return the read values
            r = None
            if fetch == 'all':
                r = cur.fetchall()
            elif fetch == 'one':
                r = cur.fetchone()
            return r

    def get_all_ids(self):
        return self.load_games(columns=['game_id'])

    def load_games(self, columns=None, where=None):
        if columns is None:
            col_str = '*'
        else:
            col_str = ','.join(columns)

        if where is None:
            where = ''

        with self.conn.cursor() as cur:
            cur.execute("SELECT {} FROM boardgames {}}".format(col_str, where))
            games = cur.fetchall()

        return pd.DataFrame(games, columns=columns)

    def load_game(self, game_id, columns=None):
        return self.load_games(columns=columns, where='WHERE game_id == {}'.format(game_id))

    def update_ids(self, new_ids):
        # Find which of the new IDs to add by differencing with existing IDs.
        old_ids = self.get_all_ids()
        to_add = list(set(new_ids).difference(set(old_ids)))
        # Insert new IDs into table.
        with self.conn.cursor() as cur:
            cur.executemany("INSERT INTO boardgames (game_id) VALUES (%s)", to_add)

    def save_games(self, df):
        columns = df.columns
        with self.conn.cursor() as cur:
            cur.executemany("INSERT INTO boardgames ({}) VALUES (%s)"
                            .format(columns), df.values)
