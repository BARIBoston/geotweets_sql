#!/usr/bin/env python3

import multiprocessing
import msgpack
import psycopg2
import shapely.geometry
import sys

NLONG = "$numberLong"
SPLITS = 4

def getitem_or_none(dict_, key):
    """ Attempt to get an item from a dict; if it does not exist, return None
    """
    if (key in dict_):
        return dict_[key]
    else:
        return None

def convert_nlong(nlong):
    """ Extract numberLong from a Mongo value if necessary """
    try:
        return nlong[NLONG]
    except TypeError:
        return nlong

class Importer(object):

    def __init__(self):
        """ Initialize Importer object

        Args:
            commit_interval: The number of tweets that a commit should be
                automatically made after
        """

        self.connection = psycopg2.connect("dbname = geotweets")
        self.cursor = self.connection.cursor()
        self.n_imported = 0

    def import_tweets(self, split_n):
        """ Import a tweet """

        with open("users%d.msgpack" % split_n, "rb") as f:
            unpacker = msgpack.Unpacker(f)
            for row in unpacker:
                self.cursor.execute(
                    """INSERT INTO users
                        (id, name, screen_name, description, verified, geo_enabled,
                        statuses_count, followers_count, friends_count, time_zone,
                        lang, location)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                    [x.decode() if type(x) == bytes else x for x in row]
                )
                self.n_imported += 1
                sys.stdout.write("\r%d" % self.n_imported)

        with open("places%d.msgpack" % split_n, "rb") as f:
            unpacker = msgpack.Unpacker(f)
            for row in unpacker:
                self.cursor.execute(
                    """INSERT INTO places
                        (id, country, full_name, place_type, bounding_box)
                    VALUES
                        (%s, %s, %s, %s, ST_SetSRID(%s::geometry, 4326))
                    ON CONFLICT DO NOTHING""",
                    [x.decode() if type(x) == bytes else x for x in row]
                )
                self.n_imported += 1
                sys.stdout.write("\r%d" % self.n_imported)

        with open("tweets%d.msgpack" % split_n, "rb") as f:
            unpacker = msgpack.Unpacker(f)
            for row in unpacker:
                self.cursor.execute(
                    """INSERT INTO tweets
                        (id, user_id, place_id, text, created_at, hashtags, urls,
                        media, lang, mentioned_user_ids, quoted_status_id,
                        in_reply_to_status_id, in_reply_to_user_id, coordinates)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        ST_SetSRID(%s::geometry, 4326)
                        )
                    ON CONFLICT DO NOTHING""",
                    [x.decode() if type(x) == bytes else x for x in row]
                )
                self.n_imported += 1
                sys.stdout.write("\r%d" % self.n_imported)

def import_split(split_n):
    importer = Importer()
    importer.import_tweets(split_n)
    importer.connection.commit()

if (__name__ == "__main__"):
    pool = multiprocessing.Pool(SPLITS)
    pool.map(import_split, list(range(SPLITS)))
    pool.close()
    pool.join()
