#!/usr/bin/env python3

import psycopg2
import shapely.geometry

NLONG = "$numberLong"
DEFAULT_COMMIT_INTERVAL = 10000
DEFAULT_DB = "bari"

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

    def __init__(self, db = DEFAULT_DB, commit_interval = DEFAULT_COMMIT_INTERVAL):
        """ Initialize Importer object

        Args:
            commit_interval: The number of tweets that a commit should be
                automatically made after
        """

        self.connection = psycopg2.connect("dbname = %s" % db)
        self.cursor = self.connection.cursor()

    def import_tweet(self, data):
        """ Import a tweet """

        user = data["user"]
        place = data["place"]
        user_id  = convert_nlong(user["id"])

        self.cursor.execute(
            """INSERT INTO geotweets_users
                (id, name, screen_name, description, verified, geo_enabled,
                statuses_count, followers_count, friends_count, time_zone,
                lang, location)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING""",
            (
                user_id,
                user["name"],
                user["screen_name"],
                user["description"],
                user["verified"],
                user["geo_enabled"],
                user["statuses_count"],
                user["followers_count"],
                user["friends_count"],
                user["time_zone"],
                user["lang"],
                user["location"]
            )
        )

        if (place is not None):
            place_id = place["id"]
            self.cursor.execute(
                """INSERT INTO geotweets_places
                    (id, country, full_name, place_type, bounding_box)
                VALUES
                    (%s, %s, %s, %s, ST_SetSRID(%s::geometry, 4326))
                ON CONFLICT DO NOTHING""",
                (
                    place["id"],
                    place["country"],
                    place["full_name"],
                    place["place_type"],
                    shapely.geometry.shape(place["bounding_box"]).wkb_hex
                )
            )
        else:
            place_id = None

        entities = data["entities"]
        if ("urls" in entities):
            urls = [
                url["expanded_url"]
                for url in entities["urls"]
            ]
        else:
            urls = None

        if ("media" in entities):
            media = [
                media["media_url"]
                for media in entities["media"]
            ]
        else:
            media = None

        if ("hashtags" in entities):
            hashtags = [
                hashtag["text"]
                for hashtag in entities["hashtags"]
            ]
        else:
            hashtags = None

        if ("user_mentions" in entities):
            mentioned_user_ids = [
                int(convert_nlong(user["id"]))
                for user in entities["user_mentions"]
            ]
        else:
            mentioned_user_ids = None

        self.cursor.execute(
            """INSERT INTO geotweets
                (id, user_id, place_id, text, created_at, hashtags, urls,
                media, lang, mentioned_user_ids, quoted_status_id,
                in_reply_to_status_id, in_reply_to_user_id, coordinates)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                ST_SetSRID(%s::geometry, 4326)
                )
            ON CONFLICT DO NOTHING""",
            (
                convert_nlong(data["id"]),
                user_id,
                place_id,
                data["text"],
                data["created_at"],
                hashtags,
                urls,
                media,
                data["lang"],
                mentioned_user_ids,
                convert_nlong(getitem_or_none(data, "quoted_status_id")),
                convert_nlong(getitem_or_none(data, "in_reply_to_status_id")),
                convert_nlong(getitem_or_none(data, "in_reply_to_user_id")),
                shapely.geometry.shape(data["coordinates"]).wkb_hex
            )
        )

if (__name__ == "__main__"):
    import argparse
    import datetime
    import gzip
    import json
    import tqdm

    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs = "+")
    args = parser.parse_args()

    importer = Importer()

    error_file = "geotweets_sql_errors_%s.json.gz" % datetime.datetime.now().isoformat()
    n_errors = 0

    for input_file in tqdm.tqdm(args.inputs, desc = "files imported", position = 0):
        with gzip.open(input_file, "r") as f, gzip.open(error_file, "w") as f_errors:
            for line in tqdm.tqdm(f, position = 1):
                try:
                    importer.import_tweet(json.loads(line))
                except Exception as error:
                    n_errors += 1
                    f_errors.write(line)
        importer.connection.commit()

    if (n_errors > 0):
        print("%d errors (check %s)" % (n_errors, error_file))
