#!/usr/bin/env python3

import psycopg2
import shapely.geometry

NLONG = "$numberLong"

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
        self.connection = psycopg2.connect("dbname = geotweets")
        self.cursor = self.connection.cursor()

    def import_tweet(self, data):
        user = data["user"]
        place = data["place"]
        user_id  = convert_nlong(user["id"])

        self.cursor.execute(
            """INSERT INTO users
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
                """INSERT INTO places
                    (id, country, full_name, place_type)
                VALUES
                    (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING""",
                (
                    place["id"],
                    place["country"],
                    place["full_name"],
                    place["place_type"]
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
            """INSERT INTO tweets
                (id, user_id, place_id, text, created_at, hashtags, urls,
                lang, mentioned_user_ids, in_reply_to_status_id,
                in_reply_to_user_id, coordinates)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
                data["lang"],
                mentioned_user_ids,
                convert_nlong(getitem_or_none(data, "in_reply_to_status_id")),
                convert_nlong(getitem_or_none(data, "in_reply_to_user_id")),
                shapely.geometry.shape(data["coordinates"]).wkb_hex
            )
        )

if (__name__ == "__main__"):
    import json
    import sys

    importer = Importer()

    with open(sys.argv[1], "r") as f:
        i = 0
        for line in f:
            importer.import_tweet(json.loads(line))
            i += 1

            if (i % 5000):
                importer.connection.commit()

            sys.stdout.write("\rimported %d tweets" % i)
            sys.stdout.flush()

    print("")
    importer.connection.commit()
