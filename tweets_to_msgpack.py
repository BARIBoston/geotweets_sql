#!/usr/bin/env python3

import msgpack
import shapely.geometry

NLONG = "$numberLong"
DEFAULT_COMMIT_INTERVAL = 5000

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

    def __init__(self, commit_interval = DEFAULT_COMMIT_INTERVAL):
        """ Initialize Importer object

        Args:
            commit_interval: The number of tweets that a commit should be
                automatically made after
        """

        self.n_imported = 0
        self.commit_interval = commit_interval
        self.files = {
            key: open("%s.msgpack" % key, "wb")
            for key in ["users", "places", "tweets"]
        }

    def close(self):
        for (key, file_) in self.files.items():
            file_.close()

    def import_tweet(self, data):
        """ Import a tweet """

        user = data["user"]
        place = data["place"]
        user_id  = convert_nlong(user["id"])

        self.files["users"].write(msgpack.packb(
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
        ))

        if (place is not None):
            place_id = place["id"]
            self.files["places"].write(msgpack.packb(
                (
                    place["id"],
                    place["country"],
                    place["full_name"],
                    place["place_type"],
                    shapely.geometry.shape(place["bounding_box"]).wkb_hex
                )
            ))
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

        self.files["tweets"].write(msgpack.packb(
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
        ))

        self.n_imported += 1

if (__name__ == "__main__"):
    import json
    import sys

    importer = Importer()
    error_file = "%s_errors.json" % sys.argv[1]

    with open(sys.argv[1], "r") as f, open(error_file, "w") as f_errors:
        for line in f:
            try:
                importer.import_tweet(json.loads(line))
            except Exception as error:
                print(error)
                f_errors.write(line)
            sys.stdout.write("\rimported %d tweets" % importer.n_imported)
            sys.stdout.flush()

    print("")
    importer.close()
