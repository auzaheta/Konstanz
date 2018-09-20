#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2018
# Inspired in Pascal Jürgens and Andreas Jungherr code


"""
Set functions to retrieve information about current president interview in radio
"""
import os
import sys
sys.path.insert(0, 'src')

import rest
import streaming
#import database
import logging
import json
import datetime
from pytz import timezone
import peewee
from progress.bar import Bar
import pymongo as mo

MST = timezone("MST")

#
# Helper Functions
#


def print_tweet(tweet):
    """
    Print a tweet as one line:
    user: tweet
    """
    logging.warning(
        u"{0}: {1}, {2}".format(tweet["user"]["screen_name"], tweet["text"], tweet["lang"]))


def print_notice(notice):
    """
    This just prints the raw response, such as:
    {u'track': 1, u'timestamp_ms': u'1446089368786'}}
    """
    logging.error(u"{0}".format(notice))


#
# Setup
#
def import_json(fi):
    """
    Load json data from a file into the database.
    """
    logging.warning("Loading tweets from json file {0}".format(fi))
    for line in open(fi, "rb"):
        data = json.loads(line.decode('utf-8'))
        database.create_tweet_from_dict(data)


def save_user_archive_to_file():
    """
    Fetch all available tweets for one user and save them to a text file, one tweet per line.
    (This is approximately the format that GNIP uses)
    """
    filepath = os.path.join('output', 'santos.json')
    with open(filepath, "w") as f:
        archive_generator = rest.fetch_user_archive("JuanManSantos")
        for page in archive_generator:
            for tweet in page:
                f.write(json.dumps(tweet) + "\n")
    logging.warning(u"Wrote tweets from @JuanManSantos to file santos.json")


def save_track_keywords(keywords, filepath, keyNumber, db, collection):
    """
    Track two keywords with a tracking stream and save machting tweets.
    To stop the stream, press ctrl-c or kill the python process.
    """
    # Set up save in mongo
    client = mo.MongoClient()
    cl = client.get_database(db).get_collection(collection)
    def save_tweet(tweet):
        cl.insert_one(tweet)
    try:
        stream = streaming.stream(
            on_tweet=save_tweet, on_notification=print_notice, track=keywords,
            filepath=filepath, keyNumber=keyNumber
        )
    except (KeyboardInterrupt, SystemExit):
        client.close()
        logging.error("User stopped program, exiting!")


def save_follow_users(users, filepath, keyNumber, db, collection):
    """
    Track two keywords with a tracking stream and save machting tweets.
    To stop the stream, press ctrl-c or kill the python process.
    """
    # Set up save in mongo
    client = mo.MongoClient()
    cl = client.get_database(db).get_collection(collection)
    def save_tweet(tweet):
        cl.insert_one(tweet)
    try:
        stream = streaming.stream(
            on_tweet=save_tweet, on_notification=print_notice, follow=users,
            filepath=filepath, keyNumber=keyNumber
        )
    except (KeyboardInterrupt, SystemExit):
        client.close()
        logging.error("User stopped program, exiting!")

