#!/usr/bin/env python
# -*- coding: utf-8 -*-
# # documentation ----
# # File: 02Collect.py
# # Python Versions: 3.5.2 x86_64
# #
# # Author(s): AU
# #
# #
# # Description: Collect information of tweeters accounts and hashtags
# #              include mongo in system path
# #
# # Inputs: txt files of hashtags an users ids
# #
# # Outputs: txt files with id of candidates
# #
# # File history:
# #   20180220: creation

# # # # # libraries
import os
import sys
sys.path.insert(0, os.path.join('src', 'functions'))

import pymongo as mo
import json
import itertools as it
import pandas as pd
import setFunctions as sf

# open mongo in root folder using a cmd
# mongod --dbpath .\output\mongo --logpath .\output\mdb2018MMDD.log

# # # # # general settings
# set database conection and collections
# (1) connection
#client = mo.MongoClient()
#isinstance(client, mo.mongo_client.MongoClient)
# (2) get/create database and collection
#db = client.get_database('tweet')
    # or db = client.tweet

infoFilepath = os.path.join('input', 'info.xlsx')
keywordTrack = pd.read_excel(infoFilepath, sheetname='keywords')

infoFilepath = os.path.join('input', 'users02.txt')
userFollow = pd.read_table(infoFilepath)


keyFilepath = os.path.join('input', 'keysApp04.json')

# # # # # start tweet collection
# sf.save_track_keywords collect a sample from the Twitter API from the list of words given
# the list can contain hashtags, it keep the complete JSON Tweet information in db.col given by last two parameters
# set information to stream with one token
# kwt1 = keywordTrack.query('grupo == 1.0').filter(items=['key'])
# left hashtags 
#kwt1 = keywordTrack.query('grupo == 1.0').ix[:, 'key'].values.tolist()
#sf.save_track_keywords(kwt1, keyFilepath, 0, 'tweet', 'right')

# right hashtags
#kwt2 = keywordTrack.query('grupo == 2.0').ix[:, 'key'].values.tolist()
#sf.save_track_keywords(kwt2, keyFilepath, 3, 'tweet', 'left')

# keywords
kwt = keywordTrack.query('(grupo == 1.0) | (grupo == 2.0)').ix[:, 'key'].values.tolist()
sf.save_track_keywords(kwt, keyFilepath, 0, 'tweet', 'keyw')

# users
# sf.save_follow_users follow a list of users IDS
ids = userFollow.ix[:, 'id'].values.tolist()
sf.save_follow_users(ids, keyFilepath, 3, 'tweet', 'user')



# # # # # try collection
# test if given app autorization works, the keyFilepath follows the squeme present in keysApp04.json
# multiple app autorization could be specific in JSON format, twitter_auth.test try the one specific for keyNumber
twitter_auth.test(filepath=keyFilepath, keyNumber=1)