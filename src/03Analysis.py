#!/usr/bin/env python
# -*- coding: utf-8 -*-
# # documentation ----
# # File: 03Analysis.py
# # Python Versions: 3.5.2 x86_64
# #
# # Author(s): AU
# #
# #
# # Description: summary statistics from the Tweeter data collection
# #
# #
# # Inputs: mongo db
# #
# # Outputs: xlsx files with summary statistics
# #
# # File history:
# #   20180424: creation

# # # # # libraries
import os
import pymongo as mo
import pandas as pd
import sys
import spacy
from bson.code import Code
from bson import json_util
import datetime
import itertools
import re

# open mongo in root folder
# mongod --dbpath .\output\mongo --logpath .\output\mdb20180918.log

# # # # # general settings
# set database connection and collections
# (1) connection

client = mo.MongoClient()
db = client.tweet  # connect to tweet db

pp = client.preprocess

# # list of names or accounts to identify candidates
infoFilepath = os.path.join('input', 'info.xlsx')
keywordTrack = pd.read_excel(infoFilepath, sheet_name='keywords')
candidate = pd.read_excel(infoFilepath, sheet_name='candidato')

# # subset of candidates to compute summary statistics
candAna = candidate[(candidate.Analizar == 1.0) & (candidate.candidato != 'VM')]

keywordTrack.query('candidato == "GP"')
keywordTrack.groupby('candidato').count()
keywordTrack[keywordTrack.grupo != 3].groupby('candidato').count()

# # data frame with the set of hashtags/keywords/screen_name used to identified mentions of candidates
kwId = pd.merge(keywordTrack[keywordTrack.grupo != 3], candidate, how='inner')
kwId.groupby('candidato').count()
kwId.groupby('candidato').sum()
kwId.groupby('candidato')['grupo'].agg('sum')
kwId = kwId[kwId.Analizar == 1.0]  # # set of candidates to look for

kwId.to_excel('output/kwId.xlsx')

# # set of keyWords/Hashtags initially consider neutral
keyNeutral = keywordTrack.query('candidato == "Neutral"')  # # neutral keywords

# # nlp processor
nlp = spacy.load('es_core_news_sm', disable=['parser'])

# # pos to keep for further analysis
posKeep = {u'ADJ', u'NOUN', u'ADV'}

# # fields to keep from users
user_kp = {u'id', u'description', u'verified', u'followers_count', u'statuses_count', u'friends_count', u'verified',
           u'screen_name', u'following', u'listed_count', u'created_at', u'listed_count', u'name', u'favourites_count'}


# # function to identify if the candidate is mention in the tweet


def identify(doc, keyNeutral, kwId, posKeep):
    """
    Create a document when one candidate is mention
    :param doc: Tweet
    :param keyNeutral: neutral hashtags
    :param kwId: candidates and different ways to identified them
    :param posKeep:  pos tags to keep with their tokens
    :return: dict with information to keep
    """
    if not doc['retweeted']:
        um = {usermentions['screen_name'].lower() for usermentions in doc['entities']['user_mentions']}
        um = um.union({hashtags['text'].lower() for hashtags in doc['entities']['hashtags']})

        nlp_doc = nlp(doc['text'])

		# name entities
        entities = [ent.text.lower() for ent in nlp_doc.ents]
		# tokens that are not stop words and have only alpha  characters
        tokens = [token.text.lower() for token in nlp_doc if not token.is_stop and token.is_alpha]

		# check if candidates is mention by: name entities | in lemmatized token | direct mention
        row_mask = kwId.isin(entities).any(1) | kwId.isin(tokens).any(1) | kwId.isin(um).any(1)
        candidatos = kwId['candidato'][row_mask].drop_duplicates().tolist()

        if candidatos:
            token_keep = list({(token.lemma_.lower(), token.pos_) for token in nlp_doc
                               if token.pos_ in posKeep and token.is_alpha and len(token.lemma_) > 3})
            doc_keep = {u'id': doc['id'], u'text': doc['text'], u'created_at': doc['created_at'],
                        u'favorite_count': doc['favorite_count'], u'retweet_count': doc['retweet_count'],
                        u'reply_count': doc['reply_count'], u'user': doc['user']['id'],
                        u'candidatos': candidatos, u'mentions': list(um),
                        u'tokens': [list(pair) for pair in token_keep]}

            if doc['coordinates']:
                doc_keep['geo'] = True
                doc_keep['coordinates'] = doc['coordinates']

            row_mask = keyNeutral.isin(um).any(1) | keyNeutral.isin([token for token, _ in doc_keep['tokens']]).any(1)
            neutral = keyNeutral['keyLower'][row_mask].drop_duplicates().tolist()
            if neutral:
                doc_keep['neutral'] = True
                doc_keep['word_neutral'] = neutral

            return doc_keep

    return False


# # update over all the tweet, which mention candidates
i = 0

# db.keyw.find().skip(6433000).limit(4170000)
# db.keyw.find().skip(10603000)
# for d in db.keyw.find().skip(6433000).limit(4170000):
for d in db.keyw.find().skip(10802828):
    keep = identify(d, keyNeutral, kwId, posKeep)
    i += 1
    if keep:
        pp.filter.update_one({'_id': '{:x}'.format(keep['id'])}, {'$set': keep}, upsert=True)
        user = {key: value for key, value in d['user'].items() if key in user_kp}
        pp.user.update_one({'_id': '{:x}'.format(user['id'])}, {'$set': user}, upsert=True)

    if i % int(1E5) == 0:
        print('Processed ' + str(i / 1E6))


for d in db.right.find():
    keep = identify(d, keyNeutral, kwId, posKeep)
    i += 1
    if keep:
        pp.filter.update_one({'_id': '{:x}'.format(keep['id'])}, {'$set': keep}, upsert=True)
        user = {key: value for key, value in d['user'].items() if key in user_kp}
        pp.user.update_one({'_id': '{:x}'.format(user['id'])}, {'$set': user}, upsert=True)

    if i % int(1E5) == 0:
        print('Processed ' + str(i))


# correct not direct mentions of candidates screen_name by error in excel format
coll = db.collection_names()
coll = coll[:2] + coll[3:]
correct = ['IvanDuque', 'DeLaCalleHum']
for col in coll:
    for d in db[col].find({'entities.user_mentions.screen_name': {'$in': correct}}):
        keep = identify(d, keyNeutral, kwId, posKeep)
        i += 1
        if keep:
            pp.filter.update_one({'_id': '{:x}'.format(keep['id'])}, {'$set': keep}, upsert=True)
            user = {key: value for key, value in d['user'].items() if key in user_kp}
            pp.user.update_one({'_id': '{:x}'.format(user['id'])}, {'$set': user}, upsert=True)

        if i % int(1E5) == 0:
            print "Processed", str(i), datetime.datetime.now()

# # change date
#created_at = 'Mon Jun 8 10:51:32 +0000 2009' # Get this string from the Twitter API
#dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S +0000 %Y')

# # list of neutral hashtags/keywords
neutrales = set(['debate2018', 'lagranencuesta', 'eleccionescolombia2018', 'presidencial', 'colombiadecide', 'congreso',
             'eleccionescolombia', 'revistasemana', 'debatevicepresidencialenlaw', 'semanaelecciones2018',
             'elecciones2018', 'ojoasuvoto', 'eleccionesenlafm', 'ambienteyelecciones2018', 'debate', 'moecolombia',
             'transparencia', 'retoelectoralrcn', 'pulsoporelpoder', 'eleccionesseguras', 'elecciones', 'rcncontuelección'])
# # regular expressions to identify topics in text Tweets
edu = re.compile(".*educaci.{0,2}$")
eco = re.compile("econo[^typ]+$")
amb = re.compile(".*ambien[^c]+$")
sal = re.compile("salud$")

i = 0
#for obj in pp.filter.find().skip(1572205):
#for obj in pp.filter.find({'amb': {'$exists': False}, 'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'ambien[^c]+$'}}}}):
for obj in pp.filter.find({'edu': {'$exists': False}, 'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'educaci.{0,2}$'}}}}):
    i += 1
    time = datetime.datetime.strptime(obj['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    changes = {"date" : time}
    if re.match('^RT', obj['text']) :
        changes['rt'] = True

    if any(edu.match(tok[0]) for tok in obj['tokens']):
        changes['edu'] = True

    if any(eco.match(tok[0]) for tok in obj['tokens']):
        changes['eco'] = True

    if any(amb.match(tok[0]) for tok in obj['tokens']):
        changes['amb'] = True

    if any(sal.match(tok[0]) for tok in obj['tokens']):
        changes['sal'] = True

    if obj.has_key('word_neutral') and neutrales.intersection(set(obj['word_neutral'])):
        changes['isAna'] = True

    pp.filter.update_one({'_id':obj['_id']},{'$set': changes})

    if i % int(1E5) == 0:
        print "Processed", str(i / 1E6), datetime.datetime.now()


# identify if it is direct retweet from candidates oficial accounts
rtDir = re.compile("^RT @(.+): .+")
for obj in pp.filter.find({'isAna': True, 'rt': True}):
    i += 1

    userDir = rtDir.match(obj['text'])
    if userDir:
        row_mask = candAna.isin([userDir.group(1).lower()]).any(1)
        candidato = candAna['candidato'][row_mask].tolist()
        if candidato:
            changes = {"rtDir": candidato[0]}
            pp.filter.update_one({'_id':obj['_id']},{'$set': changes})

    if i % int(1E4) == 0:
        print "Processed", str(i / 1E4), datetime.datetime.now()


pp.filter.find({'isDir': {'$exists': True}}).count()

# identify in not retweet if it is a direct mention of candidates oficial account
i = 0
for obj in pp.filter.find({'isAna': True, 'rt': {'$exists': False}}):
    i += 1

    row_mask = candAna.isin(obj['mentions']).any(1)
    candidato = candAna['candidato'][row_mask].tolist()
    if candidato:
        changes = {"menDir": candidato}
        pp.filter.update_one({'_id':obj['_id']},{'$set': changes})

    if i % int(1E4) == 0:
        print "Processed", str(i / 1E4), datetime.datetime.now()



# # # summary statistics
# # count tweets by candidate
res = pp.filter.aggregate([
    {'$unwind': '$candidatos'},
    {"$sortByCount": '$candidatos'}
    ])
candCount = pd.DataFrame(list(res))
candIds = candidate.query('Analizar == 1.0')['candidato'].tolist()
candPairs = itertools.combinations(iter(candIds), 2)

# # count tweets by neutral words
res = pp.filter.aggregate([
    {'$unwind': '$word_neutral'},
    {"$sortByCount": '$word_neutral'}
    ])
neutCount = pd.DataFrame(list(res))

# # count tweets by candidate and neutral words
res = pp.filter.aggregate([
    {'$unwind': '$word_neutral'},
    {'$unwind': '$candidatos'},
    {"$group": {
        "_id": {
            'candidatos': '$candidatos',
            'word_neutral': '$word_neutral'
            },
        "count": {"$sum": 1}
        }
    }
    ])  # # '$_id.state'

#,
#   {'$match': {'count': {'$gt': 1}}},
#   {'$sort': {'count': -1}}

candNeutral = pd.DataFrame(list(res))
candNeutral = pd.concat([candNeutral.drop(['_id'], axis=1), candNeutral['_id'].apply(pd.Series)], axis=1)
mind = pd.MultiIndex.from_product([candCount._id.tolist(), neutCount._id.tolist()], names = ['candidatos', 'word_neutral'])
candNeutral = candNeutral.set_index(['candidatos', 'word_neutral']).reindex(mind, fill_value=0).reset_index()

    # # count tweets by pair of candidates given neutral words using MapReduce
mapNeuCand = Code("""
function(){
    if (this.word_neutral) {
        for(var w=0; w < this.word_neutral.length; w++) {
            var word = this.word_neutral[w];
            for(var i=0; i < this.candidatos.length - 1; i++) {
                var first = this.candidatos[i];
                if (first != "VM") {
                    for(var j=i+1; j < this.candidatos.length; j++) {
                        var second = this.candidatos[j];
                        if (second != "VM") {
                            emit({wd: word, cl: first, cd: second}, 1);
                        }
                    }
                }
            }
        }
    }
}
    """)

reducer = Code("""
    function(key, values){
        var total = 0;
        for (var i = 0; i < values.length; i++){
            total += values[i];
        }
        return total;
    }
""")

result = pp.filter.map_reduce(mapNeuCand, reducer, "neuPCand")

neuPCand = pd.DataFrame(list(result.find()))
neuPCand = pd.concat([neuPCand.drop(['_id'], axis=1), neuPCand['_id'].apply(pd.Series)], axis=1)

candPairs = neuPCand[['cl', 'cd']].drop_duplicates().apply(lambda x: x.tolist(), axis=1)
mind = itertools.product(*[candPairs, neutCount._id.tolist()])
mind = pd.DataFrame([[l[0], l[1], j] for l, j in mind ], columns=['cl', 'cd', 'wd'])

neuPCand = pd.merge(neuPCand, mind, how='outer').fillna(0)

# # average jaccard similarity computation
jaccard = pd.merge(neuPCand, candNeutral, how='inner', left_on=['cl', 'wd'], right_on=['candidatos', 'word_neutral'],
                   suffixes=('','_l')).drop(['candidatos', 'word_neutral'], axis=1)
jaccard = pd.merge(jaccard, candNeutral, how='inner', left_on=['cd', 'wd'], right_on=['candidatos', 'word_neutral'],
                   suffixes=('','_d')).drop(['candidatos', 'word_neutral'], axis=1)
jaccard['jaccard'] = jaccard['value'] / (jaccard['count'] + jaccard['count_d'] - jaccard['value'])

wdJaccard = jaccard.groupby('wd').jaccard.agg(['mean', 'std'])
wdJaccard['cv'] = wdJaccard['std'] / (wdJaccard['mean'] + .000000001)
wdJaccard.sort_values('cv').query("mean > 0")


# # # multiple querys to see intermediate results
# # count by tokens
res = pp.filter.aggregate([
    {'$unwind': '$tokens'},
    {"$sortByCount": '$tokens'},
    {'$match': {'count': {'$gt': 1000}}}
    ])

tokensCount = pd.DataFrame(list(res))
tokensCount = pd.concat([tokensCount.drop(['_id'], axis=1), tokensCount['_id'].apply(pd.Series)], axis=1)
tokensCount.columns = ['count', 'word', 'pos']
tokensCount.query("word=='cryptocurrency'")
tokensCount[tokensCount.word.str.match('criptomoneda')]
tokensCount[tokensCount.word.str.match('educaci.{0,2}$')]
tokensCount[tokensCount.word.str.match('econo[^typ]+$')]
tokensCount[tokensCount.word.str.match('ambien[^c]+$')]
tokensCount[tokensCount.word.str.match('salud$')]
#tokensCount[tokensCount.word.str.match('trabaj')]
#tokensCount[tokensCount.word.str.match('plebiscito')]
#tokensCount[tokensCount.word.str.match('mujer')]

pp.filter.find_one({'tokens':{'$elemMatch':{'$elemMatch': {'$in':[u'trabajosíhay']}}}})
list(pp.filter.find({'tokens':{'$elemMatch':{'$elemMatch': {'$in':[u'salud']}}}}).skip(1000).limit(1))
pp.user.find_one({'_id': '{:x}'.format(2768064549L)})
pp.filter.find({'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'educaci.{0,2}$'}}}}).count()
ver = pp.filter.find({'amb': {'$exists': False}, 'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'ambien[^c]+$'}}}})
ver = pp.filter.find({'eco': {'$exists': False}, 'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'econo[^typ]+$'}}}})
ver = pp.filter.find({'edu': {'$exists': False}, 'tokens':{'$elemMatch':{'$elemMatch': {'$regex':u'educaci.{0,2}$'}}}})
ver2 = next(ver)
pp.filter.find({'edu': True}).count()
pp.filter.find({'amb': {'$exists': False}}).count()
pp.filter.find_one({'text': {'$regex': 'petro\W.+Trump|Trump.+petro'}})

ver = pp.filter.aggregate([
    {'$sample': {'size': 200}},
    {'$match': {'rt': True, 'isAna': True}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 10000}},
    {'$match': {'text': {'$regex': 'petro\W.+Trump|Trump.+petro\W'}}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 1000}},
    {'$match': {'text': {'$regex': '(?![Gg]ustavo)?.+[ dD][AaeE]l [Pp]etro.[^h].+(?![Gg]ustavo)?'}}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 10000}},
    {'$match': {'tokens':{'$elemMatch':{'$elemMatch': {'$regex': 'xxx'}}}}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 10000}},
    {'$match': {'mentions':{'$in':['yosigoalpetro', 'petrodivisa', 'elpetrolostienelocos', '2millonesyvamospormas',
                                   'petrosayfuckyoutrump', 'petrocaribe']}}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 10000}},
    {'$match': {'mentions':{'$in':['nicolasmaduro']}}},
    {'$project':{'text': 1}}
])

ver = pp.filter.aggregate([
    {'$sample': {'size': 10000}},
    {'$match': {'mentions':{'$elemMatch':{'$regex': 'cripto|crypto[^m]'}}}},
    {'$project':{'text': 1}}
])
for doc in ver:
    print(doc)

# # # delete tweets related with Petro Cryptocurrency
delTrump = pp.filter.delete_many({'text': {'$regex': 'petro\W.+Trump|Trump.+petro\W'}})
delTrump.deleted_count
delCrypto = pp.filter.delete_many({'tokens':{'$elemMatch':{'$elemMatch': {'$regex': 'crypto'}}}})
delCrypto.deleted_count
delCripto = pp.filter.delete_many({'tokens':{'$elemMatch':{'$elemMatch': {'$regex': 'criptomoneda'}}}})
delCripto.deleted_count

delCriptoc = pp.filter.delete_many({'tokens':{'$elemMatch':{'$elemMatch': {'$regex': 'cripto'}}}})
delCriptoc.deleted_count

delCriptod = pp.filter.delete_many({'mentions':{'$elemMatch':{'$regex': 'cripto|crypto[^m]'}}})
delCriptod.deleted_count

delVene = pp.filter.delete_many({'mentions':{'$in':['elpetrofuturodetodos', 'elmundoconvenezuela', 'vicevenezuela',
                                   'todossomosvenezuela', 'mercaloficial', 'nicolasmaduro']}})
delVene.deleted_count
delVened = pp.filter.delete_many({'text': {'$regex': 'moneda digital|moneda virtual'}})
delVened.deleted_count

delVenee = pp.filter.delete_many({'text':
            {'$regex': '[Mm]oneda comercial|[Cc]ripto-moneda|[Cc]riptomoneda|[Pp]etro e[sn] la moneda|moneda [pP]etro|[mM]oneda [Vv]enezolana|malandromoneda|[Mm]oneda oficial|[Mm]oneda limpia|[Mm]oneda nacional|moneda(s)? [Dd]igital'}})
delVenee.deleted_count

delVenef = pp.filter.delete_many({'mentions':{'$in':['yosigoalpetro', 'petrodivisa', 'elpetrolostienelocos', '2millonesyvamospormas',
                                   'petrosayfuckyoutrump', 'petrocaribe']}})
delVenef.deleted_count


delVeneg = pp.filter.delete_many({'text': {'$regex': '(?![Gg]ustavo)?.+[ dD][AaeE]l [Pp]etro.+(?![Gg]ustavo)?'}})
delVeneg.deleted_count

# # compute ego network from retweets
res = pp.filter.aggregate([
    {'$match':{'isAna': True, 'rt': True, 'amb': True}},
    {'$unwind': '$candidatos'},
    {'$match': {'candidatos': {'$ne': 'VM'}}},
    {"$group": {
        "_id": {
            'to':'$candidatos',
            'from':'$user'
            },
        "count": {'$sum': 1}
        }
    }
    ])
netRT = pd.DataFrame(list(res))
netRT = pd.concat([netRT.drop(['_id'], axis=1), netRT['_id'].apply(pd.Series)], axis=1)
netRT2 = netRT.groupby(['to'])['count'].count().to_frame(name = 'count').reset_index()

candPairs = itertools.combinations(iter(netRT2['to'].tolist()), 2)
netRT3 = list()
for pair in candPairs:
    pairList = list()
    p1 = pair[0]
    p2 = pair[1]

    pairList.append(p1)
    pairList.append(p2)
    set1 = set(netRT[netRT['to'] == p1]['from'].tolist())
    set2 = set(netRT[netRT['to'] == p2]['from'].tolist())

    pairList.append(len(set1))
    pairList.append(len(set2))

    cap = len(set1.intersection(set2))
    cup = len(set1.union(set2))
    jaccard = float(cap) / float(cup)

    pairList.append(cap)
    pairList.append(cup)
    pairList.append(jaccard)
    netRT3.append(pairList)


jaccardRT = pd.DataFrame(netRT3, columns=['cl', 'cd', 'count_l', 'count_d', 'cap', 'cup', 'jaccard'])
jaccardRT.pivot(index='cd', columns='cl',values='jaccard')

# # compute for direct retweet
res = pp.filter.aggregate([
    {'$match':{'isAna': True, 'rtDir': {'$exists': True, '$ne': 'VM'}}},
    {"$group": {
        "_id": {
            'to':'$rtDir',
            'from':'$user'
            },
        "count": {'$sum': 1}
        }
    }
    ])
netRTD = pd.DataFrame(list(res))
netRTD = pd.concat([netRTD.drop(['_id'], axis=1), netRTD['_id'].apply(pd.Series)], axis=1)
netRTD2 = netRTD.groupby(['to'])['count'].count().to_frame(name = 'count').reset_index()

candPairs = itertools.combinations(iter(netRT2['to'].tolist()), 2)
netRT3 = list()
for pair in candPairs:
    pairList = list()
    p1 = pair[0]
    p2 = pair[1]

    pairList.append(p1)
    pairList.append(p2)
    set1 = set(netRTD[netRTD['to'] == p1]['from'].tolist())
    set2 = set(netRTD[netRTD['to'] == p2]['from'].tolist())

    pairList.append(len(set1))
    pairList.append(len(set2))

    cap = len(set1.intersection(set2))
    cup = len(set1.union(set2))
    jaccard = float(cap) / float(cup)

    pairList.append(cap)
    pairList.append(cup)
    pairList.append(jaccard)
    netRT3.append(pairList)


jaccardRTD = pd.DataFrame(netRT3, columns=['cl', 'cd', 'count_l', 'count_d', 'cap', 'cup', 'jaccard'])
jaccardRTD.pivot(index='cl', columns='cd',values='jaccard')



# # compute ego-networks from mentions
res = pp.filter.aggregate([
    {'$match':{'isAna': True, 'rt': {'$exists': False}, 'amb': True}},
    {'$unwind': '$candidatos'},
    {'$match': {'candidatos': {'$ne': 'VM'}}},
    {"$group": {
        "_id": {
            'to':'$candidatos',
            'from':'$user'
            },
        "count": {'$sum': 1}
        }
    }
    ])
netMen = pd.DataFrame(list(res))
netMen = pd.concat([netMen.drop(['_id'], axis=1), netMen['_id'].apply(pd.Series)], axis=1)
netMen2 = netMen.groupby(['to'])['count'].count().to_frame(name = 'count').reset_index()

candPairs = itertools.combinations(iter(netRT2['to'].tolist()), 2)
netRT3 = list()
for pair in candPairs:
    pairList = list()
    p1 = pair[0]
    p2 = pair[1]

    pairList.append(p1)
    pairList.append(p2)
    set1 = set(netMen[netMen['to'] == p1]['from'].tolist())
    set2 = set(netMen[netMen['to'] == p2]['from'].tolist())

    pairList.append(len(set1))
    pairList.append(len(set2))

    cap = len(set1.intersection(set2))
    cup = len(set1.union(set2))
    jaccard = float(cap) / float(cup)

    pairList.append(cap)
    pairList.append(cup)
    pairList.append(jaccard)
    netRT3.append(pairList)


jaccardMen = pd.DataFrame(netRT3, columns=['cl', 'cd', 'count_l', 'count_d', 'cap', 'cup', 'jaccard'])
jaccardMen.pivot(index='cd', columns='cl',values='jaccard')


# # compute ego-networks directed
res = pp.filter.aggregate([
    {'$match':{'isAna': True, 'rt': {'$exists': False}, 'menDir': {'$exists': True}}},
    {'$unwind': '$menDir'},
    {"$group": {
        "_id": {
            'to':'$menDir',
            'from':'$user'
            },
        "count": {'$sum': 1}
        }
    }
    ])
netMenD = pd.DataFrame(list(res))
netMenD = pd.concat([netMenD.drop(['_id'], axis=1), netMenD['_id'].apply(pd.Series)], axis=1)
netMenD2 = netMenD.groupby(['to'])['count'].count().to_frame(name = 'count').reset_index()

candPairs = itertools.combinations(iter(netRT2['to'].tolist()), 2)
netRT3 = list()
for pair in candPairs:
    pairList = list()
    p1 = pair[0]
    p2 = pair[1]

    pairList.append(p1)
    pairList.append(p2)
    set1 = set(netMenD[netMenD['to'] == p1]['from'].tolist())
    set2 = set(netMenD[netMenD['to'] == p2]['from'].tolist())

    pairList.append(len(set1))
    pairList.append(len(set2))

    cap = len(set1.intersection(set2))
    cup = len(set1.union(set2))
    jaccard = float(cap) / float(cup)

    pairList.append(cap)
    pairList.append(cup)
    pairList.append(jaccard)
    netRT3.append(pairList)


jaccardMenD = pd.DataFrame(netRT3, columns=['cl', 'cd', 'count_l', 'count_d', 'cap', 'cup', 'jaccard'])
jaccardMenD.pivot(index='cd', columns='cl',values='jaccard')

# # total tweets by weeks or days
res = pp.filter.aggregate([
    #{'$match':{'isAna': True, 'rt': {'$exists': False}, 'menDir': {'$exists': True}}},
    {'$unwind': '$candidatos'},
    {'$match': {'candidatos': {'$ne': 'VM'}}},
    {"$group": {
        "_id": {
            'candidato':'$candidatos',
            'week': {'$week': '$date'}
            },
        "total": {'$sum': 1},
        'edu': {'$sum': {'$cond': {'if': {'$gte': ['$edu', None]}, 'then': 1, 'else': 0}}},
        'eco': {'$sum': {'$cond': {'if': {'$gte': ['$eco', None]}, 'then': 1, 'else': 0}}},
        'amb': {'$sum': {'$cond': {'if': {'$gte': ['$amb', None]}, 'then': 1, 'else': 0}}},
        'rt': {'$sum': {'$cond': {'if': {'$gte': ['$rt', None]}, 'then': 1, 'else': 0}}},
        'neutral': {'$sum': {'$cond': {'if': {'$gte': ['$isAna', None]}, 'then': 1, 'else': 0}}}
        }
    }
    ])


res = pp.filter.aggregate([
    #{'$match':{'isAna': True, 'rt': {'$exists': False}, 'menDir': {'$exists': True}}},
    {'$unwind': '$candidatos'},
    {'$match': {'candidatos': {'$ne': 'VM'}}},
    {"$group": {
        "_id": {
            'candidato':'$candidatos',
            'year':{'$year':"$date"},
            'month':{'$month':"$date"},
            'day':{'$dayOfMonth':"$date"}
            },
        "total": {'$sum': 1},
        'edu': {'$sum': {'$cond': {'if': {'$gte': ['$edu', None]}, 'then': 1, 'else': 0}}},
        'eco': {'$sum': {'$cond': {'if': {'$gte': ['$eco', None]}, 'then': 1, 'else': 0}}},
        'amb': {'$sum': {'$cond': {'if': {'$gte': ['$amb', None]}, 'then': 1, 'else': 0}}},
        'rt': {'$sum': {'$cond': {'if': {'$gte': ['$rt', None]}, 'then': 1, 'else': 0}}},
        'neutral': {'$sum': {'$cond': {'if': {'$gte': ['$isAna', None]}, 'then': 1, 'else': 0}}}
        }
    }
    ])

countHist = pd.DataFrame(list(res))
countHist = pd.concat([countHist.drop(['_id'], axis=1), countHist['_id'].apply(pd.Series)], axis=1)
countHist.to_excel('output/counts2.xlsx')


# # query initial date and final date of data collection
res = pp.filter.aggregate([
    {"$group": {
        "_id": {'isAna': '$isAna'},
        "total": {'$sum': 1},
        'maxDate': {'$max': '$date'},
        'minDate': {'$min': '$date'}
        }
    }
    ])
fechas=pd.DataFrame(list(res))
print fechas

