from distutils.log import error
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
import json
import pymongo
import numpy as np
import requests
import stanza
# stanza.download('id', processors='tokenize, mwt, pos, lemma, depparse')

nlp = stanza.Pipeline('id', processors='tokenize, mwt, pos, lemma, depparse')

def is_negated(message):
    doc = nlp(message)
    # find advmod
    advmods = []
    for sent in doc.sentences:
        for word in sent.words:
            if word.deprel == 'advmod':
                advmods.append(word.text)
    if len(advmods) > 0:
        return 1
    return 0

# database
client = pymongo.MongoClient("mongodb+srv://root:t3lk0mtr5@roc5cluster.72ope.mongodb.net/?retryWrites=true&w=majority")
db = client.get_database('sentiment_db')
tweets = db.tweets
tweet_with_place = db.tweet_with_place
tweet_telkom_area = db.tweet_telkom_area
tweet_endorsed = db.tweet_endorsed
tweet_negated = db.tweet_negated

class StdOutListener(StreamListener):
    def __init__(self, tracks):
        self.tracks=tracks
    
    def calculateCentroid(self, place):
        """ Calculates the centroid from a bounding box."""
        # Obtain the coordinates from the bounding box.
        coordinates = place[0]
        
        longs = np.unique( [x[0] for x in coordinates] )
        lats  = np.unique( [x[1] for x in coordinates] )
        
        if len(longs) == 1 and len(lats) == 1:
            # return a single coordinate
            return (longs[0], lats[0])
        elif len(longs) == 2 and len(lats) == 2:
            # If we have two longs and lats, we have a box.
            central_long = np.sum(longs) / 2
            central_lat  = np.sum(lats) / 2
        else:
            raise ValueError("Non-rectangular polygon not supported.")

        return (central_long, central_lat)

    def save_to_mongo(self, coll, data):
        try:
            # insert into new collection
            coll.insert_one(data)
            # self.output.write(tweet)
        except pymongo.errors.DuplicateKeyError:
            # skip document because it already exists in new collection
            print("something wrong when saving to db")

    def on_data(self, data):
        tweet = json.loads(data)

        # all
        with open('tweet.json', 'a') as outfile: 
            json.dump(tweet, outfile)
            outfile.write('\n')
        if "extended_tweet" in tweet:
            if is_negated(tweet['extended_tweet']["full_text"]):
                # with open('tweet_negated.json', 'a') as outfile: # negated
                #     json.dump(tweet, outfile)
                #     outfile.write('\n')
                self.save_to_mongo(tweets, tweet)
        else:
            if is_negated(tweet['text']):
                # with open('tweet_negated.json', 'a') as outfile: # negated
                #     json.dump(tweet, outfile)
                #     outfile.write('\n')
                self.save_to_mongo(tweet_negated, tweet)
        
        # endorsed tweet
        if "aktivitastanpabatas" in tweet["text"].lower():
            # with open('tweet_endorsed.json', 'a') as outfile: # endorsed tweet
            #     json.dump(tweet, outfile)
            #     outfile.write('\n')
            self.save_to_mongo(tweet_endorsed, tweet)
        elif "extended_tweet" in tweet and "aktivitastanpabatas" in tweet["extended_tweet"]["full_text"].lower():
            # with open('tweet_endorsed.json', 'a') as outfile: # endorsed tweet
            #     json.dump(tweet, outfile)
            #     outfile.write('\n')
            self.save_to_mongo(tweet_endorsed, tweet)

        # with place data
        if tweet['place'] is not None: 
            tweet['centroid'] = self.calculateCentroid(tweet['place']['bounding_box']['coordinates'])
            URL = f"https://api-emas.telkom.co.id:9093/api/area/findByLocation?"
            PARAMS = {'lon': tweet['centroid'][0], 'lat': tweet['centroid'][1]}
            r = requests.get(url = URL, params = PARAMS)
            telkom_area = r.json()

            print(tweet['user']['screen_name']+": "+tweet['text']+", "+tweet['place']['name']+", "+str(tweet['centroid'])+", "+str(telkom_area))
            if "regional" in telkom_area is not None:
                tweet['telkom_area'] = telkom_area
                # with open('tweet_telkom_area.json', 'a') as outfile: # with place
                #     json.dump(tweet, outfile)
                #     outfile.write('\n')
                self.save_to_mongo(tweet_telkom_area, tweet)
            else:
                tweet['telkom_area'] = telkom_area
                # with open('tweet_with_place.json', 'a') as outfile: # with place & telkom area
                #     json.dump(tweet, outfile)
                #     outfile.write('\n')
                self.save_to_mongo(tweet_with_place, tweet)
        return True

    def on_error(self, status):
        print(status)

if __name__ == "__main__":
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    tracks = ['indihome', 'indihum', 'indi home', 'indihom', 
    'telkomsel', 'biznet', 'firstmedia', 'myrepublic']

    while True:
        try:
            listener = StdOutListener(tracks=tracks)
            print("stream start...")
            stream = Stream(auth, listener)
            stream.filter(track=tracks)
        except: 
            print("something wrong")
            continue
    
    # listener = StdOutListener(tracks=tracks)
    # print("stream start...")
    # stream = Stream(auth, listener)
    # stream.filter(track=tracks)
    