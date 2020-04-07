from pymongo import MongoClient
from random import randint
import datetime
import json
from bson import json_util
from bson.json_util import dumps
from bson.json_util import dumps, CANONICAL_JSON_OPTIONS
from dotenv import load_dotenv
import os

pathout = "../data/json_news_tagged/"
pathoutbundle = "../data/json_news_tagged_bundle/"
load_dotenv(verbose=True)

def createjson(item):
  out = dict()
  out["id"] = item["id"]
  out["content"] = item["content"]
  out["date"] = item["date"].strftime("%m/%d/%Y, %H:%M:%S")
  out["tags"] = item["tags"]
  out["newspaper"] = item["newspaper"]
  return out

def save_news_into_jsons(news):
  for item in news:
    #item = createjson(item)
    #print(item)
    filename = pathout + item["id"] + "--" + item["newspaper"] + ".json"
    
    with open(filename, 'w',encoding="utf-8") as outfile:
      json.dump(item, outfile, ensure_ascii=False)


def save_news_into_json_bundle(news):
  filename = pathoutbundle + "large-bundle-corona.json"
  out = []
  for item in news:
    item = createjson(item)
    out = out + [item]
    
  with open(filename, 'w',encoding="utf-8") as outfile:
    json.dump(out, outfile, ensure_ascii=False)

def main():
    MONGO_URL = os.getenv("database_url")

    client = MongoClient(MONGO_URL, unicode_decode_error_handler='ignore')
    db = client["news-scraped-with-tags"]

    #query = {"date": {
    #    "$gte":   datetime.datetime.strptime("2018-11-01", '%Y-%m-%d'),
    #    "$lt":     datetime.datetime.strptime("2019-12-31", '%Y-%m-%d')}
    #}

    query = {"tags":{"$ne" : None}, "date": {
        "$gte":   datetime.datetime.strptime("2014-05-01", '%Y-%m-%d'),
       "$lt":     datetime.datetime.strptime("2021-11-01", '%Y-%m-%d')}}
    print(query)

    count = db["NewsContentScraped"].find(query)
    count2 = db["NewsContentScraped"].find(query).count()

    print(count2)
    #save_news_into_jsons( count)
    save_news_into_json_bundle(count)

if __name__ == "__main__":
    main()



#tar -cJf tagged-news-2013-2019.tar.xz large-bundle-corona.json