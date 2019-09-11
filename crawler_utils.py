import json
import os
import pymongo

def loadAccessData(platform):
    with open('config.json',) as json_data_file:
        data = json.load(json_data_file)[platform]
        return data["consumer_key"], data["consumer_secret"], data["access_token"], data["access_token_secret"]
    
def loadLastId(platform):
    with open('crawler-log.json',) as json_data_file:
            data = json.load(json_data_file)[platform]
            return data["last_retrieved_id"]
        
def readCrawlerLog():
    if (os.path.exists('crawler-log.json')):
        with open('crawler-log.json', 'r+') as json_file:
            data = json.load(json_file)
            json_file.close()
        return data
    else:
        with open('crawler-log.json', 'a+') as json_file:
            return {}
        
def writeCrawlerLog(platform, newData):
    logData = readCrawlerLog()
    if (logData):
        tmp = logData[platform]
        logData[platform] = newData
        
        with open('crawler-log.json', 'w') as json_file:
            json.dump(logData, json_file)
            json_file.close()
    else:
        logData[platform] = newData
        with open('crawler-log.json', 'w') as json_file:
            json.dump(logData, json_file)
            json_file.close()
                        
def crawlTwitter(api, q, since_id):
    results = api.search(q=q, since_id=since_id, result_type="recent")
    return results

def connect():
    client = pymongo.MongoClient('localhost', 27017)
    return client