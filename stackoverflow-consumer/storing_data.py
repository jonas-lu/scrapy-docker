import os, redis, logging, json, time
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)

crawler_name = os.environ.get('CRAWLER_NAME')
db_name = os.environ.get('DB_NAME')
collection_name = os.environ.get('COLLECTION_NAME')

logging.info('Using crawler name: %s', crawler_name)
logging.info('Using db name: %s', db_name)
logging.info('Using collection: %s', collection_name)

r = redis.StrictRedis(host='redis', port=6379, db=0)
mongo = MongoClient('mongo', 27017)
db = mongo[db_name]
collection = db[collection_name]

result_key = crawler_name + ':items'

while True:
    question = r.blpop(result_key, 120)
    if question:
        doc = json.loads(question[1].decode('utf-8'))
        collection.insert_one(doc)
        logging.info('Saved question: %s', doc['title'])
    else:
        logging.info('Empty result queue, sleep')
        time.sleep(10)
