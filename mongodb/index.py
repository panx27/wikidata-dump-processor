import logging
import argparse

from pymongo import MongoClient


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('host', help='MongoDB host')
    parser.add_argument('port', help='MongoDB port')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    args = parser.parse_args()

    host = args.host
    port = int(args.port)
    db_name = args.db_name
    collection_name = args.collection_name

    logger.info('db name: %s' % db_name)
    logger.info('collection name: %s' % collection_name)
    logger.info('indexing...')
    client = MongoClient(host=host, port=port)
    collection = client[db_name][collection_name]

    # { id: 1 }
    logger.info('index key: { id: 1 }')
    collection.create_index('id', unique=True)

    # { sitelinks.enwiki.title: 1 }
    logger.info('index key: { sitelinks.enwiki.title : 1 }')
    key = [('sitelinks.enwiki.title', 1)]
    pfe = {'sitelinks.enwiki.title': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { sitelinks.enwiki.title: 1, id: 1 }
    logger.info('index key: { sitelinks.enwiki.title: 1, id: 1 }')
    key = [('sitelinks.enwiki.title', 1), ('id', 1)]
    pfe = {'sitelinks.enwiki.title': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { labels.en.value: 1, id: 1 }
    logger.info('index key: { labels.en.value: 1, id: 1 }')
    key = [('labels.en.value', 1), ('id', 1)]
    pfe = {'labels.en.value': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P279.mainsnak.datavalue.value.id: 1 }
    logger.info('index key: { claims.P279.mainsnak.datavalue.value.id: 1 }')
    key = [('claims.P279.mainsnak.datavalue.value.id', 1)]
    pfe = {'claims.P279.mainsnak.datavalue.value.id': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P31.mainsnak.datavalue.value.id: 1 }
    logger.info('index key: { claims.P31.mainsnak.datavalue.value.id: 1 }')
    key = [('claims.P31.mainsnak.datavalue.value.id', 1)]
    pfe = {'claims.P31.mainsnak.datavalue.value.id': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)

    # { claims.P646.mainsnak.datavalue.value: 1 }
    logger.info('index key: { claims.P646.mainsnak.datavalue.value: 1 }')
    key = [('claims.P646.mainsnak.datavalue.value', 1)]
    pfe = {'claims.P646.mainsnak.datavalue.value': {'$exists': True}}
    collection.create_index(key, partialFilterExpression=pfe)
