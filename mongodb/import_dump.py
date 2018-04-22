import sys
import os
import bz2
import ujson as json
from itertools import islice
from pymongo import MongoClient
import multiprocessing
import logging
from configparser import ConfigParser
import argparse


logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)


def process(lines):
    try:
        verbose = False
        if verbose:
            pid = os.getpid()
            logger.info('%s started' % pid)
        client = MongoClient(host=host, port=port)
        collection = client[db_name][collection_name]
        data = []
        for line in lines:
            line = line.decode('utf-8').rstrip('\n')
            if line == '[' or line == ']':
                continue
            if line[-1] == ',':
                line = line[:-1]
            d = json.loads(line)
            data.append(d)
        collection.insert(data)
        client.close()
        if verbose:
            logger.info('%s finished' % pid)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        msg = 'unexpected error: %s | %s | %s' % \
              (exc_type, exc_obj, exc_tb.tb_lineno)
        logger.error(msg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pconf', help='Path to config file')
    parser.add_argument('inpath', help='ath to inpath file (x-all.json.bz2)')
    parser.add_argument('db_name', help='Database name')
    parser.add_argument('collection_name', help='Collection name')
    parser.add_argument('--chunk_size', '-c', default=10000,
                        help='Chunk size (default=10000)')
    parser.add_argument('--nworker', '-n', default=1,
                        help='Number of workers (default=1)')
    args = parser.parse_args()

    config_path = args.pconf
    config = ConfigParser()
    config.read(config_path)
    host = config.get('mongodb', 'host')
    port = config.getint('mongodb', 'port')
    pdata = args.inpath # /nas/data/m1/panx2/data/KBs/dump/wikidata/20180412/20180412-all.json.bz2
    db_name = args.db_name
    collection_name = args.collection_name
    nworker = int(args.nworker)
    chunk_size = int(args.chunk_size)

    logger.info('db name: %s' % db_name)
    logger.info('collection name: %s' % collection_name)
    client = MongoClient(host=host, port=port)
    logger.info('drop collection')
    client[db_name].drop_collection(collection_name)

    logger.info('importing...')
    pool = multiprocessing.Pool(processes=nworker)
    logger.info('# of workers: %s' % nworker)
    logger.info('chunk size: %s' % chunk_size)
    logger.info('parent pid: %s' % os.getpid())
    with bz2.BZ2File(pdata) as f:
        for n_lines in iter(lambda: tuple(islice(f, chunk_size)), ()):
            pool.apply_async(process, args=(n_lines,),)
    pool.close()
    pool.join()

    logger.info('indexing...')
    collection = client[db_name][collection_name]
    collection.create_index('id', unique=True)
    collection.create_index('sitelinks.enwiki.title', sparse=True)
    k = [('sitelinks.enwiki.title', 1), ('id', 1)]
    pfe = {'sitelinks.enwiki.title': {'$exists': True}}
    collection.create_index(k, partialFilterExpression=pfe)

    logger.info('done.')
