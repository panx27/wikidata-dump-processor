# wikidata-dump-processor
Import Wikidata [json dump](https://dumps.wikimedia.org/wikidatawiki/entities/) (.json.bz2) into [Mongodb](https://www.mongodb.com/) and create index

- Index:

     [Wikidata ID](https://www.wikidata.org/wiki/Wikidata:Identifiers): `{ id: 1 }`
     
     [English Wikipedia Title](https://www.wikidata.org/wiki/Help:Sitelinks): `{ sitelinks.enwiki.title: 1 }`
     
     [Freebase ID](https://www.wikidata.org/wiki/Property:P646): `{ claims.P646.mainsnak.datavalue.value: 1 }`

     [subclass of](https://www.wikidata.org/wiki/Property:P279): `{ claims.P279.mainsnak.datavalue.value.id: 1 }`
     
     [instance of](https://www.wikidata.org/wiki/Property:P31): `{ claims.P31.mainsnak.datavalue.value.id: 1 }`

     all properties: `{ claims: 1 }`

- [Partial Index](https://docs.mongodb.com/manual/core/index-partial/) for [Covered Query](https://docs.mongodb.com/manual/core/query-optimization/#covered-query):
     `{ sitelinks.enwiki.title: 1, id: 1 }`
     `{ labels.en.value: 1, id: 1 }`

- Performance: ~3 hours for importing, ~1 hour for indexing (`--nworker 12`, `--chunk_size 10000`, based on 20180717 dump (25 GB))


## Quickstart
Step 1: import

    usage: import.py [-h] [--chunk_size CHUNK_SIZE] [--nworker NWORKER]
                     inpath host port db_name collection_name

    positional arguments:
      inpath                Path to inpath file (xxxxxxxx-all.json.bz2)
      host                  MongoDB host
      port                  MongoDB port
      db_name               Database name
      collection_name       Collection name

    optional arguments:
      --chunk_size CHUNK_SIZE, -c CHUNK_SIZE
                            Chunk size (default=10000, RAM usage depends on chunk
                            size)
      --nworker NWORKER, -n NWORKER
      
Step 2: index

    usage: index.py [-h] host port db_name collection_name

    positional arguments:
      host             MongoDB host
      port             MongoDB port
      db_name          Database name
      collection_name  Collection name
      

## Miscellaneous
- If you get `errno:24 Too many open files` error, try to increase system limits. For example, in Linux, you can run `ulimit -n 64000` in the console running mongod.
