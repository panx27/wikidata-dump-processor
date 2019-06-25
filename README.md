# wikidata-dump-processor
import Wikidata json dump (.json.bz2) into Mongodb

- Index fields: `{ id: 1 }`, `{ sitelinks.enwiki.title: 1 }`

- [Partial Index](https://docs.mongodb.com/manual/core/index-partial/) for [Covered Query](https://docs.mongodb.com/manual/core/query-optimization/#covered-query): `{ sitelinks.enwiki.title: 1, id: 1 }`, `{ labels.en.value: 1, id: 1 }`

- Performance: ~3 hours for importing, ~1 hour for indexing (`--nworker 12`, `--chunk_size 10000`, based on 20180717 dump (25 GB))
