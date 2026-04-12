#!/bin/bash
mkdir -p out/
cat log/build_db.log log/complete_db.log > out/execution.log
tar -czf out/imdb.tar.gz --transform='s!.*/!!' *.sqlite log/*.log
cd out/
find . -type f -name '*.json' -print0 | tar -I 'zstd -3' -cf ../cache.tar.zst ./
mv ../cache.tar.zst ./
cd ..
sqlite3 imdb.sqlite < sql/set_index.sql
zstd -3 imdb.sqlite
mv imdb.sqlite.zst out/
cd out/
tree -H . -o index.html