#!/bin/bash
mkdir -p out/
cat log/build_db.log log/complete_db.log > out/execution.log
tar -czf out/imdb.tar.gz --transform='s!.*/!!' *.sqlite log/*.log
cd out/
tree -H . -o index.html
tree