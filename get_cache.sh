#!/bin/bash
FILE="cache.tar.zst"
if [ ! -f "${FILE}" ]; then
    URL="${PAGE_URL}/${FILE}"
    echo "[..] $URL"
    wget -q "$URL" -O "${FILE}"
    if [ $? -eq 0 ]; then
        echo "[OK] $URL"
    else
        echo "[KO] $URL"
    fi
fi
if [ -f "${FILE}" ]; then
    mkdir -p ./out/
    echo "[..] $FILE"
    tar --zstd -xf "${FILE}" -C ./out/
    if [ $? -eq 0 ]; then
        echo "[OK] $FILE"
    else
        echo "[KO] $FILE"
    fi
fi