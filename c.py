#!/bin/bash

FILE="/home/co/uploader.json.lock"

while true; do
    if [ -f "$FILE" ]; then
        echo "$(date): File exists, removing $FILE"
        rm "$FILE"
    else
        echo "$(date): File does not exist"
    fi
    sleep 60
done
