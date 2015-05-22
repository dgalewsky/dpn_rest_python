#!/bin/bash

while :
do
    echo "Polling..."
    python dpn_replicating_node.py
    sleep 5
done

