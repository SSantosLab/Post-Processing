#!/bin/bash

if pidof -x "Adam.sh" >/dev/null; then
    echo "Adam is awake."
else
    echo "Adam is asleep."
    echo "Waking Adam"
    . Adam.sh
fi