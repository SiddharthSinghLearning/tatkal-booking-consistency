#!/bin/sh
cd "$(dirname "$0")"
python3 tatkal_offline.py --sweep-seconds 20
