# memc load

Searches files by pattern. Every line in each file are parsed (assumed it is some log entry) and packed by protobuf and then posted to memcached. Where is possible threads and processes are used.

## Installation

### Install protobuf

'''shell
brew install protobuf
protoc  --python_out=. ./appsinstalled.proto
pip install protobuf
'''

### Install memcached

'''shell
pip install python-memcached
'''

## Usage example

'''shell
python memc_load.py --pattern=/Users/GoodInc/Downloads/*.tsv.gz
'''
