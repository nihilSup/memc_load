# memc load

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
