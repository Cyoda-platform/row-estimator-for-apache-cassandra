# Row cassandra estimator
copied base from https://github.com/aws-samples/row-estimator-for-apache-cassandra

Original project has bug (can not connect to table, if table name with upper case) and can not iterate over all tables in keyspace

## How to run
```bash


# 1. activate virtual env
python3 -m venv venv
source venv/bin/activate

# 2. install dependecies
pip install -r requirements.txt

# 3. run 
 python3 ./src/main.py --hostname 127.0.0.1 --port 9042 --keyspace <your_keyspace>   

# 4. deactivate virtual env
deactivate
```

