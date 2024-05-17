# Row cassandra estimator
copied base from https://github.com/aws-samples/row-estimator-for-apache-cassandra
Original project has bug and work with all tables in keyspace

## How to run
```bash
# 1. activate virtual env
python3 -m venv venv
source venv/bin/activate

# 2. install dependecies
pip install -r requirements.txt

# 3. run 
python3 ./src/main.py

# 4. deactivate virtual env
deactivate
```

