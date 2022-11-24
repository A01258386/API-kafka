from pymysql import Timestamp
import yaml
import logging.config
import connexion
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import requests
import uuid
import pytz
from flask_cors import CORS, cross_origin
import json
from multiprocessing import Pool
import os

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"
with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())
    # External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)
logger = logging.getLogger('basicLogger')
logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

def writetojson(dict):
    with open('healthcheckdata.json','r+') as f:
        file_data = json.load(f)
        file_data.append(dict)
        f.seek(0)
        json.dump(file_data,f,indent=4)

def healthxheck():
    with Pool() as pool:
        status


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
    # run our standalone gevent server
    app.run(port=8100, use_reloader=False,debug=True)
