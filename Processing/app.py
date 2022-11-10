from pymysql import Timestamp
import yaml
import logging.config
import connexion
from base import Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import requests
from stats import Stats
import uuid
import pytz

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DRIVE_STATS_URL = app_config['eventstore']['url']
FLY_STATS_URL = app_config['eventstore']['url2']

SQLITE_URL = f"sqlite:///{app_config['datastore']['filename']}"

DB_ENGINE = create_engine(SQLITE_URL)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def get_stats():
    session = DB_SESSION()
    results =session.query(Stats).order_by(Stats.last_updated.desc())
    session.close()

    return results

def get_drive_stats():
    """ Get stats from request """
    # timestamp = datetime.now()
    # timestamp = datetime.strptime(
    # timestamp, "%Y-%m-%d %H:%M:%S")
    timestamp = datetime.now()-timedelta(0,5)
    timestamp = timestamp.replace(microsecond=0)
    
    print(datetime.now().replace(microsecond=0))
    print(timestamp)
    # URL = DRIVE_STATS_URL+"?timestamp="+str(timestamp)
    URL = DRIVE_STATS_URL + '?timestamp=' + str(timestamp)
    TEST_URL = DRIVE_STATS_URL+"?timestamp=2022-10-2 14:22:22"
    response = requests.get(URL)
    if response.status_code==204:
        return {},204
    elif response.status_code==400:
        return {},400
    # print(response.json())
    return response.json(), response.status_code

def get_fly_stats():
    """ Get stats from request """
    # timestamp = datetime.now()
    # timestamp = datetime.strptime(
    # timestamp, "%Y-%m-%d %H:%M:%S")

    timestamp = datetime.now()-timedelta(0,5)
    timestamp = timestamp.replace(microsecond=0)

    URL = FLY_STATS_URL + '?timestamp=' + str(timestamp)
    TEST_URL = FLY_STATS_URL+"?timestamp=2022-10-2 14:22:22"
    response = requests.get(URL)
    #if the response.status_code=204,return empty json
    if response.status_code==204:
        return {},204
    elif response.status_code==400:
        return {},400
    return response.json(), response.status_code

def calculate_stats():
    """ Calculate stats """
    drive_data ,drive_status= get_drive_stats()
    fly_data ,fly_status= get_fly_stats()
    trace_id = str(uuid.uuid4())

    
    calc = {"max_speed": 0, "max_lat": 0, "min_air_pressure": 0, "min_altitute": 0, "min_weight": 0}

    ''' - max_speed
        - max_lat
        - min_air_pressure
        - min_altitute
        - min_weight'''
    
    #calculate max_speed of drive
    tmp_data = {}
    for row in drive_data:
        for key in row:
            if key not in tmp_data:
                tmp_data[key] = [row[key]]
            else:
                #add the value to the list
                tmp_data[key].append(row[key])
                
    #fly_data
    for row in fly_data:
        for key in row:
            if key not in tmp_data:
                tmp_data[key] = [row[key]]
            else:
                #append the value to the list
                tmp_data[key].append(row[key])
    print(tmp_data)
    for key in tmp_data.keys():
        if key == 'speed':
            calc['max_speed'] = max(tmp_data['speed'])

        elif key == 'lat':
            calc['max_lat'] = max(tmp_data['lat'])

        elif key == 'air_pressure':
            calc['min_air_pressure'] = min(tmp_data['air_pressure'])

        elif key == 'altitute':
            calc['min_altitute'] = min(tmp_data['altitute'])

        elif key == 'weight':
            calc['min_weight'] = min(tmp_data['weight'])
    print(calc)

    logger.info('Number of drive events received: %d', len(drive_data))
    logger.info('Number of fly events received: %d', len(fly_data))
    if drive_status != 200:
        logger.error('Error retrieving drive stats: %d', drive_status)
    elif drive_status == 204:
        logger.error('No drive stats available')
    

    if fly_status != 200:
        logger.error('Error retrieving fly stats: %d', fly_status)
    elif fly_status == 204:
        logger.error('No fly stats available')

    #log debug
    logger.debug('Trace_id for Drive and fly stats: %s',trace_id)
    return calc

def populate_stats():
    """ Periodically update stats """
    logger.info('Populate process start')
    stats = calculate_stats()
    session = DB_SESSION()
    data = Stats(
        
        stats['max_speed'],
        stats['max_lat'],
        stats['min_air_pressure'],
        stats['min_altitute'],
        stats['min_weight']
    )
    session.add(data)
    session.commit()
    session.close()
    logger.info('Populate process end')


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    # run our standalone gevent server
    init_scheduler()
    app.run(port=8100, use_reloader=False,debug=True)
