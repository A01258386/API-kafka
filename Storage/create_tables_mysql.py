from sqlalchemy import create_engine, MetaData
from sqlalchemy_utils import database_exists, create_database
import yaml
from drive import Drive
from fly import Fly

with open('app_conf.yml', 'r') as f:
    storage_config = yaml.safe_load(f.read())
STORAGE_SETTING = storage_config['datastore']

db_con = f"mysql+pymysql://{STORAGE_SETTING['user']}:{STORAGE_SETTING['password']}@kafka.westus3.cloudapp.azure.com:{STORAGE_SETTING['port']}/{STORAGE_SETTING['db']}"
engine = create_engine(db_con, echo=True, future=True)
print(db_con)

if not database_exists(engine.url):
    create_database(engine.url)
else:
    # Connect the database if exists.
    connection = engine.connect()

metadata = MetaData(engine)

Drive.metadata.create_all(engine)
Fly.metadata.create_all(engine)

