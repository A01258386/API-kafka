from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Stats(Base):
    """ drive event"""
    ''' - max_speed
        - max_lat
        - min_air_pressure
        - min_altitute
        - min_weight'''
    __tablename__ = "stats"
    id = Column(Integer, primary_key=True)
    max_speed = Column(Integer, nullable=False)
    max_lat = Column(Integer, nullable=False)
    min_airpressure = Column(Integer, nullable=False)
    min_altitute = Column(Integer, nullable=False)
    min_weight = Column(Integer, default=False)

    def __init__(self,max_speed, max_lat, min_airpressure, min_altitute, min_weight):
        """ Create a new drive reading """
        self.max_speed = max_speed
        self.max_lat = max_lat
        self.min_airpressure = min_airpressure
        self.min_altitute = min_altitute
        self.min_weight = min_weight
        # self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary Representation of a drive reading """
        dict = {}
        dict['id'] = self.id
        dict['max_speed'] = self.max_speed
        dict['max_lat'] = self.max_lat
        dict['min_airpressure'] = self.min_airpressure
        dict['min_altitute'] = self.min_altitute
        dict['min_weight'] = self.min_weight
        # dict['date_created'] = self.date_created

        return dict
