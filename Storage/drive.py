from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Drive(Base):
    """ drive event"""

    __tablename__ = "drive"
    id = Column(Integer, primary_key=True)
    speed = Column(Integer, nullable=False)
    timestamp = Column(String(100), nullable=False)
    lat = Column(Integer, nullable=False)
    long = Column(Integer, nullable=False)
    date_created = Column(DateTime, default=False)
    trace_id = Column(String(100), nullable=False)

    def __init__(self, id, speed, timestamp, lat, long, date_created, trace_id):
        """ Initializer with name """
        self.id = id
        self.speed = speed
        self.timestamp = timestamp
        self.lat = lat
        self.long = long
        self.date_created = date_created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a drive reading """
        dict = {}
        dict['id'] = self.id
        dict['speed'] = self.speed
        dict['timestamp'] = self.timestamp
        dict['lat'] = self.lat
        dict['long'] = self.long
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict
