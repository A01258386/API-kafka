from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Fly(Base):
    """ Fly event """

    __tablename__ = "fly"
    id = Column(Integer, primary_key=True)
    altitute = Column(Integer, nullable=False)
    air_pressure = Column(Integer, nullable=False)
    city = Column(String(100), nullable=False)
    weight = Column(Integer, nullable=False)
    date_created = Column(DateTime, default=False)
    trace_id = Column(String(100), nullable=False)

    def __init__(self, id, altitute, air_pressure, city, weight, date_created, trace_id):
        """ Initializer with name """
        self.id = id
        self.altitute = altitute
        self.air_pressure = air_pressure
        self.city = city
        self.weight = weight
        self.date_created = date_created
        self.trace_id = trace_id

    def to_dict(self):
        """ Dictionary Representation of a fly event reading """
        dict = {}
        dict['id'] = self.id
        dict['altitute'] = self.altitute
        dict['air_pressure'] = self.air_pressure
        dict['city'] = self.city
        dict['weight'] = self.weight
        dict['date_created'] = self.date_created
        dict['trace_id'] = self.trace_id

        return dict
