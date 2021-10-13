# encoding=utf-8

import logging
import logging.config

from cores.exception import SqlalchemyError
from sqlalchemy import Column, DECIMAL, String, create_engine, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# 创建对象的基类:
Base = declarative_base()


class DataConnector(object):
    def __init__(self, connect_string):
        """
        """
        self.connect_string = connect_string
        self.session = None
        self.engine = None
        self.log = logging.getLogger(self.__class__.__name__)

    def get_connection(self):
        DBSession = sessionmaker(bind=self.get_engine())
        self.session = DBSession()
        return self.session

    def get_engine(self):
        self.engine = create_engine(self.connect_string)
        return self.engine

    def release(self):
        try:
            self.session.close()
        except BaseException:
            raise SqlalchemyError('session close faild!')
