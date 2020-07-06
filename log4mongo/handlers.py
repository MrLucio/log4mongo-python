import asyncio
from io import StringIO
import unittest
import logging
import time
import sys
import threading
import traceback

import datetime as dt

from aiologger.filters import Filter
from aiologger.formatters.base import Formatter
from aiologger.handlers.base import Handler
from aiologger.levels import LogLevel
from aiologger.protocols import AiologgerProtocol
from aiologger.records import LogRecord

from motor.motor_asyncio import AsyncIOMotorClient

class MongoFormatter(Formatter):

    DEFAULT_PROPERTIES = LogRecord(
        '', 0, '', 0, '', '', '', '', '').__dict__.keys()

    def format(self, record):
        """Formats LogRecord into python dictionary."""
        # Standard document
        document = {
            'level': record.levelname,
            'loggerName': record.name,
            'message': record.msg,
            'timestamp': dt.datetime.utcnow()
        }
        # Standard document decorated with exception info
        if record.exc_info is not None:
            tblist = traceback.extract_tb(record.exc_info[2])
            last_traceback_item = tblist[-1]
            document.update({
                'exception':{
                    'exc_message': str(record.exc_info[1]),
                    'exc_fileName': last_traceback_item.filename,
                    'exc_method': last_traceback_item.name,
                    'exc_lineno': last_traceback_item.lineno,
                    'exc_line': last_traceback_item.line,
                    'exc_trace': Formatter.format_exception(self, record.exc_info)
                }
            })
        # Standard document decorated with extra contextual information
        if len(MongoFormatter.DEFAULT_PROPERTIES) != len(record.__dict__):
            contextual_extra = set(record.__dict__).difference(
                set(MongoFormatter.DEFAULT_PROPERTIES))
            if contextual_extra:
                for key in contextual_extra:
                    document[key] = record.__dict__[key]
        return document


class AsyncMongoHandler(Handler):

    terminator = "\n"

    def __init__(
        self,
        db,
        level=LogLevel.NOTSET, 
        collection='logs',
        loop=None,
        fail_silently=False,
        formatter=None,
        **kwargs
    ):   
        super().__init__(loop=loop)


        self.db = db
        self.collection = self.db[collection]
        self.fail_silently = fail_silently
        self.formatter = formatter or MongoFormatter
        self.mongo_job_id = None


    def set_mongo_job_id(self, mongo_job_id):
        self.mongo_job_id = mongo_job_id


    @property
    def initialized(self):
        return self.writer is not None


    async def _init_writer(self):
        pass


    async def emit(self, record):
        """Inserting new logging record to mongo database."""
        if self.collection is not None:
            try:
                await self.collection.update_one(
                    {
                        'mongo_job_id': {'$eq': self.mongo_job_id }
                    },
                    {'$push': {
                        'records': self.formatter.format(self, record)
                    }},
                    upsert=True,
                )
            except Exception as e:
                if not self.fail_silently:
                    logging.exception(e)
