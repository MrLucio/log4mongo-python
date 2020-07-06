import asyncio
from io import StringIO
import unittest
import logging
import time
import sys
import threading
import traceback

import datetime as dt

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure
from pymongo.errors import ServerSelectionTimeoutError

from aiologger.filters import Filter
from aiologger.formatters.base import Formatter
from aiologger.handlers.base import Handler
from aiologger.levels import LogLevel
from aiologger.protocols import AiologgerProtocol
from aiologger.records import LogRecord

_connection = None

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

    def __init__(self, URI, level=LogLevel.NOTSET,  collection='logs',
                 loop=None, username=None, password=None,
                 database_name=None, authentication_db='admin',
                 fail_silently=False, formatter=None, capped=False,
                 capped_max=1000, capped_size=1000000, reuse=True, **kwargs):
        """
        Setting up mongo handler, initializing mongo database connection via
        pymongo.
        If reuse is set to false every handler will have it's own MongoClient.
        This could hammer down your MongoDB instance, but you can still use
        this option.
        The default is True. As such a program with multiple handlers
        that log to mongodb will have those handlers share a single connection
        to MongoDB.
        """
        super().__init__(loop=loop)

        self.URI = URI
        self.database_name = database_name
        self.collection_name = collection
        self.username = username
        self.password = password
        self.authentication_database_name = authentication_db
        self.fail_silently = fail_silently
        self.connection = None
        self.db = None
        self.collection = None
        self.authenticated = False
        self.formatter = formatter or MongoFormatter
        self.capped = capped
        self.capped_max = capped_max
        self.capped_size = capped_size
        self.reuse = reuse
        self.mongo_job_id = None

        self._initialization_lock = asyncio.Lock(loop=self.loop)
        
        self._connect(**kwargs)


    def set_mongo_job_id(self, mongo_job_id):
        self.mongo_job_id = mongo_job_id


    @property
    def initialized(self):
        return self.writer is not None


    async def _init_writer(self):
        pass


    async def flush(self):
        self.connection.fsync(options={'async': True})


    def _connect(self, **kwargs):
        """Connecting to mongo database."""
        global _connection
        if self.reuse and _connection:
            self.connection = _connection
        else:
            self.connection = MongoClient(self.URI)
            try:
                self.connection.is_primary
            except ServerSelectionTimeoutError:
                if self.fail_silently:
                    return
                raise
            _connection = self.connection

        self.db = self.connection[self.database_name]
        if self.username is not None and self.password is not None:
            auth_db = self.connection[self.authentication_database_name]
            self.authenticated = auth_db.authenticate(self.username,
                                                      self.password)

        if self.capped:
            #
            # We don't want to override the capped collection
            # (and it throws an error anyway)
            try:
                self.collection = Collection(self.db, self.collection_name,
                                             capped=True, max=self.capped_max,
                                             size=self.capped_size)
            except OperationFailure:
                # Capped collection exists, so get it.
                self.collection = self.db[self.collection_name]
        else:
            self.collection = self.db[self.collection_name]


    async def emit(self, record):
        """Inserting new logging record to mongo database."""
        if self.collection is not None:
            try:
                self.collection.update_one(
                    {
                        'mongo_job_id': {'$eq': self.mongo_job_id }
                    },
                    {'$push': {
                        'records': self.formatter.format(self, record)
                    }},
                    upsert=True,
                )
                await self.flush()
            except Exception as e:
                if not self.fail_silently:
                    logging.exception(e)


    async def close(self):
        """
        If authenticated, logging out and closing mongo database connection.
        """
        if self.authenticated:
            self.db.logout()
        if self.connection is not None:
            self.connection.close()

