'''
Copyright (c) 2014, Emmanuel Levijarvi
All rights reserved.
License BSD
'''
import logging
import datetime
from iotrelay import Reading
from pywws import DataStore

__version__ = "1.0.0"

logger = logging.getLogger(__name__)

# hours to look back for readings
DEFAULT_LOOKBACK = 2
READING_TYPE = 'weather'


class Poll(object):
    def __init__(self, config):
        self.keys = [k.strip() for k in config['series keys'].split(',')]
        self.data_store = config['data store']
        poll_frequency = int(config['poll frequency'])
        self.delta = datetime.timedelta(seconds=poll_frequency)
        lookback = int(config.get('lookback', DEFAULT_LOOKBACK))
        hours = datetime.timedelta(hours=lookback)
        self.last_ts = datetime.datetime.utcnow() - hours

    def get_readings(self):
        current_timestamp = datetime.datetime.utcnow()
        if current_timestamp < self.last_ts + self.delta:
            logger.debug('pywws {0!s} until next reading'.format(
                         self.last_ts + self.delta - current_timestamp))
            return ()
        readings = DataStore.data_store(self.data_store)
        for reading in readings[self.last_ts:]:
            for key in self.keys:
                yield Reading(READING_TYPE, reading[key], reading['idx'], key)
        self.last_ts = current_timestamp
