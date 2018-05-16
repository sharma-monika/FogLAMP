# -*- coding: utf-8 -*-

# FOGLAMP_BEGIN
# See: http://foglamp.readthedocs.io/
# FOGLAMP_END

"""FogLAMP Sensor Readings Ingest API"""

import asyncio
from typing import List
import json
from foglamp.common import logger
from foglamp.common.statistics import Statistics
from foglamp.common.storage_client.storage_client import ReadingsStorageClient, StorageClient
from foglamp.common.storage_client.exceptions import StorageServerError


__author__ = "Terris Linenbach"
__copyright__ = "Copyright (c) 2017 OSIsoft, LLC"
__license__ = "Apache 2.0"
__version__ = "${VERSION}"

_LOGGER = logger.setup(__name__)
_STATISTICS_WRITE_FREQUENCY_SECONDS = 5


class Ingest(object):
    """Adds sensor readings to FogLAMP

    Internally tracks readings-related statistics.
    """

    _core_management_host = ""
    _core_management_port = 0
    _parent_service = None

    readings_storage = None  # type: Readings
    storage = None  # type: Storage

    _readings_stats = 0  # type: int
    """Number of readings accepted before statistics were written to storage"""

    _discarded_readings_stats = 0  # type: int
    """Number of readings rejected before statistics were written to storage"""

    _sensor_stats = {}  # type: dict
    """Number of sensor readings accepted before statistics were written to storage"""

    _write_statistics_task = None  # type: asyncio.Task
    """asyncio task for :meth:`_write_statistics`"""

    _write_statistics_sleep_task = None  # type: asyncio.Task
    """asyncio task for asyncio.sleep"""

    _stop = False
    """True when the server needs to stop"""

    _started = False
    """True when the server has been started"""

    _readings_lists = None  # type: List
    """A list of readings lists. Each list contains the inputs to :meth:`add_readings`."""

    _current_readings_list_index = 0
    """Which readings list to insert into next"""

    _insert_readings_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks for :meth:`_insert_readings`"""

    _readings_list_batch_size_reached = None  # type: List[asyncio.Event]
    """Fired when a readings list has reached _readings_insert_batch_size entries"""

    _readings_list_not_empty = None  # type: List[asyncio.Event]
    """Fired when a readings list transitions from empty to not empty"""

    _readings_lists_not_full = None  # type: asyncio.Event
    """Fired when items are removed from any readings list"""

    _insert_readings_wait_tasks = None  # type: List[asyncio.Task]
    """asyncio tasks blocking :meth:`_insert_readings` that can be canceled"""

    _last_insert_time = 0  # type: int
    """epoch time of last insert"""

    _readings_list_size = 0  # type: int
    """Maximum number of readings items in each buffer"""

    # Configuration (begin)
    _write_statistics_frequency_seconds = 5
    """The number of seconds to wait before writing readings-related statistics to storage"""

    _readings_buffer_size = 500
    """Maximum number of readings to buffer in memory"""

    _max_concurrent_readings_inserts = 5
    """Maximum number of concurrent processes that send batches of readings to storage"""

    _readings_insert_batch_size = 100
    """Maximum number of readings in a batch of inserts"""

    _readings_insert_batch_timeout_seconds = 1
    """Number of seconds to wait for a readings list to reach the minimum batch size"""

    _max_readings_insert_batch_connection_idle_seconds = 60
    """Close connections used to insert readings when idle for this number of seconds"""

    _max_readings_insert_batch_reconnect_wait_seconds = 10
    """The maximum number of seconds to wait before reconnecting to storage when inserting readings"""

    # Configuration (end)


    # Class attributes
    _num_readings = 0  # type: int
    """number of readings accepted before statistics were flushed to the database"""

    _num_discarded_readings = 0  # type: int
    """number of readings rejected before statistics were flushed to the database"""

    _write_statistics_loop_task = None  # type: asyncio.Future
    """Asyncio task for :meth:`_write_statistics_loop`"""

    _sleep_task = None  # type: asyncio.Future
    """Asyncio task for asyncio.sleep"""

    _stop = False  # type: bool
    """Set to true when the server needs to stop"""

    @classmethod
    async def _read_config(cls):
        """Creates default values for the South configuration category and then reads all
        values for this category
        """
        category = 'South'

        default_config = {
            "write_statistics_frequency_seconds": {
                "description": "The number of seconds to wait before writing readings-related "
                               "statistics to storage",
                "type": "integer",
                "default": str(cls._write_statistics_frequency_seconds)
            },
            "readings_buffer_size": {
                "description": "The maximum number of readings to buffer in memory",
                "type": "integer",
                "default": str(cls._readings_buffer_size)
            },
            "max_concurrent_readings_inserts": {
                "description": "The maximum number of concurrent processes that send batches of "
                               "readings to storage",
                "type": "integer",
                "default": str(cls._max_concurrent_readings_inserts)
            },
            "readings_insert_batch_size": {
                "description": "The maximum number of readings in a batch of inserts",
                "type": "integer",
                "default": str(cls._readings_insert_batch_size)
            },
            "readings_insert_batch_timeout_seconds": {
                "description": "The number of seconds to wait for a readings list to reach the "
                               "minimum batch size",
                "type": "integer",
                "default": str(cls._readings_insert_batch_timeout_seconds)
            },
            "max_readings_insert_batch_connection_idle_seconds": {
                "description": "Close storage connections used to insert readings when idle for "
                               "this number of seconds",
                "type": "integer",
                "default": str(cls._max_readings_insert_batch_connection_idle_seconds)
            },
            "max_readings_insert_batch_reconnect_wait_seconds": {
                "description": "The maximum number of seconds to wait before reconnecting to "
                               "storage when inserting readings",
                "type": "integer",
                "default": str(cls._max_readings_insert_batch_reconnect_wait_seconds)
            },
        }

        # Create configuration category and any new keys within it
        config_payload = json.dumps({
            "key": category,
            "description": 'South server configuration',
            "value": default_config,
            "keep_original_items": False
        })
        cls._parent_service._core_microservice_management_client.create_configuration_category(config_payload)

        # Read configuration
        config = cls._parent_service._core_microservice_management_client.get_configuration_category(category_name=category)

        cls._write_statistics_frequency_seconds = int(config['write_statistics_frequency_seconds']
                                                      ['value'])
        cls._readings_buffer_size = int(config['readings_buffer_size']['value'])
        cls._max_concurrent_readings_inserts = int(config['max_concurrent_readings_inserts']
                                                   ['value'])
        cls._readings_insert_batch_size = int(config['readings_insert_batch_size']['value'])
        cls._readings_insert_batch_timeout_seconds = int(config
                                                         ['readings_insert_batch_timeout_seconds']
                                                         ['value'])
        cls._max_readings_insert_batch_connection_idle_seconds = int(
            config['max_readings_insert_batch_connection_idle_seconds']
            ['value'])
        cls._max_readings_insert_batch_reconnect_wait_seconds = int(
            config['max_readings_insert_batch_reconnect_wait_seconds']['value'])

    @classmethod
    async def start(cls):
        """Starts the server"""
        if cls._started:
            return

        cls._core_management_host = core_mgt_host
        cls._core_management_port = core_mgt_port
        cls._parent_service = parent

        cls.readings_storage = ReadingsStorageClient(cls._core_management_host, cls._core_management_port)
        cls.storage = StorageClient(cls._core_management_host, cls._core_management_port)

        await cls._read_config()

        cls._write_statistics_loop_task = asyncio.ensure_future(cls._write_statistics_loop())

    @classmethod
    async def stop(cls):
        if cls._stop or cls._write_statistics_loop_task is None:
            return

        cls._stop = True

        if cls._sleep_task is not None:
            cls._sleep_task.cancel()
            cls._sleep_task = None

        await cls._write_statistics_loop_task
        cls._write_statistics_loop_task = None

    @classmethod
    def increment_discarded_messages(cls):
        cls._num_discarded_readings += 1

    @classmethod
    async def _write_statistics_loop(cls):
        """Periodically commits collected readings statistics"""
        _LOGGER.info("Ingest statistics writer started")
        stats = Statistics(cls.storage)

        # Register static statistics
        await stats.register('READINGS', 'The number of readings received by FogLAMP since startup')
        await stats.register('DISCARDED', 'The number of readings discarded at the input side by FogLAMP, i.e. '
                                          'discarded before being  placed in the buffer. This may be due to some '
                                          'error in the readings themselves.')
        while not cls._stop:
            # stop() calls _sleep_task.cancel().
            # Tracking _sleep_task separately is cleaner than canceling
            # this entire coroutine because allowing database activity to be
            # interrupted will result in strange behavior.
            cls._sleep_task = asyncio.ensure_future(
                                asyncio.sleep(_STATISTICS_WRITE_FREQUENCY_SECONDS))

            try:
                await cls._sleep_task
            except asyncio.CancelledError:
                pass

            cls._sleep_task = None

            try:
                # TODO Move READINGS and DISCARDED to globals
                await stats.update('READINGS', cls._num_readings)
                cls._num_readings = 0

                await stats.update('DISCARDED', cls._num_discarded_readings)
                cls._num_discarded_readings = 0
            # TODO catch real exception
            except Exception:
                _LOGGER.exception("An error occurred while writing readings statistics")

        _LOGGER.info("Ingest statistics writer stopped")

    @classmethod
    async def add_readings(cls, data: dict)->None:
        """Sends asset readings to storage layer

        Args:
            data:
            {
                "timestamp": "2017-01-02T01:02:03.23232Z-05:00",
                "asset": "pump1",
                "readings": {
                    "velocity": "500",
                    "temperature": {
                        "value": "32",
                        "unit": "kelvin"
                    }
                }
            }

        Raises KeyError: data is missing a required field
        Raises IOError: some type of failure occurred
        Raises TypeError: bad data provided
        """

        cls._num_readings += 1
        try:
            payload = dict()
            payload['readings'] = data
            cls.readings_storage.append(json.dumps(payload))
        except StorageServerError as ex:
            err_response = ex.error
            # if key error in next, it will be automatically in parent except block
            if err_response["retryable"]:  # retryable is bool
                # raise and exception handler will retry
                _LOGGER.warning("Got %s error, retrying ...", err_response["source"])
                raise
            else:
                # not retryable
                _LOGGER.error("%s, %s", err_response["source"], err_response["message"])
                batch_size = len(readings_list)
                cls._discarded_readings_stats += batch_size
        except Exception:
            cls._num_discarded_readings += 1
            _LOGGER.exception(
                "Database error occurred. Payload:\n%s",
                data)
            raise
