from __future__ import absolute_import

try:
    from ConfigParser import RawConfigParser
except ImportError:
    from configparser import RawConfigParser

import argparse
import logging
import multiprocessing
import os
import signal
import sys
import traceback
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from copy import copy
from datetime import timedelta
from functools import partial
from logging.config import dictConfig
from tempfile import gettempdir, mkstemp

#from pyramid.path import DottedNameResolver
#from pyramid.settings import asbool

import six

import zmq
from zmq.eventloop import ioloop


INITIAL_TIMEOUT = timedelta(milliseconds=10)
MAX_TIMEOUT = timedelta(seconds=45)
SOCKET_TYPES = {
    zmq.REQ: 'REQ',
    zmq.REP: 'REP',
    zmq.PUB: 'PUB',
    zmq.SUB: 'SUB',
    zmq.PAIR: 'PAIR',
    zmq.DEALER: 'DEALER',
    zmq.ROUTER: 'ROUTER',
    zmq.PUSH: 'PUSH',
    zmq.PULL: 'PULL',
}

ZMQChannel = namedtuple(
    'ZMQChannel',
    ['endpoint', 'socket_type', 'bind', 'subscription',
     'recv_handler', 'hwm', 'drained_by', 'drains'],
)
ZMQChannel.__new__ = partial(
    ZMQChannel.__new__,
    bind=False,
    subscription=('*', str(os.getpid())),
    recv_handler=None,
    # This value is additive to the ZMQ socket HWM
    hwm=1000,
    # Specifying this value will trigger HWM throttling
    drained_by=None,
    # This must be set on the next pipeline stage after drained_by
    drains=None,
)


# Unfortunately, RawConfigParser forces all option keys to lower-case
class BaseConfigParser(RawConfigParser):

    def optionxform(self, optionstr):
        return optionstr


def setup_logging(config_uri, incremental=False, **kwargs):
    logging_config = {'version': 1, 'incremental': incremental}
    cp = BaseConfigParser()
    cp.read([config_uri])

    for scontainer, sbegin in {'loggers': 'logger',
                               'handlers': 'handler',
                               'formatters': 'formatter',
                               'filters': 'filter'}.items():
        section = {}
        if not cp.has_section(scontainer):
            continue
        for sname in [i.strip()
                      for i in cp.get(scontainer, 'keys').split(',')]:
            if not sname:
                continue
            settings = {}
            for skey, sval in cp.items('_'.join([sbegin, sname])):
                if ',' in sval:
                    sval = [i.strip() for i in sval.split(',')]
                if skey in ('handlers', 'filters') and \
                   not isinstance(sval, list):
                    if sval:
                        sval = [sval]
                    else:
                        sval = []
                elif skey in ('backupCount', 'maxBytes'):
                    sval = int(sval)
                elif skey == 'stream':
                    sval = logging.config._resolve(sval)
                elif skey == 'args':
                    continue
                elif skey == 'propagate':
                    sval = asbool(sval)
                settings[skey] = sval
            section[sname] = settings

        logging_config[scontainer] = section

    root_config = logging_config['loggers'].pop('root')

    if 'zmq' in root_config['handlers'] and 'context' in kwargs:
        logging_config['handlers']['zmq']['context'] = kwargs['context']
        logging_config['handlers']['zmq']['loop'] = kwargs['loop']
        logging_config['handlers']['zmq']['async'] = kwargs.get('async', False)

    logging_config['root'] = root_config

    dictConfig(logging_config)

    if incremental:
        logging.debug('Logging re-configured.')
    else:
        logging.debug('Logging configured.')


def info_signal_handler(signal, frame):
    curr_proc = multiprocessing.current_process()
    logger = logging.getLogger('eyrie.script.stats')
    logger.error('Dumping stack for: %s, PID: %d\n%s',
                 curr_proc.name, curr_proc.pid,
                 ''.join(traceback.format_stack(frame)))


def vmprof_signal_handler(signal, frame):
    import vmprof
    curr_proc = multiprocessing.current_process()
    logger = logging.getLogger('eyrie.script.profile')
    if getattr(curr_proc, 'vmprof_enabled', False):
        logger.warn('Disabling vmprof, output path: %s',
                    curr_proc.profile_output_path)
        vmprof.disable()
        curr_proc.vmprof_enabled = False
    else:
        fileno, output_path = mkstemp(dir=curr_proc.profile_output_dir)
        curr_proc.profile_output_path = output_path
        logger.warn('Enabling vmprof, output path: %s', output_path)
        vmprof.enable(fileno)
        curr_proc.vmprof_enabled = True


def script_main(script_class, cache_region, **script_kwargs):
    loop = script_kwargs.pop('loop', None)
    start_loop = script_kwargs.pop('start_loop', True)
    blt_default = script_kwargs.pop('blocking_log_threshold', 5)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='Path to config file',
                        required=True)

    parser.add_argument('-t', '--title',
                        help='Set the running process title',
                        default=script_kwargs.get('title', script_class.title))

    parser.add_argument('-l', '--log-handler',
                        help=("Specify which log handler to use. "
                              "These are defined in the config file for each "
                              "script."))

    blt = 'Logs a stack trace if the IOLoop is blocked for more than s seconds'
    parser.add_argument('--blocking-log-threshold',
                        help=blt, default=blt_default, type=int, metavar='s')

    parser.add_argument('--profile-output-dir',
                        help="Directory to write profile output to",
                        default=gettempdir())

    if script_class.args is not None:
        for pa, kw in script_class.args:
            parser.add_argument(*pa, **kw)

    pargs = parser.parse_args()

    curr_proc = multiprocessing.current_process()
    curr_proc.profile_output_dir = pargs.profile_output_dir
    if pargs.title is not None:
        curr_proc.name = pargs.title
        if '__pypy__' not in sys.builtin_module_names:
            from setproctitle import setproctitle
            setproctitle(pargs.title)

    # TODO: add signal handlers to drop caches
    signal.signal(signal.SIGUSR1, info_signal_handler)
    signal.signal(signal.SIGUSR2, vmprof_signal_handler)

    # Pop off kwargs not relevant to script class
    kwargs = copy(vars(pargs))
    kwargs['context'] = zmq.Context()
    kwargs['async'] = True
    if loop is None:
        loop = ioloop.IOLoop.instance()
    kwargs['loop'] = loop
    for a in ['blocking_log_threshold']:
        kwargs.pop(a)

    if cache_region is not None:
        configure_caching(cache_region, pargs.config)

    setup_logging(pargs.config, **kwargs)
    vassal = script_class(**kwargs)

    def hup_signal_handler(signal, frame):
        setup_logging(vassal.config_uri, incremental=True,
                      context=vassal.context, loop=vassal.loop, async=True)

    def term_signal_handler(signal, frame):
        vassal.logger.info("%s has received terminate signal",
                           script_class.__name__)
        root = logging.getLogger()
        for handler in root.handlers:
            if hasattr(handler, 'stream'):
                handler.stream.flush()
        vassal.terminate()

    signal.signal(signal.SIGHUP, hup_signal_handler)
    signal.signal(signal.SIGINT, term_signal_handler)
    signal.signal(signal.SIGTERM, term_signal_handler)

    if pargs.blocking_log_threshold > 0:
        vassal.loop.set_blocking_log_threshold(pargs.blocking_log_threshold)
    vassal.loop.add_callback(vassal.logger.info,
                             "%s has begun processing messages",
                             script_class.__name__)
    if start_loop:
        try:
            vassal.loop.start()
        except zmq.Again:
            print('Terminating with unsent messages')

    return vassal
