#!/home/students/mbachows/python/bin/python
# -*- coding: utf-8 -*-
import time
import sys
from threading import Thread
import logging
import argparse
import subprocess

import zmq

def worker(context, sockSync, sockWatchdog):
    sync = context.socket(zmq.REQ)
    sync.connect(sockSync)

    sync.send('hello')
    id = sync.recv()
    logger = logging.getLogger('runner.%s'%id)
    logger.debug('ready')
    watchdog = context.socket(zmq.REP)
    watchdog.setsockopt(zmq.IDENTITY, id)
    watchdog.connect(sockWatchdog)

    while True:
        if 'stop' == watchdog.recv():
            logger.debug('received stop signal')
            break
        command = watchdog.recv()
        logger.debug('received job to do: %s'%command)
        logger.debug('running')
        try:
            subprocess.check_call(command, stdin=subprocess.PIPE, \
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        except Exception, e:
            logger.error(str(e))
        watchdog.send(str(time.time()))
        logger.debug('job done')
        time.sleep(1)

def main(nmbr, sockSync, sockWatchdog):
    context = zmq.Context()

    #worker(context, sockSync, sockWatchdog)
    #return
    for i in range(nmbr):
        Thread(target=worker, args=(context, sockSync, sockWatchdog)).start()


if __name__ == '__main__':
    # input parser
    parser = argparse.ArgumentParser(description='Job runner')
    # basic configuration
    ru = parser.add_argument_group('Runner', 'Arguments for job runner')
    ru.add_argument('-n', '--number', dest='count', default=1, \
        help='Number of threads to start' )
    ru.add_argument('-o', '--output', default=None, dest='logfilename', \
        help='Name of file where to write logs')
    ru.add_argument('-s', '--sync-socket', dest='sync_socket',
        default='tcp://*:5562', help='ZMQ socket string to connect "sync" queue to')
    ru.add_argument('-w', '--watchdog-socket', dest='watchdog_socket',
        default='tcp://*:5561', help='ZMQ socket string to connect "watchdog" queue to')
    ru.add_argument('-v', '--verbose', action='store_const', dest='verbose', \
        const=True, default=False, help='Display startup messages')
    ru.add_argument('--version', action='version', version='%(prog)s 1.0', \
        help='Prints version')
    
    args = parser.parse_args()
    # configure logger
    logger = logging.getLogger('runner')
    if args.logfilename is None:
        loghandler = logging.StreamHandler()
    else:
        loghandler = logging.FileHandler(args.logfilename, 'a+')
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    loghandler.setFormatter(formatter)
    logger.addHandler(loghandler)
    # verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logging.getLogger('').setLevel(logging.ERROR)
    # run
    main(\
        int(args.count), \
        args.sync_socket, \
        args.watchdog_socket)

