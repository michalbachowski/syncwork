#!/home/students/mbachows/python/bin/python
# -*- coding: utf-8 -*-
import time
import sys
import logging
import argparse

import zmq

def main(nmbr, command, sockSync, sockWatchdog):
    logger = logging.getLogger('server')
    # Prepare our context and sockets
    context = zmq.Context()
    
    sync = context.socket(zmq.REP)
    sync.bind(sockSync)

    watchdog = context.socket(zmq.XREQ)
    watchdog.bind(sockWatchdog)

    # sync
    logger.debug('syncing')
    for i in range(nmbr):
        sync.recv()
        logger.debug('+ worker')
        sync.send(str(i))
    logger.debug('ready')
    time.sleep(1)
    while True:
        # start
        start = time.time()
        logger.debug('start job')
        for i in range(nmbr):
            watchdog.send(str(i), zmq.SNDMORE)
            watchdog.send('', zmq.SNDMORE)
            watchdog.send('start', zmq.SNDMORE)
            watchdog.send(command)

        # receive
        logger.debug('awaiting responses')
        stop = time.time()+24*60*60
        # receive responses
        for i in range(nmbr):
            id = watchdog.recv()
            # dummy
            watchdog.recv()
            tmp = float(watchdog.recv())
            logger.debug('response from runner %s received'%id)
            if tmp < stop:
                stop = tmp
            # end
        logger.debug('job finished')
        print("%u %f" % (nmbr, stop - start))
        time.sleep(1)
        break

    # send stop signal to workers
    logger.debug('stopping runners')
    for i in range(nmbr):
        watchdog.send(str(i),zmq.SNDMORE)
        watchdog.send('',zmq.SNDMORE)
        watchdog.send('stop')
    time.sleep(1)
    logger.debug('done')

if __name__ == '__main__':
    # input parser
    parser = argparse.ArgumentParser(description='Job server')
    # basic configuration
    ru = parser.add_argument_group('Server', 'Arguments for job server')
    ru.add_argument('command', metavar='COMMAND', help='Command to pass to runners')
    ru.add_argument('-n', '--number', dest='count', default=1, \
        help='Number of workers to synchronize with' )
    ru.add_argument('-o', '--output', default=None, dest='logfilename', \
        help='Name of file where to write logs')
    ru.add_argument('-s', '--sync-socket', dest='sync_socket',
        default='tcp://*:5562', help='ZMQ socket string to bind "sync" queue to')
    ru.add_argument('-w', '--watchdog-socket', dest='watchdog_socket',
        default='tcp://*:5561', help='ZMQ socket string to bind "watchdog" queue to')
    ru.add_argument('-v', '--verbose', action='store_const', dest='verbose', \
        const=True, default=False, help='Display startup messages')
    ru.add_argument('--version', action='version', version='%(prog)s 1.0', \
        help='Prints version')

    args = parser.parse_args()
    # configure logger
    logger = logging.getLogger('server')
    if args.logfilename is None:
        logger.addHandler(logging.StreamHandler())
    else:
        filelogger = logging.FileHandler(args.logfilename)
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        filelogger.setFormatter(formatter)
        logger.addHandler(filelogger)
    # verbosity
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logging.getLogger('').setLevel(logging.ERROR)
    # run
    main(
        int(args.count), \
        args.command,
        args.sync_socket, \
        args.watchdog_socket)

