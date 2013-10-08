#!/usr/bin/env python

import pika
import logging
import sys


logger = logging.getLogger(__name__)
logger.setLevel( logging.INFO )
logger.setLevel( logging.DEBUG )


def main( argv ):
    if len(argv) != 2:
        logging.warning("Usage: %s <deviceName>" % (argv[0],))
        return 1

    device = argv[1]

    connection = pika.BlockingConnection( pika.ConnectionParameters( host='localhost' ) )
    channel = connection.channel()

    def callback(ch, method, properties, body):
        print " [x] Received %r" % (body,)

    channel.basic_consume(callback,
                          queue=device,
                          no_ack=True)

    print 'Waiting for messages ...'
    channel.start_consuming()

    
if __name__ == "__main__":
    sys.exit( main( sys.argv ) )
