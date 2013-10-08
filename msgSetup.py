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

    channel.exchange_declare(exchange='fluffyExch',
                             type='direct')
      
    channel.queue_declare(queue=device,durable=True)

    channel.queue_bind(exchange='fluffyExch',
                       queue=device,
                       routing_key=device)

    return 0


if __name__ == "__main__":
    sys.exit( main( sys.argv ) )

    
