#!/usr/bin/env python

import pika
import logging
import sys


def main( argv ):
    if len(argv) != 3:
        logging.warning("Usage: %s <device> <msg>" % (argv[0],))
        return 1

    device = argv[1]
    msg =  argv[2]
        
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    properties=pika.BasicProperties( expiration=str(30*1000) )
    channel.basic_publish(exchange='fluffyExch',
                          routing_key=device,
                          body=msg,
                          properties=properties 
                        )

    logging.info( "%s <- %s"%( device , msg ) )   # 
    connection.close()
    return 0

    
if __name__ == "__main__":
    sys.exit( main( sys.argv ) )

    
