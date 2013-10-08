#!/usr/bin/env python

import json 
import tornado.ioloop
import tornado.web
import tornado.template
from tornado import websocket
from pika import adapters
import pika
import logging
import redis
from time import time
from datetime import timedelta

logger = logging.getLogger(__name__)
logger.setLevel( logging.INFO )
logger.setLevel( logging.DEBUG )

# TODO - setup qos for low prefetch 

class MainHTML(tornado.web.RequestHandler):
    def get(self):
        logger.info( "HTML redirect root page"  )
        self.redirect( "/test/alice" );

class TestHTML(tornado.web.RequestHandler):
    def get(self,user):
        logger.info( "Test HTML for user " + user )
        self.render( "client.html" , user=user );

class PubServer(tornado.web.RequestHandler):
    def post(self,user):
        msg = self.request.body
        logger.info( "Post request user=%s msg=%s " % ( user, msg ) )

        cacheId = self.application.redis.get( "online-epoch1-"+user ) 
        if not cacheId:
            # TODO - put code here to send to Urban Airship
            pass

        self.application.pc.publish( user, msg )
        
        response = {}
        response['ok'] = True
        self.write(response)
        
        
class WebSocketServer(tornado.websocket.WebSocketHandler):
    def open(self, user):
        self.user = user
        self.ackTag = 0
        self.id = int( time()*1000.0 )
        logger.info( "WebSocket opened for user " + user )
        self.application.pc.add_event_listener( self, self.user, self.id )
        self.application.redis.setex( "online-epoch1-"+self.user, timedelta( seconds=10*60 ), self.id ) 
        logger.info( "Online %s/%s " % ( self.user, self.id ) )

    def on_message(self, message):
        self.application.redis.setex( "online-epoch1-"+self.user, timedelta( seconds=10*60 ), self.id ) 
        # TODO need to catch a bunch of exceptions around here 
        obj = json.loads( message )
        logger.info( "ACK:" + str( obj['ack'] ) )
        if  obj['ack'] != 0:
            self.ackTag = 0
            self.application.pc.do_ack( obj['ack'] )

    def on_close(self):
        self.application.pc.remove_event_listener( self, self.id, self.ackTag )
        logger.info( "WebSocket closed" )
        # TODO - make next 3 lines atomic with LUA 
        cacheId = self.application.redis.get( "online-epoch1-"+self.user ) 
        if cacheId:
            if int(self.id) == int(cacheId):
                self.application.redis.delete( "online-epoch1-"+self.user )
                logger.info( "Offline %s/%s " % ( self.user, self.id ) )

    def on_event(self, message, ackTag ):
        self.ackTag = ackTag
        self.write_message( json.dumps(  { 'body':message, 'ackTag':ackTag }  ) )        

        
class MessageConsumer(object):
 
    def __init__(self, io_loop):
        logger.info('MessageConsumer: __init__')
        self.io_loop = io_loop
 
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
 
        self.event_listeners = set([])
 
    def connect(self):
        if self.connecting:
            return
        logger.info('MessageConsumer: Connecting to RabbitMQ')
        self.connecting = True
 
        cred = pika.PlainCredentials('guest', 'guest')
        param = pika.ConnectionParameters( host='localhost', port=5672, virtual_host='/', credentials=cred )
        self.connection = adapters.TornadoConnection(param,
            on_open_callback=self.on_connected)
        self.connection.add_on_close_callback(self.on_closed)
 
    def on_connected(self, connection):
        logger.info('MessageConsumer: connected to RabbitMQ')
        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)
 
    def on_channel_open(self, channel):
        logger.info('MessageConsumer: Channel open')
        self.channel = channel
        
    def on_closed(self, connection):
        logger.info('MessageConsumer: rabbit connection closed')
        self.io_loop.stop()

    def publish( self, user, msg):
        logger.info('MessageConsumer: publish')
        properties=pika.BasicProperties( expiration=str(30*1000) ) # time in ms 
        self.channel.basic_publish( exchange='fluffyExch', routing_key=user, properties=properties, body=msg );
        
    def on_message(self, channel, method, header, body):
        logger.info('MessageConsumer: message received: %s' % body)
        logger.info('MessageConsumer: message received: method = %s' % str(method) )
        logger.info('MessageConsumer: message received: header = %s' % str(header) )
        self.notify_listeners( body, method.delivery_tag , user=method.routing_key )

    def do_ack(self, tag ):
        self.channel.basic_ack( delivery_tag = tag );
 
    def notify_listeners(self, msg, ackTag, user ):
        for listener in self.event_listeners:
            if listener.user == user :
                listener.on_event( msg, ackTag )
                logger.info('MessageConsumer: notified %s' % repr(listener) )
 
    def add_event_listener(self, listener, user, consumerTag ):
        self.event_listeners.add(listener)
        logger.info('MessageConsumer: listener %s added' % repr(listener))
        self.consumer_tag = self.channel.basic_consume( self.on_message,
                                                        queue=user,
                                                        consumer_tag= str(consumerTag) )
                
    def remove_event_listener(self, listener, consumerTag, ackTag ):
        try:
            self.event_listeners.remove(listener)
            logger.info('MessageConsumer: listener %s for consumerTag %s removed' %( repr(listener), consumerTag) )
            self.channel.basic_cancel( consumer_tag= str(consumerTag), nowait=True )
            if ackTag != 0 :
                self.channel.basic_nack( delivery_tag=ackTag, multiple=True, requeue=True )
        except KeyError:
            pass


application = tornado.web.Application([
    (r"/test/(.*)", TestHTML),
    (r"/sub/(.*)", WebSocketServer),
    (r"/pub/(.*)", PubServer),
    (r"/", MainHTML),
])


def main():
    io_loop = tornado.ioloop.IOLoop.instance()

    application.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    application.pc = MessageConsumer(io_loop)
    application.pc.connect()
 
    application.listen(8888)
    io_loop.start()


if __name__ == "__main__":
    main()
