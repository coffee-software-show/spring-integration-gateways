#!/usr/bin/env python

import json
import os

import pika
import utils


def start_rabbitmq_processor(
        requests_q: str,
        rabbit_host: str,
        rabbit_username: str,
        rabbit_password: str,
        rabbit_vhost: str,
        process_job_requests_fn,
):
    utils.log(
        f"Establishing a connection to RabbitMQ host {rabbit_host}, having virtual host {rabbit_vhost}, with username {rabbit_username}.")

    if rabbit_vhost is not None:
        rabbit_vhost = rabbit_vhost.strip()
        if rabbit_vhost == "/" or rabbit_vhost == "":
            rabbit_vhost = None

    utils.log('setting heartbeat=0')

    if rabbit_vhost is None:
        utils.log('vhost is None')
        params = pika.ConnectionParameters(
            host=rabbit_host,
            credentials=pika.PlainCredentials(rabbit_username, rabbit_password),
            heartbeat=0,
        )
    else:
        utils.log('vhost is not None')
        params = pika.ConnectionParameters(
            host=rabbit_host,
            virtual_host=rabbit_vhost,
            credentials=pika.PlainCredentials(rabbit_username, rabbit_password),
            heartbeat=0
        )

    def debug_message(body, properties):
        debug_message = os.linesep.join([str(a) for a in [os.linesep, '-' * 50, properties, body, os.linesep]])
        utils.log(debug_message)

    with pika.BlockingConnection(params) as connection:
        with connection.channel() as channel:
            utils.log("connected.")
            for method_frame, properties, body in channel.consume(requests_q):
                debug_message(body, properties)
                replies_q = properties.reply_to
                object_request = json.loads(body)
                result = process_job_requests_fn(object_request)
                json_response = json.dumps(result)
                try:
                    #import time
                    #time.sleep(10) # sleeps 10s

                    channel.basic_publish(
                        '',
                        replies_q,  # properties.routing_key,
                        json_response,
                        pika.BasicProperties(
                            correlation_id=properties.correlation_id,
                            content_type="text/plain",
                            delivery_mode=1
                        )
                    )
                    channel.basic_ack(method_frame.delivery_tag)
                except:
                    print('there was an error')
