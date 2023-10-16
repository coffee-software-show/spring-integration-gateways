#!/usr/bin/env python3

import tempfile
import rmq
import json
import threading
import utils


def runner():
    requests_q = 'uppercase-requests'

    def handle_job(request):
        print(request)
        return {'reply': str.upper(request['request'])}

    while True:
        try:
            rmq.start_rabbitmq_processor(
                requests_q,
                ##replies_q,
                '127.0.0.1',
                'user',
                'password',
                '/',
                handle_job,
            )
        except Exception as ex:
            utils.exception(
                ex,
                message="There was some sort of error installing a RabbitMQ listener. Restarting the processor... ",
            )


if __name__ == "__main__":

    def run_rmq():

        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            try:
                retry_count += 1
                utils.log("launching RabbitMQ background thread")
                runner()
            except Exception as e:
                exception(
                    e,
                    message="something went wrong trying to start the RabbitMQ processing thread!",
                )

        log("Exhausted retry count of %s times." % max_retries)


    for f in [run_rmq]:
        threading.Thread(target=f).start()

    log("launched RabbitMQ and Flask threads.")
