import datetime
import logging
import multiprocessing
import os
import resource
import shutil
import types
import typing


def process(fn: types.FunctionType, id_str: str):
    logging.info(f"before {id_str} @ {datetime.datetime.now()}")
    proc = multiprocessing.Process(target=fn)
    proc.start()
    logging.info(f"after {id_str} @ {datetime.datetime.now()}")
    return proc

def load_config(file) -> typing.Dict[str, str]:
    import json

    with open(file) as fp:
        return json.loads(fp.read())

def parse_uri(uri) -> typing.Dict[str, str]:
    ' parses a url of the form rmq://user:pw@127.0.0.1/vhost '

    print('parsing', uri)
    import urllib.parse

    parsed_uri = urllib.parse.urlparse(uri)
    u, p = parsed_uri.netloc.split("@")[0].split(":")
    h = parsed_uri.netloc.split("@")[1]
    host = None
    port = None
    if ":" in h:
        host, port = h.split(":")
    else:
        host = h

    res: typing.Dict[str, object] = {
        "scheme": str(parsed_uri.scheme),
        "path": parsed_uri.path[1:],
        "username": u,
        "password": p,
        "port": port,
        "host": host,
    }
    return res


def is_linux() -> bool:
    import platform

    if platform.system().lower() == "linux":
        return True
    return False


def limit_memory():
    if not is_linux():
        return

    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    memory: int = get_memory()
    log("there is %s memory" % memory)
    resource.setrlimit(resource.RLIMIT_AS, (int(memory / 4), hard))


def get_memory() -> int:
    if not is_linux():
        return -1

    with open("/proc/meminfo", "r") as mem:
        free_memory = 0
        for i in mem:
            log(i)
            split_line = i.split()
            if str(split_line[0]) in "MemFree:":
                free_memory += int(split_line[1]) / 1024  # kB
    return free_memory


def exception(e: BaseException, message=None):
    assert e is not None, "the exception must not be None"
    if message is not None:
        log(message)
    log(str(type(e)))
    logging.exception(e)


configured_logging = False


def log(s: object):
    from logging import getLogger, Formatter, StreamHandler

    global configured_logging
    if not configured_logging:
        the_logger = getLogger()
        the_logger.setLevel(logging.INFO)
        the_formatter = Formatter(
            "%(asctime)s [%(threadName)s] [%(levelname)s] %(name)s: %(message)s  "
        )
        console_handler = StreamHandler()
        console_handler.setFormatter(the_formatter)
        the_logger.addHandler(console_handler)
        configured_logging = True
    logging.info(s)


def require_env_variable(k) -> str:
    assert k in os.environ, 'the environment variable "%s" does not exist.' % k
    return os.environ[k]


def reset_and_recreate_directory(d):
    if os.path.exists(d):
        shutil.rmtree(d, ignore_errors=True, onerror=None)
    os.makedirs(d)
    assert os.path.exists(d), "the directory %s does not exist" % d
