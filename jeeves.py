"""
rvi-jeeves is a python script used from cron to place jobs on a rabbitmq bus.

It reads a config file that should be in the form of:

Configuration File Example
=============================
[serviceName]
rabbit_host = str
rabbit_user = str
rabbit_pass = str
rabbit_vhost = str
rabbit_port = 5672
rabbit_queue = str
rabbit_exchange = str
rabbit_routing_key = str
rabbit_message_body = {}
=============================

Configuration File Options
=============================
rabbit_host  -
    default: localhost
    type: String

rabbit_user  -
    default: guest
    type: String

rabbit_pass  -
    default: guest
    type: String

rabbit_vhost -
    default: /
    type: String

rabbit_port -
    default: 5672
    type: int

rabbit_queue -
    default: None
    type: String

rabbit_exchange -
    default: None
    type: String

rabbit_routing_key -
    default: None
    type: String

rabbit_message_body -
    default: None
    type: Any
    Notes:
        If you want to send JSON this should be a valid python dict.
        If you want to send a String it MUST BE in quotes
        Quotes are required here for a string because ast.literal_eval is used
Notes:
    All String values should stay unquoted unless otherwise noted.
    For example:
        "localhost" would parse into '"localhost"'
=============================

Configuration File Location
=============================
Your config file can be located anywhere. By default it will look for an
environment variable called PCOPS_PRIV.

That should be something like:
export PCOPS_PRIV="/home/automation/.priv_dir

It will then look for: $PCOPS_PRIV/rvi-jeeves.conf

If that location is not there and you do not set the file location using the
provided command line switches the application will fail.
=============================

General Usage
=============================

rvi-jeeves -f /home/automation/staging/rvi-jeeves.conf -s vConsuela

This is is meant to run from cron there is no stdout unless there is an error.

Currently only publishing to a basic queue is implemented.
"""

__author__ = "Michael Rice <michael@michaelrice.org>"
__licenses__ = "MIT"

import os
import ast
import pika
import json
import argparse
import ConfigParser
import logging

logging.getLogger('pika').setLevel(logging.ERROR)

def get_connection_info(config_inst, service_name):
    """
    config_inst should be a ConfigParser.ConfigParser().readfp instance
    This will extract the needed info from the config into a dict we can use
    to pass to our connection.

    service should be a String with the name of the service
    [serviceName]
    xxx = yyy
    """
    connection_info = {}
    try:
        host = config_inst.get(service_name, "rabbit_host")
    except ConfigParser.NoOptionError:
        host = "localhost"

    connection_info.update({"rabbit_host": host})
    try:
        user = config_inst.get(service_name, "rabbit_user")
    except ConfigParser.NoOptionError:
        user = "guest"

    connection_info.update({"rabbit_user": user})
    try:
        passwd = config_inst.get(service_name, "rabbit_pass")
    except ConfigParser.NoOptionError:
        passwd = "guest"

    connection_info.update({"rabbit_pass": passwd})
    try:
        vhost = config_inst.get(service_name, "rabbit_vhost")
    except ConfigParser.NoOptionError:
        vhost = "/"

    connection_info.update({"rabbit_vhost": vhost})
    try:
        port = int(config_inst.get(service_name, "rabbit_port"))
        connection_info.update({"rabbit_port": port})
    except ConfigParser.NoOptionError:
        connection_info.update({"rabbit_port": 5672})

    # if any of these arent set we cant set a default
    try:
        connection_info.update({"rabbit_queue":
                                config_inst.get(service_name, "rabbit_queue")})
    except ConfigParser.NoOptionError:
        connection_info.update({"rabbit_queue": None})

    try:
        connection_info.update({"rabbit_exchange":
                                config_inst.get(service_name, "rabbit_exchange")})
    except ConfigParser.NoOptionError:
        connection_info.update({"rabbit_exchange": None})

    try:
        connection_info.update({"rabbit_routing_key":
                                config_inst.get(service_name, "rabbit_routing_key")})
    except ConfigParser.NoOptionError:
        connection_info.update({"rabbit_routing_key": None})

    try:
        connection_info.update({"rabbit_message_body":
                                config_inst.get(service_name, "rabbit_message_body")})
    except ConfigParser.NoOptionError:
        raise SystemExit("rabbit_message_body parameter missing from config and is required.")

    return connection_info


def connect_to_bus(rabbit_connection_info=None):
    """
    Supported kwargs are:
    rabbit_host - default localhost
    rabbit_user - default guest
    rabbit_pass - default guest
    rabbit_vhost - default /
    rabbit_port  - default 5672

    This will connect to the bus and return a connection
    """
    rabbit_host = rabbit_connection_info.get("rabbit_host")
    rabbit_user = rabbit_connection_info.get("rabbit_user")
    rabbit_pass = rabbit_connection_info.get("rabbit_pass")
    rabbit_vhost = rabbit_connection_info.get("rabbit_vhost")
    rabbit_port = rabbit_connection_info.get("rabbit_port")
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    connection_params = pika.ConnectionParameters(host=rabbit_host,
                                                  virtual_host=rabbit_vhost,
                                                  credentials=credentials,
                                                  port=rabbit_port)
    return pika.BlockingConnection(connection_params)


def get_channel(connection=None):
    """
    Creates and returns a channel from a rabbit connection
    """
    return connection.channel()


def publish_basic(channel, connection_info):
    msg_body = ast.literal_eval(connection_info.get("rabbit_message_body"))
    if type(msg_body) is dict:
        msg_body = json.dumps(msg_body)
    channel.basic_publish(exchange="", routing_key=connection_info.get("rabbit_queue"), body=msg_body)

if __name__ == "__main__":

    CONF_FILE = str(os.getenv("PCOPS_PRIV")) + "/{0}".format("rvi-jeeves.conf")
    parser = argparse.ArgumentParser()
    help_msg = "Name of the service to use defined in the config file."
    parser.add_argument("-s", "--service", help=help_msg, required=True, type=str)
    parser.add_argument("-f", "--configfile", help="Full path to config file")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-b", "--basic", help="Use a basic queue to publish", action="store_true")
    args = parser.parse_args()
    if args.configfile:
        CONF_FILE = args.configfile
    config = ConfigParser.ConfigParser()
    try:
        config.readfp(open(CONF_FILE))
    except IOError:
        print "Config file missing."
        raise SystemExit("Please create {0} ".format(CONF_FILE))

    service = args.service
    connection_info = get_connection_info(config, service)
    rabbit_connection = connect_to_bus(connection_info)
    rabbit_channel = get_channel(connection=rabbit_connection)
    if args.basic:
        publish_basic(rabbit_channel, connection_info)
        rabbit_connection.close()
        exit()
