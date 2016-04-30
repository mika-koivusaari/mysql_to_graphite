#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
#


from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    DeleteRowsEvent,
    UpdateRowsEvent
)
from ConfigParser import SafeConfigParser
from ConfigParser import NoSectionError
import MySQLdb
import socket
import calendar
import time
from datetime import datetime
from sys import exit

MYSQL_SETTINGS = None

CARBON_SERVER = None
CARBON_PORT = None

_EPOCH = datetime(1970, 1, 1)

repLogFile = None
repLogPosition = None
repLogConfig = SafeConfigParser()

graphiteConfHost = None
graphiteConfUser = None
graphiteConfPasswd = None
graphiteConfDb = None
repHost = None
repPort = None
repUser = None
repPasswd = None
config = SafeConfigParser()

def readGraphiteConfig():
  global graphiteConfHost
  global graphiteConfUser
  global graphiteConfPasswd 
  global graphiteConfDb

  db = MySQLdb.connect(host=graphiteConfHost, user=graphiteConfUser, passwd=graphiteConfPasswd, db=graphiteConfDb)
  cursor = db.cursor()

  cursor.execute("select sensorid,graphitepath,formula from graphite")
  # get the resultset as a tuple
  result = cursor.fetchall()
  graphiteConfig = {}
  for record in result:
    rec = dict({'graphitepath':record[1],'formula':record[2]})
    graphiteConfig[record[0]]=rec

  cursor.close()
  db.close()
  print "Read graphite conf."
  return graphiteConfig

def main():
  global repLogFile
  global repLogPosition
  global repLogConfig

  graphiteConfig = readGraphiteConfig()

  try:
    print "alku"
    sock = socket.socket()
    sock.connect((CARBON_SERVER, CARBON_PORT))
    print 'Carbon socket opened.'
    print 'replicationlogfile', repLogFile
    print 'replicationlogposition', repLogPosition
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=2,
        only_events=[WriteRowsEvent,DeleteRowsEvent,UpdateRowsEvent],
        blocking=True,
        log_file=repLogFile,
        log_pos=repLogPosition,
        resume_stream=False if repLogPosition==None else True)
    print "stream avattu"

    for binlogevent in stream:
#      print stream.log_file, stream.log_pos
      repLogFile = stream.log_file
      repLogPosition = stream.log_pos
#      print "%s:%s:" % (binlogevent.schema, binlogevent.table)
      if binlogevent.schema == "weather" and binlogevent.table == "graphite":
        graphiteConfig = readGraphiteConfig()
      if binlogevent.schema == "weather" and binlogevent.table == "data":
#      prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)

        for row in binlogevent.rows:
          if isinstance(binlogevent, WriteRowsEvent):
            vals = row["values"]
            if vals["sensorid"] in graphiteConfig:
              conf = graphiteConfig[vals["sensorid"]]
              value = float(vals["value"])
              if conf["formula"]!=None and conf["formula"]!="":
                value=eval(conf["formula"], {"__builtins__": {}}, {"value":value,"round":round})
              #path value unix_timestamp
              #print type(vals["time"])
              #print dir(vals["time"])
              #message = '%s %f %s\n' % (conf["graphitepath"], value, calendar.timegm(vals["time"].timetuple()))
              #_time = vals["time"]
              #print _time.hour, _time.tzinfo
              #message = '%s %f %s\n' % (conf["graphitepath"], value, time.mktime((_time.year, _time.month, _time.day, _time.hour, _time.minute, _time.second, -1, -1, -1)))
              message = '%s %f %d\n' % (conf["graphitepath"], value, round((vals["time"] - _EPOCH).total_seconds()))
              sock.sendall(message)
              #print message
              #print type(vals["time"])
              print str(vals["sensorid"]), str(vals["time"]), str(value)
              print message

  except KeyboardInterrupt:
    stream.close()
    sock.close()
    repLogConfig.set('replicationlog','file',repLogFile)
    repLogConfig.set('replicationlog','position',str(repLogPosition))
    with open('replogposition.ini', 'w') as f:
      repLogConfig.write(f)


if __name__ == "__main__":
    repLogConfig.read('replogposition.ini')
    try:
      repLogFile=repLogConfig.get('replicationlog','file')
      repLogPosition=repLogConfig.getint('replicationlog','position')
    except NoSectionError:
      repLogConfig.add_section('replicationlog');
    print 'replicationlogfile', repLogFile
    print 'replicationlogposition', repLogPosition
    config.read('mysql_to_graphite.ini')
    try:
      graphiteConfHost = config.get('graphite_config','host')
      graphiteConfUser = config.get('graphite_config','user')
      graphiteConfPasswd = config.get('graphite_config','passwd')
      graphiteConfDb = config.get('graphite_config','db')
      repHost = config.get('replication_connection','host')
      repPort = config.getint('replication_connection','port')
      repUser = config.get('replication_connection','user')
      repPasswd = config.get('replication_connection','passwd')
      MYSQL_SETTINGS = {
        "host": repHost,
        "port": repPort,
        "user": repUser,
        "passwd": repPasswd
      }
      CARBON_SERVER = config.get('carbon','host')
      CARBON_PORT = config.getint('carbon','port')
    except NoSectionError:
      print 'Error in mysql_to_graphite.ini'
      exit()
    main()

