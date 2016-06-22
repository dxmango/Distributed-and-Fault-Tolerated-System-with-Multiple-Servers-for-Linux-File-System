#!/usr/bin/env python
import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import time
import datetime

from xmlrpclib import Binary
import sys, pickle, xmlrpclib
mediator = xmlrpclib.Server("http://127.0.0.1:51234")
dataserver1 = xmlrpclib.Server("http://127.0.0.1:51241")
dataserver2 = xmlrpclib.Server("http://127.0.0.1:51242")
dataserver3 = xmlrpclib.Server("http://127.0.0.1:51243")
dataserver4 = xmlrpclib.Server("http://127.0.0.1:51244")
dataserver5 = xmlrpclib.Server("http://127.0.0.1:51245")
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver1.list_contents()
  print "data in dataserver1 before restarting (dataserver1.list_contents()):"
  print value1
  dataserver1.restart0()
  value2 = dataserver1.list_contents()
  print "data in dataserver1 after restarting without restoring data (dataserver1.list_contents()):"
  print value2
  mediator.restart0()
  value3 = dataserver1.list_contents()
  print "data in dataserver1 after restarting with restoring data (dataserver1.list_contents()):"
  print value3
except:
  print "something wrong"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
'''
try:
  value1=dataserver2.list_contents()
  print "data in dataserver2 before restarting (dataserver2.list_contents()):"
  print value1
  dataserver2.restart0()
  value2 = dataserver2.list_contents()
  print "data in dataserver2 after restarting without restoring data (dataserver2.list_contents()):"
  print value2
  mediator.restart0()
  value3 = dataserver2.list_contents()
  print "data in dataserver2 after restarting with restoring data (dataserver2.list_contents()):"
  print value3
except:
  print "something wrong"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver3.list_contents()
  print "data in dataserver3 before restarting (dataserver3.list_contents()):"
  print value1
  dataserver3.restart0()
  value2 = dataserver3.list_contents()
  print "data in dataserver3 after restarting without restoring data (dataserver3.list_contents()):"
  print value2
  mediator.restart0()
  value3 = dataserver3.list_contents()
  print "data in dataserver3 after restarting with restoring data (dataserver3.list_contents()):"
  print value3
except:
  print "something wrong"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver4.list_contents()
  print "data in dataserver4 before restarting (dataserver4.list_contents()):"
  print value1
  dataserver4.restart0()
  value2 = dataserver4.list_contents()
  print "data in dataserver4 after restarting without restoring data (dataserver4.list_contents()):"
  print value2
  mediator.restart0()
  value3 = dataserver4.list_contents()
  print "data in dataserver4 after restarting with restoring data (dataserver4.list_contents()):"
  print value3
except:
  print "something wrong"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver5.list_contents()
  print "data in dataserver5 before restarting (dataserver5.list_contents()):"
  print value1
  dataserver5.restart0()
  value2 = dataserver5.list_contents()
  print "data in dataserver5 after restarting without restoring data (dataserver5.list_contents()):"
  print value2
  mediator.restart0()
  value3 = dataserver5.list_contents()
  print "data in dataserver5 after restarting with restoring data (dataserver5.list_contents()):"
  print value3
except:
  print "something wrong"'''
