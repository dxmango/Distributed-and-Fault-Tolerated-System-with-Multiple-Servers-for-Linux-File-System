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
controlserver = xmlrpclib.Server("http://127.0.0.1:51234")
dataserver1 = xmlrpclib.Server("http://127.0.0.1:51241")
dataserver2 = xmlrpclib.Server("http://127.0.0.1:51242")
dataserver3 = xmlrpclib.Server("http://127.0.0.1:51243")
dataserver4 = xmlrpclib.Server("http://127.0.0.1:51244")
dataserver5 = xmlrpclib.Server("http://127.0.0.1:51245")

print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver1.list_contents()
  print "data in dataserver1 before crashing (dataserver1.list_contents()):"
  print value1
  dataserver1.terminate()
  value2=dataserver1.list_contents()
  print "dataserver1.list_contents()"
  print value2
except:
  print "server1 crashes"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver2.list_contents()
  print "data in dataserver2 before crashing (dataserver2.list_contents()):"
  print value1
  dataserver2.terminate()
  value2=dataserver2.list_contents()
  print "dataserver2.list_contents()"
  print value2
except:
  print "server2 crashes"
'''
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver3.list_contents()
  print "data in dataserver3 before crashing (dataserver3.list_contents()):"
  print value1
  dataserver3.terminate()
  value2=dataserver3.list_contents()
  print "dataserver3.list_contents()"
  print value2
except:
  print "server3 crashes"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver4.list_contents()
  print "data in dataserver4 before crashing (dataserver4.list_contents()):"
  print value1
  dataserver4.terminate()
  value2=dataserver4.list_contents()
  print "dataserver4.list_contents()"
  print value2
except:
  print "server4 crashes"
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
try:
  value1=dataserver5.list_contents()
  print "data in dataserver5 before crashing (dataserver5.list_contents()):"
  print value1
  dataserver5.terminate()
  value2=dataserver5.list_contents()
  print "dataserver5.list_contents()"
  print value2
except:
  print "server5 crashes"'''
