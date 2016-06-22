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

print "+++++++++++++++++++++++++++++++++++++"
print "NOTE: please make sure you have created a 'a.txt'or writen something in 'a.txt' file in fusemount before run this test_corrupted.py"
print "+++++++++++++++++++++++++++++++++++++"

# corrup data in data_server1
value1=dataserver1.list_contents()
print "data in data_sever1 before corrupted (dataserver1.list_contents()):"
print value1
dataserver1.corrupt("/a.txt&&data")
value2=dataserver1.list_contents()
print "data in data_sever1 after corrupted (dataserver1.list_contents()):"
print value2
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
# corrup data in data_server2
value1=dataserver2.list_contents()
print "data in data_sever2 before corrupted (dataserver2.list_contents()):"
print value1
dataserver2.corrupt("/a.txt&&data")
value2=dataserver2.list_contents()
print "data in data_sever2 after corrupted (dataserver2.list_contents()):"
print value2

'''
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
# corrup data in data_server3
value1=dataserver3.list_contents()
print "data in data_sever3 before corrupted (dataserver3.list_contents()):"
print value1
dataserver3.corrupt("/a.txt&&data")
value2=dataserver3.list_contents()
print "data in data_sever3 after corrupted (dataserver3.list_contents()):"
print value2
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"

# corrup data in data_server4
value1=dataserver4.list_contents()
print "data in data_sever4 before corrupted (dataserver4.list_contents()):"
print value1
dataserver4.corrupt("/a.txt&&data")
value2=dataserver4.list_contents()
print "data in data_sever4 after corrupted (dataserver4.list_contents()):"
print value2
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
# corrup data in data_server5
value1=dataserver5.list_contents()
print "data in data_sever5 before corrupted (dataserver5.list_contents()):"
print value1
dataserver5.corrupt("/a.txt&&data")
value2=dataserver5.list_contents()
print "data in data_sever5 after corrupted (dataserver5.list_contents()):"
print value2'''

