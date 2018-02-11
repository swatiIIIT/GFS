import rpyc
import uuid
import threading
import math
import random
import ConfigParser
import signal
import pickle
import sys
import os
import random
from socket import *
import socket

import datetime as dt
from threading import Thread

from rpyc.utils.server import ThreadedServer

FILE_DIR="./Files/"
activeChunk = {}


def int_handler(signal, frame):
  pickle.dump((MasterService.exposed_Master.file_table,MasterService.exposed_Master.block_mapping),open('fs.img','wb'))
  sys.exit(0)


class MasterService(rpyc.Service, threading.Thread):
  class exposed_Master():
    file_table = {}
    block_mapping = {}
    block_size = 1048576
    replication_factor = 2

    def __init__(self):
      output = open(FILE_DIR+'files.txt','rb')
      if os.path.getsize(FILE_DIR+'files.txt')>0 : 
        self.__class__.file_table = pickle.load(output) 
      output.close()

    def exposed_read(self,fname):
      mapping = self.__class__.file_table[fname]
      return mapping

    def exposed_write(self,dest,size):
      self.__class__.file_table[dest]=[]

      num_blocks = self.calc_num_blocks(size)
      blocks = self.alloc_blocks(dest,num_blocks)
      output = open(FILE_DIR+'files.txt', 'w')
      pickle.dump(self.__class__.file_table, output)
      output.close()
      return blocks

    def exposed_delete_file_table_entry(self,file_entry,fname):
      for block in file_entry:
        for m in [self.get_chunk()[_] for _ in block[1]]:
          self.delete_from_chunkserver(block[0],m)

      del self.__class__.file_table[fname]
      output = open(FILE_DIR+'files.txt', 'w')
      pickle.dump(self.__class__.file_table, output)
      output.close()

    def exposed_get_file_table_entry(self,fname):
      if fname in self.__class__.file_table:
        return self.__class__.file_table[fname]
      else:
        return None

    def exposed_get_file_table(self):
      return self.__class__.file_table

    def delete_from_chunkserver(self,block_uuid,chunkserver):
      host,port = chunkserver
      try:
        con=rpyc.connect(host,port=port)
        chunk = con.root.ChunkServer()
      except Exception as e:
        print "Chunk Server is down"
        exit()
      chunk.delete(block_uuid)

    def exposed_get_block_size(self):
      return self.__class__.block_size

    def exposed_get_chunkservers(self):
      return activeChunk

    def get_chunk(self):
      return activeChunk

    def calc_num_blocks(self,size):
      return int(math.ceil(float(size)/self.__class__.block_size))

    def exists(self,file):
      return file in self.__class__.file_table

    def alloc_blocks(self,dest,num):
      blocks = []
      for i in range(0,num):
        block_uuid = uuid.uuid1()
        nodes_ids = random.sample(activeChunk.keys(),self.__class__.replication_factor)
        blocks.append((block_uuid,nodes_ids))
        self.__class__.file_table[dest].append((block_uuid,nodes_ids))
      return blocks


######################################################################################

class Heartbeat(Thread):

    def __init__(self):
        Thread.__init__(self)



    def run(self):
        global activeChunk
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        server_start = dt.datetime.now()
        flag = True
        port = 60001
        while flag:
            try:
                ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                ss.bind(('10.2.7.55', port))

                server_start = dt.datetime.now()
                print 'Serving on host at port ',port
                flag = False

            except IOError as e:
                print 'Cannot bind server to port'
                print e
                port+=1
                exit()


            iptable = dict()

            packets_lost = dict()
        while True:

            try:
                # If no client pings the server for a whole minute, we have to assume that all clients are closed
                ss.settimeout(15)
                rand = random.randint(0, 100)
                message, address = ss.recvfrom(1024)
                message_list = message.split('\t')
                IP = address[0]
                port = address[1]
                message = dt.datetime.strptime(message_list[1],"%Y-%m-%d %H:%M:%S.%f")

                try:

                    if not IP in activeChunk:
                        activeChunk[IP] = [0,0]
                        activeChunk[IP][0] = IP
                        activeChunk[IP][1] = str(port-1)


                    for i in activeChunk :
                      try:
                        ss.sendto("reply  ",activeChunk[i][0])

                      except socket.error as e:
                        print " could connect ",e

                    # If the client is pinging the server for the first time, create a key for its address
                    if not address in iptable:
                        iptable[address] = [0,0,0]
                        packets_lost[address] = 0

                    iptable[address][0] += 1

                    print 'Received packet no',iptable[address][0],'from',address,'.',
                    print 'The sequence number sent with the packet was',message_list[0]
                    now = dt.datetime.now()

                    # Delay between the time at which the client sent the packet and the time server received it
                    response = now - message

                    # Timestamp of most recent ping from the client
                    iptable[address][2] = now
                    packets_lost[address] = 0


                except TypeError:
                    print 'Client at',address, 'alive'


            except Exception as e:

                if type(e) == timeout:
                    print "NOTE: It's been a whole minute since the last client pinged the server."
                else:
                    print e


class Test(Thread):
    def __init__(self   ):
        Thread.__init__(self)

    def run(self):
        t = ThreadedServer(MasterService, port = 60000)
        t.start()

##########################################################################################


if __name__ == "__main__":
  if not os.path.isdir(FILE_DIR):
    os.mkdir(FILE_DIR)
    output = open(FILE_DIR+'files.txt', 'w')
  signal.signal(signal.SIGINT,int_handler)
  threadedserverobj = Test()
  hbeatobj = Heartbeat()
  hbeatobj.start()
  threadedserverobj.start()
  hbeatobj.join()

  ss.close()
