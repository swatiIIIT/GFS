import rpyc
import uuid
import os
from threading import Thread
import random
import socket
import datetime as dt
import time

from rpyc.utils.server import ThreadedServer

DATA_DIR="./chunkserver/"

class ChunkServerService(rpyc.Service):
  class exposed_ChunkServer():
    blocks = {}

    def exposed_put(self,block_uuid,data,chunkservers):
      with open(DATA_DIR+str(block_uuid),'w') as f:
        f.write(data)
      if len(chunkservers)>0:
        self.forward(block_uuid,data,chunkservers)


    def exposed_get(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      with open(block_addr) as f:
        return f.read()

    def exposed_delete(self,block_uuid):
      block_addr=DATA_DIR+str(block_uuid)
      if not os.path.isfile(block_addr):
        return None
      os.remove(block_addr)

    def forward(self,block_uuid,data,chunkservers):
      print "forwaring to:"
      print block_uuid, chunkservers
      chunkserver=chunkservers[0]
      chunkservers=chunkservers[1:]
      host,port=chunkserver
      try :
        con=rpyc.connect(host,port=port)
        chunkserver = con.root.ChunkServer()
        chunkserver.put(block_uuid,data,chunkservers)
      except Exception as e:
        print "Chunk Server is down"
        exit()

######################################################################

class Heartbeat(Thread):
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

        Thread.__init__(self)



    def run(self):
        flag = True
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        port = 50001
        while flag:
            try:
                clientSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                clientSocket.bind(('10.2.7.55', port))
                print 'Client bound to port', port
                flag = False

            except IOError:
                print 'Cannot bind client to port',port,'. Trying with the next one...'
                port+=1

            except OverflowError:
                port = 1024
            scount = 0
            rcount = 0
            seq = 1

        while True:

            try:
                now = dt.datetime.now()
                clientSocket.settimeout(1)
                clientSocket.sendto(( str(seq) + '\t' + str(now) ), (self.ip,self.port))
  
                scount+=1
                seq+=1
                time.sleep(5)

            except TypeError:
                print "Server sent an unexpected response"
                break

            except Exception as e:
                print e


class Test(Thread):
    def __init__(self   ):
        Thread.__init__(self)

    def run(self):
        t = ThreadedServer(ChunkServerService, port = 50000)
        t.start()



#########################################################################

if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  threadedserverobj = Test()
  hbeatobj = Heartbeat('10.2.7.55',60001)
  threadedserverobj.start()
  hbeatobj.start()
  hbeatobj.join()
  clientSocket.close()
