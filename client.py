import rpyc
import sys
import os
import time

DATA_DIR="./Recieved_Data/"
LOG_DIR="./Logs/"

def send_to_chunkserver(block_uuid,data,chunkservers):
  print "sending: " + str(block_uuid) + str(chunkservers)
  chunkserver = chunkservers[0]
  chunkservers=chunkservers[1:]
  host=chunkserver[0]
  port=chunkserver[1]
  try:
    con=rpyc.connect(host,port=port)
    chunkserver = con.root.ChunkServer()
    chunkserver.put(block_uuid,data,chunkservers)
  except Exception as e:
    print "Chunk Server is down"
    exit()


def read_from_chunkserver(block_uuid,chunkserver):
  host,port = chunkserver
  try:
    con=rpyc.connect(host,port=port)
    chunkserver = con.root.ChunkServer()
  except Exception as e:
    print "Chunk Server is down"
    exit()
  return chunkserver.get(block_uuid)

def get(master,fname):
  file_table = master.get_file_table_entry(fname)
  if not file_table:
    print "404: file not found"
    return

  for block in file_table:
    for m in [master.get_chunkservers()[_] for _ in block[1]]:
      data = read_from_chunkserver(block[0],m)
      if data:
        with open(DATA_DIR+fname, 'ab') as f:
        	print 'file opened'
        	f.write(data);
        	f.close();
        break
    else:
        print "No blocks found. Possibly a corrupt file"

def put(master,source,dest):
  file_table = master.get_file_table()
  keys = list(file_table)

  if dest in keys:
  	print "Please provide unique Alias name as it already exists "
  	return
  size = os.path.getsize(source)
  blocks = master.write(dest,size)
  with open(source) as f:
    for b in blocks:
      data = f.read(master.get_block_size())
      block_uuid=b[0]
      chunkservers = [master.get_chunkservers()[_] for _ in b[1]]
      send_to_chunkserver(block_uuid,data,chunkservers)

def delete(master,fname):
  file_table = master.get_file_table_entry(fname)
  if not file_table:
    print "404: file not found"
    return
  else:
  	print master
  	master.delete_file_table_entry(file_table,fname)
  


def main(args):
  con=rpyc.connect("10.2.7.55",port=60000)
  master=con.root.Master()
  start_time=time.strftime("%x")
  stime=start_time.replace('/','_')
  filename=LOG_DIR+stime+".txt"

  if os.path.exists(filename):
    append_write = 'ab'
  else:
    append_write = 'w'

  if args[0] == "get":
    data="get at "+time.strftime("%c")+"\n"
    with open(filename, append_write) as f:
      f.write(data);
      f.close();
    get(master,args[1])
  elif args[0] == "put":
    data="put at "+time.strftime("%c")+"\n"
    with open(filename, append_write) as f:
        f.write(data);
        f.close();
    put(master,args[1],args[2])
  elif args[0] == "delete":
    data="delete at "+time.strftime("%c")+"\n"
    with open(filename, append_write) as f:
        f.write(data);
        f.close();
    delete(master,args[1])
  else:
    print "try 'put srcFile destFile OR get file OR delete file'"


if __name__ == "__main__":
  if not os.path.isdir(DATA_DIR): os.mkdir(DATA_DIR)
  if not os.path.isdir(LOG_DIR): os.mkdir(LOG_DIR)
  main(sys.argv[1:])
