#!/usr/bin/env python
from __future__ import print_function
import random
import sys
import requests
from StringIO import StringIO
import os


def _print_warning():
  banner="""* 				CAUTION:
This tool generate script to force allocate shards and mark Cluster GREEN
This means it wont fix any data loss. This is handy when you dont have backups to restore and want to fore mark CLUSTER GREEN
	"""
  print(banner)

def _get_unassigned_shards():
  shards=[]
  r=requests.get('http://localhost:9200/_cat/shards')
  lines=StringIO(r.text)
  for line in lines:
      line=line.strip()
      if "UNASSIGNED" in line:
	 tokens=line.split(" ")
	 if tokens[0] == "lists":
	    yield tokens[0],str(0),tokens[2]
	 else:
	    yield tokens[0],tokens[1],tokens[2] 

def _get_search_nodes():
   search_nodes=[]
   try:
	r=requests.get('http://localhost:9200/_cat/nodes?h=name')
	lines=StringIO(r.text)
	for line in lines:
	   host=line.strip()
	   if "-search" in host:
		search_nodes.append(host)
   except Exception as e:
	print("Error : {0}".format(e))
	sys.exit(-1)
   return search_nodes

_print_warning()
a=_get_search_nodes()
print("We have {0} data nodes".format(len(a)))
fileName="/tmp/forceAllocate.sh"
print("* Generating {0}".format(fileName))
cm1="""
echo "* Adjusting shards per index"
for idx in `curl -s localhost:9200/_cat/indices | egrep -i "red|yellow" | awk '{ print $3; }'`;
do
curl -XPUT localhost:9200/$idx/_settings  -d '{ "index.routing.allocation.total_shards_per_node": 3 }' > /dev/null 2>&1
done
"""
try:
    os.remove(fileName)
except Exception as e:
    pass
cm2="""
echo "\n* Enabling Cluster Allocation"
curl -XPUT http://localhost:9200/_cluster/settings -d '{
    "transient" : {
        "cluster.routing.allocation.enable": "all" } }' > /dev/null 2>&1
echo "\n* Changing Recovery Params" 
curl -XPUT localhost:9200/_cluster/settings -d '{ 
"persistent" : { 
"indices.recovery.max_bytes_per_sec": "200mb", 
"indices.recovery.concurrent_streams": 5 } } > /dev/null 2>&1'
echo "\n* Waiting for few seconds...."
sleep 30s
"""
l = open (fileName,'w')
l.write(cm1)
l.write(cm2)
for (index,shard,TYPE) in _get_unassigned_shards(): 
   cmd="""
curl -XPOST 'localhost:9200/_cluster/reroute' -d '{
    "commands": [{
        "allocate": {
            "index": "INDEX",
            "shard": SHARD,
            "node": "NODE",
            "allow_primary": PR
        }
    }]
}'
"""
   if TYPE == "r":
      TYPE=str(0)
   else:
      TYPE=str(1)

   l.write(cmd.replace("INDEX",index).replace("SHARD",shard).replace("NODE",random.choice(a)).replace("PR",TYPE))

l.close()
print("* Running bash {0}".format(fileName))
os.system("/bin/bash {0} ".format(fileName))
