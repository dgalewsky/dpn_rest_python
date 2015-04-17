from dpnclient import client, util
import dpn_rest_settings
import hashlib
import logging
import pprint
import requests
import sys
import os.path
     
def usage() :
    print("Usage: " + sys.argv[0] + " <DPN Object Id>")
    sys.exit(0)
    
    
logging.basicConfig(filename='dpn.log',level=logging.DEBUG)
logging.captureWarnings(True)    

config = dpn_rest_settings.DEV

if (len(sys.argv) == 1) :
    usage()

obj_id = sys.argv[1]
obj_path = "/dpn/outgoing/" + obj_id + ".tar"

print ("Ingesting: " + obj_path)

if os.path.isfile(obj_path) == False :
    print("Error: " + obj_path + " does not exit")
    sys.exit(0)
    
# While we are here - we might as well compute the size of the bag

file_size = os.path.getsize(obj_path)

print("Size of file: " + obj_path + " : "  + str(file_size))

print("REST Server URL: " + config['url'])

# Create a dpn-rest client

myclient = client.Client(dpn_rest_settings, dpn_rest_settings.DEV)

# I bet we need to add a fixity parameter

try:
    response = myclient.create_bag_entry(obj_id, 1024, 'D')
except Exception as ex:  
    print("Error: " +  str(ex))
    sys.exit(0)

# If all goes well - print the response.

pprint.pprint(response)

# Create a transfer

try:
    # obj_id, bag_size, username, fixity
    response = myclient.create_transfer_request(obj_id, 1024, 'aptrust', 'ou812-fixity')
except Exception as ex:  
    print("Error: " +  str(ex))
    sys.exit(0)
    
pprint.pprint(response) 
