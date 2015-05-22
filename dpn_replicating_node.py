# dpn_replicating_node.py
#
# 1. Query a remote node for pending transfer requests.
# 2. Use rsync to copy files in the transfer requests.
# 3. Calculate the sha-265 checksums of the files.
# 4. Send the checksums back to the remote node.
# ----------------------------------------------------------------------
from dpnclient import client, util
import dpn_rest_settings
import hashlib
import os, sys
import subprocess, logging
import pprint

class dpn_replicating_node:

    def __init__(self, config):
        self.client = client.Client(dpn_rest_settings, dpn_rest_settings.DEV)

    def replicate_files(self, namespace):
    	    
        """
        Replicate bags from the specified namespace.
        """
        requests = self.client.get_transfer_requests(namespace)
        
        print("Pending requests on server: " + namespace + " - " + str(len(requests)))

        for request in requests:
            link = request['link']
            replication_id = request['replication_id']
            
            # download the file via rsync
            print("Downloading {0}".format(link))
            local_path = self.copy_file(link)

            if (local_path == ""):
                return
    
            # calculate the checksum
            checksum = util.digest(local_path, "sha256")
            
            # send the checksum as receipt
            print("Returning checksum receipt {0}".format(checksum))
            self.client.set_transfer_fixity(namespace, replication_id, checksum)

    def copy_file(self, location):
        filename = os.path.basename(location.split(":")[1])
        dst = os.path.join(dpn_rest_settings.INBOUND_DIR, filename)
        command = ["rsync", "-Lav", "--compress",
                   "--compress-level=0", "--quiet", location, dst]
        print(" ".join(command))
        try:
            with subprocess.Popen(command, stdout=subprocess.PIPE) as proc:
                print(str(proc.communicate()[0]))
            if (os.path.isfile(dst) == False):
                print("Error downloading file: " + dst)
                return ""
            return dst

        except Exception as err:
            print("ERROR Transfer failed: {0}".format(err))
            raise err


if __name__ == "__main__":

    logging.captureWarnings(True)

    try:
        xfer = dpn_replicating_node(dpn_rest_settings.DEV)
        
    except Exception as err:
        print("ERROR Transfer failed: {0}".format(err))
        sys.exit(0)        

    # Iterate all of the servers in the KEYS dict in the settings file
    for key in dpn_rest_settings.KEYS:
        print ("Polling Server: " + key)

        xfer.replicate_files(key)
