import sys
import time
import os
#To get date & time
timert = time.strftime("%Y%m%d%H%M%S")
#creation of the filr 
path = "/opt/nfs/UCMDB-SMAX-ETL-PROD/summary-result/"
filename = "etl-result-" + timert + ".txt"
filepath = path + filename
open(filepath, 'w')
print(filepath)

