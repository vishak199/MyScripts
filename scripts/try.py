import os
from glob import glob
import pandas as pd
import os.path
import sys
from datetime import datetime

PATH = 
EXT = "*.csv"
all_csv_files = [file
                 for path, subdir, files in os.walk(PATH)
                 for file in glob(os.path.join(path, EXT))]
print(all_csv_files)
