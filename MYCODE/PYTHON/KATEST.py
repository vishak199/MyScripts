import os
from glob import glob
import pandas as pd
import os.path
import sys
from datetime import datetime,date, timedelta
import datetime
import openpyxl
from openpyxl import load_workbook
from openpyxl import Workbook
PATH = sys.argv[1]
ArticleBaseOnRequest_Path = PATH +r'/ArticleBaseOnRequest.xlsx'
Person_Path = PATH +r'/Person.xlsx'
PersonGroup_Path = PATH + r'/PersonGroup.xlsx'
Article_Path = PATH +r'/Article.xlsx'
Request_Path = PATH +r'/Request.xlsx'
EXT = "*.zip"
all_zip_files = [file
                 for path, subdir, files in os.walk(PATH)
                 for file in glob(os.path.join(path, EXT))]
for x in all_zip_files:
    os.remove(x)
######### For Person########
EXT2 = "Person_*.csv"
all_person_files = [file
                 for path, subdir, files in os.walk(PATH)
                 for file in glob(os.path.join(path, EXT2))]
df_from_person_file = (pd.read_csv(f, sep=',') for f in all_person_files)
df_person_merged   = pd.concat(df_from_person_file, ignore_index=True)
df_person_merged.drop(df_person_merged.columns[[0, 1]], axis = 1, inplace = True)
df=df_person_merged.to_excel(Person_Path, index=False)
wb=load_workbook(Person_Path)
sheet=wb.active
for y in all_person_files:
    os.remove(y)
print("Person file is done")