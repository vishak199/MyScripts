import pandas as pd
import os
from glob import glob

 

#df = pd.read_csv('data.csv')
#df = pd.read_csv('/opt/nfs/merge_Basefolder/12-05-2021-11-33-19_296108567/ExtractedFiles/ArticleBaseOnRequest_0.csv')

 

all_csv_files = [file
                 for path, subdir, files in os.walk('/opt/nfs/merge_Basefolder/12-05-2021-11-33-19_296108567/ExtractedFiles/')
                 for file in glob(os.path.join(path, "*.csv"))]
#print(all_csv_files)
df_from_each_file = (pd.read_csv(f, sep=',') for f in all_csv_files)
df_merged = pd.concat(df_from_each_file, ignore_index=True)

 

df=df_merged.to_csv('/opt/nfs/merge_Basefolder/12-05-2021-11-33-19_296108567/ExtractedFiles/test2.csv', index=False)

 

#print(df.to_string())
