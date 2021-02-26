# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 09:49:11 2019

@author: jiayinli
"""

import os
import requests
import re
import datetime
import teradata
import pandas as pd

starttime = datetime.datetime.now()

def returnDF(this_query):
    # return the result of this query in format of pandas.DataFrame
    with open('C:/Tasks/JDBC_config/DB_config_yfw.txt') as file:
        db_config = file.read()
        db_server = re.findall('Server:(.*)',db_config)[0]
        user_name = re.findall('User:(.*)',db_config)[0]
        user_pw = re.findall('Password:(.*)',db_config)[0]
    df = pd.DataFrame()
    udaExec = teradata.UdaExec(appName="TestConnection", version="0.1", logConsole=False)
    session = udaExec.connect(method="odbc", system=db_server, username=user_name, password=user_pw)
    
    for row in session.execute(this_query):
        df = df.append(pd.Series(list(row)), ignore_index=True)
    session.close()
    return df

userdf = returnDF('''
    select slr_id, encrypted_user_id, user_slctd_id from P_AUB2C_T.AU_clinics_recom_eGD;
    ''')

header_list = [
    "slr_id"
    , "encrypted_user_id"
    , "user_slctd_id"
    ]

userdf.columns = header_list

logfile = "C:/Tasks/Demo_AU/upload_log_b2c.txt"
#clear log file
with open(logfile, "r+") as f:
    f.truncate()
    
Path = "C:/Tasks/Demo_AU/pdf_output/"
filelist = os.listdir(Path)

print("start uploading")

with open(logfile,'a') as f2:
    for pdf_file in filelist:
        pattern = re.compile(r"\d+")
        encrypted_user_id = int(pattern.findall(pdf_file)[0])
        user_slctd_id = userdf.loc[(userdf["encrypted_user_id"]==encrypted_user_id),"user_slctd_id"].values[0]    
        
        with open(Path + pdf_file, 'rb') as f:
            r = requests.post('https://submissions.ebay.com.au/api/SellerFile', files={'file': f}, data={'userId': user_slctd_id, 'puuId':encrypted_user_id}, headers={'Authorization': 'basic FD447D68-3395-482D-9EBF-7425E7E1F827'})
            print(encrypted_user_id, 'has been uploaded')

        f2.write(r.text)
        f2.write('\n')
        
print(starttime)
print(datetime.datetime.now())


# code used to upload specific files which are failed to be uploaded before
#see https://blog.csdn.net/yyhhlancelot/article/details/82228803 to find out how to locate specific file name


# =============================================================================
# with open(logfile,'a') as f2:
#     bool = userdf['user_slctd_id'].str.contains('\*')
#     filter_data = userdf[bool]
#     for user_slctd_id in filter_data['user_slctd_id']:
#         encrypted_user_id = userdf.loc[(userdf['user_slctd_id']==user_slctd_id),'encrypted_user_id'].values[0]
#         print(Path + str(encrypted_user_id) + '_May_2019.pdf')
#         with open(Path + str(encrypted_user_id) + '_May_2019.pdf', 'rb') as f:
#             r = requests.post('https://submissions.ebay.com.au/api/SellerFile', files={'file': f}, data={'userId': user_slctd_id, 'puuId':encrypted_user_id}, headers={'Authorization': 'basic FD447D68-3395-482D-9EBF-7425E7E1F827'})
#             print(encrypted_user_id, 'has been uploaded')
#         f2.write(r.text)
#         f2.write('\n')
# =============================================================================


