"""
Automating Reporting
@author Fletcher Herman, 2020/Feb

"""
import ftplib
import pyodbc
import pandas as pd
import os
from datetime import datetime
from io import StringIO
import boto3
import time

#class ReportAutomation:
#    """
#    This class is used to extract SQL from server, process and write to various location.
#    """

#    def __init__():
#        """
#        Init
#        """
    
def py_sql_exec(sql_path, server, db):
    """
    connect to server/db execute query at sql_path return query results
    @sql_path: location of sql script you want to execute
    @server: server name
    @db: db name
    
    """
    server = server
    db = db
    cnxn = pyodbc.connect('DRIVER={SQL Server};server=' + server + ';database=' + db + ';trusted_connection=true')
        
    # execute SQL query
    f = open(sql_path, 'r')
    query = " ".join(f.readlines())
    data = pd.read_sql(query, cnxn)
    
    cnxn.close()
    
    return(data)   
        
def ftp_client_connect_write(file_name, ftp_loc):
    """
    connect to FTP client, change working directory, write file & close connection
    @file_name: name of file (as will appear on server)
    @ftp_loc: path to open tmp report from
    
    """
    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'
    
    ftp = ftplib.FTP('ftp.s7.exacttarget.com')
    ftp.login("7220142", "j.4A3pM.e")
    ftp.cwd('Import/SCV Upload/'+ftp_loc+'/')
    
    # write report to FTP server
    ftp.storbinary('STOR ' + file_name, open(out_path+file_name, "rb"))
    ftp.quit()
    
    print(file_name +" finished export to ftp")


def sftp_client_connect_write(file_name, ftp_loc):
    """
    connect to FTP client, change working directory, write file & close connection
    @file_name: name of file (as will appear on server)
    @ftp_loc: path to open tmp report from
    
    """
    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None   
    srv = pysftp.Connection(host = "ftp.s7.exacttarget.com", username = "7220142", password = "j.4A3pM.e", cnopts=cnopts)
    
    with srv.cd('Import/SCV Upload/'+ftp_loc+'/'):
        srv.put(out_path+file_name, file_name)

    srv.close()
    
    print(file_name +" finished export to sftp")  

def to_s3(bucket, folder, file_name, out_path, content):
    content.to_csv(out_path+file_name, index=False)
    client = boto3.client('s3', region_name='ap-southeast-2')
    client.upload_file(out_path+file_name, bucket, folder+'/'+file_name)        
        
def scv_func(sql_conn, ftp_conn):  
    """
    builds SCV report and writes to FTP
    @sql_conn: py_sql_exec()
    @ftp_conn: ftp_client_connect_write()
    
    """
    start_time = time.time()
   
    #path to write tmp report 
    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'
        
    # paths to SQL    
    sfmc_segments_path = 'C:\\Users\\fletcher.herman\\Documents\\Analysis\\PythonScripts\\scheduled_jobs\\SFMC_segments.sql'
    
    # execute sql
    server = 'ctn-sqlhost01\core'
    db = 'RMS'

    ts = time.time()    
    SCV = sql_conn(sfmc_segments_path, server, db)
    te = time.time()
    tt = ((te-ts)/60)
    print(f"SCV file exported (mins): {tt:.2f}")
    
    sub_cols = ['customer_id', 'Transacted_Cat_Curve', 'Transacted_Cat_Menswear', 'Transacted_Cat_Sports', 'Transacted_KIDS_flag', 'Transacted_Cat_CoBrands', 'LIFESTAGE','Transacted_BABY']

    def subset_file(raw_file, cid, sb):
        df = raw_file.loc[raw_file[sb] != 'null'][[cid, sb]].reset_index(drop = True)
        return(df)

    cat_curve = subset_file(SCV, sub_cols[0], sub_cols[1])
    cat_menswear = subset_file(SCV, sub_cols[0], sub_cols[2])
    cat_sports = subset_file(SCV, sub_cols[0], sub_cols[3])
    cat_kids = subset_file(SCV, sub_cols[0], sub_cols[4])
    cat_cobrands = subset_file(SCV, sub_cols[0], sub_cols[5])
    cat_lifestage = subset_file(SCV, sub_cols[0], sub_cols[6])
    cat_baby = subset_file(SCV, sub_cols[0], sub_cols[7])

    # date variable
    date_var = datetime.now().strftime("%d-%m-%Y")

    # file names
    cat_curve_file_name = 'cat_curve' + '_' + date_var + '.csv'
    cat_menswear_file_name = 'cat_menswear' + '_' + date_var + '.csv'
    cat_sports_file_name = 'cat_sports' + '_' + date_var + '.csv'
    cat_kids_file_name = 'cat_kids' + '_' + date_var + '.csv'
    cat_cobrands_file_name = 'cat_cobrands' + '_' + date_var + '.csv'
    cat_lifestage_file_name = 'cat_lifestage' + '_' + date_var + '.csv'
    cat_baby_file_name = 'cat_baby' + '_' + date_var + '.csv'

    def tmp_store(file, file_name):
        file.to_csv(out_path+file_name,index=False)
        print(file_name+' exported')

    to_store = [(cat_curve, cat_curve_file_name),
                (cat_menswear, cat_menswear_file_name),
                (cat_sports, cat_sports_file_name),
                (cat_kids, cat_kids_file_name),
                (cat_cobrands, cat_cobrands_file_name),
                (cat_lifestage, cat_lifestage_file_name),
                (cat_baby, cat_baby_file_name)]  
    
    for fl, fl_nm in to_store:
        tmp_store(fl, fl_nm) 

    to_write = [(cat_curve_file_name, 'Transacted_Cat_Curve'),
                (cat_menswear_file_name, 'Transacted_Cat_Menswear'), 
                (cat_sports_file_name, 'Transacted_Cat_Sports'), 
                (cat_kids_file_name, 'Transacted_kids'),
                (cat_cobrands_file_name, 'Transacted_CoBrands'),
                (cat_lifestage_file_name, 'Lifestage'),
                (cat_baby_file_name, 'Transacted_BABY')]

    for nm, loc in to_write:
        ftp_conn(nm,loc)

    print('job done')
    print("--- %s seconds ---" % (time.time() - start_time))

   
def agePro_func(sql_conn):  
    """
    builds SCV report and writes to FTP
    @sql_conn: py_sql_exec()
    
    """
    start_time = time.time()
    
    #path to write tmp report 
    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'
        
    # paths to SQL    
    perks_path = 'C:\\Users\\fletcher.herman\\Documents\\Analysis\\PythonScripts\\scheduled_jobs\\PERKS.sql'
    age_pro_path = 'C:\\Users\\fletcher.herman\\Documents\\Analysis\\PythonScripts\\scheduled_jobs\\AgeProfiling.sql'
    
    # execute sql
    server = 'ctn-sqlhost01\core'
    db = 'RMS'
    
    perks = sql_conn(perks_path, server, db)
    print('perks file exported')
    
    tran_summ = sql_conn(age_pro_path, server, db)
    print('transaction file exported')
    
     # drop irrelevant rows
    drop = ['Missing']    
    perks_c = perks[~perks['age_band'].isin(drop)]


    # build report
    ageProfile = pd.merge(perks[['customer_id','age_segment']],
                 tran_summ,
                 how='inner',
                 on='customer_id')
    
    ageProfile_out = ageProfile[['store_currency_code','channel','trans_order_date','division','department','category','age_segment','aud_sales']]
    
    ageProfile_out = ageProfile_out.groupby(['store_currency_code','channel','trans_order_date','division','department','category','age_segment'])\
                                   .sum()\
                                   .reset_index()


    # date variable
    date_var = datetime.now().strftime("%d-%m-%Y")
    
    # file name
    file_name = 'ageProfiling' + '_' + date_var + '.csv'
    
    print('writing to s3')
    bucket = 'cog-analytics'
    csv_buffer = StringIO()
    ageProfile_out.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, file_name).put(Body=csv_buffer.getvalue())   
    print('job done')
    print("--- %s seconds ---" % (time.time() - start_time))

def ageProItem_func(sql_conn):  
    """
    builds SCV report and writes to FTP
    @sql_conn: py_sql_exec()
    
    """
    start_time = time.time()
    
    #path to write tmp report 
    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'
        
    # paths to SQL    
    perks_path = 'C:\\Users\\fletcher.herman\\Documents\\Analysis\\PythonScripts\\scheduled_jobs\\PERKS.sql'
    age_pro_path = 'C:\\Users\\fletcher.herman\\Documents\\Analysis\\PythonScripts\\scheduled_jobs\\AgeProfiling_item.sql'
    
    # execute sql
    server = 'ctn-sqlhost01\core'
    db = 'RMS'
    
    perks = py_sql_exec(perks_path, server, db)
    print('perks file exported')
    
    tran_summ = py_sql_exec(age_pro_path, server, db)
    print('transaction file exported')

    # build report
    ageProfileItem = pd.merge(perks[['customer_id','age_segment']],
                     tran_summ,
                     how='inner',
                     on='customer_id')

    # drop irrelevant rows
    ageProfileItem = ageProfileItem.loc[ageProfileItem['age_segment'] != 'Missing']
    
    ageProfileItem_out = ageProfileItem[['store_currency_code','channel','TradeWeekCode','division','department','category','item', 'age_segment','aud_sales']]
    
    ageProfileItem_out = ageProfileItem_out.groupby(['store_currency_code','channel','TradeWeekCode','division','department','category', 'item','age_segment'])\
                                           .sum()\
                                           .reset_index()

    # date variable
    date_var = datetime.now().strftime("%d-%m-%Y")
    
    # file name
    file_name = 'ageProfilingItem' + '_' + date_var + '.csv'

    #write to s3
    to_s3('cog-analytics', 'ageProfileItem', file_name, out_path, ageProfileItem_out)
    print("=====================Age Profile Item Job DONE=====================")                