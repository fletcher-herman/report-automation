"""
Automating Reporting
@author Fletcher Herman, 2020/Feb

"""
import ftplib
import pyodbc
import pandas as pd
import os
from datetime import datetime

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
        
def ftp_client_connect_write(file_name, out_path):
    """
    connect to FTP client, change working directory, write file & close connection
    @file_name: name of file (as will appear on server)
    @out_path: path to open tmp report from
    """
    
    ftp = ftplib.FTP('ftp.s7.exacttarget.com')
    ftp.login("7220142", "j.4A3pM.e")
    ftp.cwd('Import/PyAutomatedDataStream/')
    
    # write report to FTP server
    ftp.storbinary('STOR ' + file_name, open(out_path+file_name, "rb"))
    ftp.quit()
    
    print("finished export to ftp")        
        
def scv_func(sql_conn, ftp_conn):  
    """
    builds SCV report and writes to FTP
    @sql_conn: py_sql_exec()
    @ftp_conn: ftp_client_connect_write()
    """
    
    #path to write tmp report
    start_time = time.time()

    out_path = 'C:\\Users\\fletcher.herman\\Downloads\\Exports\\'
            
    # paths to SQL    
    perks_path = 'C:\\Users\\fletcher.herman\\Documents\\PythonScripts\\scheduled_jobs\\PERKS.sql'
    trans_summ_path = 'C:\\Users\\fletcher.herman\\Documents\\PythonScripts\\scheduled_jobs\\trans_summ_for_SCV.sql'
    
    # execute sql
    server = 'ctn-sqlhost01\core'
    db = 'RMS'
    
    perks = sql_conn(perks_path, server, db)
    print('perks file exported')
    
    tran_summ = sql_conn(trans_summ_path, server, db)
    print('transaction file exported')
    
    # build report
    scv = pd.merge(perks[['customer_id']],
                           tran_summ[['customer_id','transacted_kids','transacted_cat_menswear','transacted_kids_flag','transacted_cat_sports','transacted_cat_curve']],
                   on='customer_id')

    # date variable
    date_var = datetime.now().strftime("%d-%m-%Y")
    
    # file name
    file_name = 'scv' + '_' + date_var + '.csv'
    print('run date: ' + date_var)

    # store report at out location
    scv.to_csv(out_path+file_name, index=False)
    
    #call ftp function (conects and writes report to ftp server)
    ftp_conn(file_name, out_path)
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
    perks_path = 'C:\\Users\\fletcher.herman\\Documents\\PythonScripts\\scheduled_jobs\\PERKS.sql'
    age_pro_path = 'C:\\Users\\fletcher.herman\\Documents\\PythonScripts\\scheduled_jobs\\AgeProfiling.sql'
    
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
    ageProfile = pd.merge(perks_c[['customer_id','age_segment']],
                 age_pro_summ,
                 how='inner',
                 on='customer_id')
    
    ageProfile_out = ageProfile[['store_currency_code','channel','trans_order_date','division','department','category','age_segment','aud_sales']]
    
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