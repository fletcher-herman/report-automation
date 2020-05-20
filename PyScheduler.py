"""
Automating Reporting - Scheduler
@author Fletcher Herman, 2020/Feb

"""

from apscheduler.schedulers.blocking import BlockingScheduler
import  time
import ReportAutomationFunctions as RPF
import logging

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

sql_conn = RPF.py_sql_exec
ftp_conn = RPF.sftp_client_connect_write
scv_job = RPF.scv_func
agePro_job = RPF.agePro_func
ageProItem_job = RPF.ageProItem_func

scheduler = BlockingScheduler(timezone="Australia/Sydney")

scheduler.add_job(lambda: scv_job(sql_conn, ftp_conn), 'cron', day_of_week='mon-sun', hour=12,minute=16)
#scheduler.add_job(lambda: ageProItem_job(sql_conn), 'cron', day_of_week='mon-sun', hour=10,minute=28)
#scheduler.add_job(lambda: agePro_job(sql_conn), 'cron', day_of_week='mon-sun', hour=10,minute=48)

scheduler.start()

while True:
    time.sleep(1)
    