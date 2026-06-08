from dateutil.parser import parse
import threading
import pymysql as MySQLdb
import subprocess
import time
import logging
import logging.config
from configparser import ConfigParser
import requests
import sys

global dbport
global dbsocket
global clientThrdKillId
dbport = sys.argv[1]
dbsocket = sys.argv[2]

config_file = "/etc/logConfig/ticketLog.conf"##
logging.config.fileConfig(''+config_file+'')###
logmain = logging.getLogger('main')##

#================= Read Configuration file =================================
configFiledata = ConfigParser()
configFiledata.readfp(open(r'/etc/logConfig/autoTicket.conf'))
centralDb = configFiledata.get('Mysql_Info','centralDb')
clientDb = configFiledata.get('Mysql_Info','clientDb')
api_url = configFiledata.get('Mysql_Info','api')
source_values = configFiledata.get('Mysql_Info','source_arr')
source_arr = source_values.split(',')

#============= Read all login credentials for Primary 1 mysql server from configuration file ================
mysqlHost_Pri1 = configFiledata.get('mysql_primary_write','mysqlHost')
mysqlUser_Pri1 = configFiledata.get('mysql_primary_write','mysqlUser')
mysqlPasswd_Pri1 = configFiledata.get('mysql_primary_write','mysqlPasswd')
#============= Read all login credentials for Primary 2 mysql server from configuration file ================
mysqlHost_Pri2 = configFiledata.get('mysql_primary_read','mysqlHost')
mysqlUser_Pri2 = configFiledata.get('mysql_primary_read','mysqlUser')
mysqlPasswd_Pri2 = configFiledata.get('mysql_primary_read','mysqlPasswd')
#============= Read all login credentials for secondary mysql server from configuration file ================
mysqlHost_Sec1 = configFiledata.get('mysql_secodary_write','mysqlHost')
mysqlUser_Sec1 = configFiledata.get('mysql_secodary_write','mysqlUser')
mysqlPasswd_Sec1 = configFiledata.get('mysql_secodary_write','mysqlPasswd')
#============= Read all login credentials for secondary mysql server from configuration file ================
mysqlHost_Sec2 = configFiledata.get('mysql_secodary_read','mysqlHost')
mysqlUser_Sec2 = configFiledata.get('mysql_secodary_read','mysqlUser')
mysqlPasswd_Sec2 = configFiledata.get('mysql_secodary_read','mysqlPasswd')
#======================== End Here ===========================================

global allDept_EmailArr
global allDept_PhoneArr
global dept_wise_user_info
dept_wise_user_info = {}
lock = threading.Lock()
allDept_EmailArr = []
allDept_PhoneArr = []

def sqlConnect(mysqlHost,mysqlUser,mysqlPasswd,db_name,port=None,unix_sock=None):
        db = ""
        cur = ""
        try:
                try:
                        db = MySQLdb.connect(""+mysqlHost+"",""+mysqlUser+"",""+mysqlPasswd+"",""+str(db_name)+"",port=int(port),unix_socket=db_var["MASTER_DB_SOCKET"])
                        cur = db.cursor()
                        logmain.info("Creating mysql connection with mysql socket.")
                except:
                        db = MySQLdb.connect(""+mysqlHost+"",""+mysqlUser+"",""+mysqlPasswd+"",""+str(db_name)+"",port=int(port))
                        cur = db.cursor()
                        logmain.info("Creating mysql connection without mysql socket.")
        except:
                logmain.error("Unable to create connection with "+db_name)
                #time.sleep(10)
        return db, cur

def dbHandler(dbName, dbport=None, dbsocket= None, conn_flag=None):
                mdbPri1_write = ""
                curPri1_write = ""

                mdbPri2_read = ""
                curPri2_read = ""

                mdbSec1_write = ""
                curSec1_write = ""

                mdbSec1_read = ""
                curSec1_read = ""
                if conn_flag==None:
                        mdbPri1_write, curPri1_write = sqlConnect(mysqlHost_Pri1, mysqlUser_Pri1, mysqlPasswd_Pri1, dbName,port=dbport,unix_sock=dbsocket)
                        db_obj_write = mdbPri1_write
                        cur_obj_write = curPri1_write
                        if (bool(mdbPri1_write)==False and bool(curPri1_write)==False):
                                mdbSec1_write, curSec1_write = sqlConnect(mysqlHost_Sec1, mysqlUser_Sec1, mysqlPasswd_Sec1, dbName, port=dbport,unix_sock=dbsocket)
                                db_obj_write = mdbSec1_write
                                cur_obj_write = curSec1_write
                        mdbPri2_read,curPri2_read = sqlConnect(mysqlHost_Pri2, mysqlUser_Pri2, mysqlPasswd_Pri2, dbName, port=dbport,unix_sock=dbsocket)
                        db_obj_read = mdbPri2_read
                        cur_obj_read = curPri2_read
                        if (bool(mdbPri2_read) == False and  bool(curPri2_read) == False):
                                if (str(mysqlHost_Sec2)!=str(mysqlHost_Sec1)):
                                        mdbSec2_read, curSec2_read = sqlConnect(mysqlHost_Sec2,mysqlUser_Sec2, mysqlPasswd_Sec2, dbName, port=dbport,unix_sock=dbsocket)
                                        db_obj_read = mdbSec2_read
                                        cur_obj_read = curSec2_read
                                else:
                                        db_obj_read = mdbSec1_write
                                        cur_obj_read = curSec1_write
                        return db_obj_write, cur_obj_write, db_obj_read, cur_obj_read
                elif conn_flag == "WRITE":
                        mdbPri1_write, curPri1_write = sqlConnect(mysqlHost_Pri1, mysqlUser_Pri1, mysqlPasswd_Pri1, dbName, port=dbport,unix_sock=dbsocket)
                        db_obj_write = mdbPri1_write
                        cur_obj_write = curPri1_write
                        if (bool(mdbPri1_write)==False and bool(curPri1_write)==False):
                                mdbSec1_write, curSec1_write = sqlConnect(mysqlHost_Sec1, mysqlUser_Sec1, mysqlPasswd_Sec1, dbName, port=dbport,unix_sock=dbsocket)
                                db_obj_write = mdbSec1_write
                                cur_obj_write = curSec1_write
                        return db_obj_write,cur_obj_write
                elif conn_flag == "READ":
                        mdbPri2_read, curPri2_read = sqlConnect(mysqlHost_Pri2, mysqlUser_Pri2, mysqlPasswd_Pri2, dbName, port=dbport,unix_sock=dbsocket)
                        db_obj_read = mdbPri2_read
                        cur_obj_read = curPri2_read
                        if (bool(mdbPri2_read) == False and  bool(curPri2_read) == False):
                                if (str(mysqlHost_Sec2)!=str(mysqlHost_Sec1)):
                                        mdbSec2_read, curSec2_read = sqlConnect(mysqlHost_Sec2, mysqlUser_Sec2, mysqlPasswd_Sec2, dbName, port=dbport,unix_sock=dbsocket)
                                        db_obj_read = mdbSec2_read
                                        cur_obj_read = curSec2_read
                                else:
                                        mdbSec1_read, curSec1_read = sqlConnect(mysqlHost_Sec1, mysqlUser_Sec1, mysqlPasswd_Sec1, dbName, port=dbport,unix_sock=dbsocket)
                                        db_obj_read = mdbSec1_read
                                        cur_obj_read = curSec1_read
                        return db_obj_read, cur_obj_read

def queryExecuter(query,flag,db_object,cur_object):
        logqueryExecuter = logging.getLogger('queryExecuter')###
        queryResult = []
        update_res = 0
        if ("INSERT" == flag or "DELETE" == flag or "UPDATE" == flag):
                try:
                        lock.acquire()
                        update_res = cur_object.execute(query)
                        logqueryExecuter.info(query)###
                        db_object.commit()
                        lock.release()
                except MySQLdb.InterfaceError:
                        lock.release()
                        logqueryExecuter.error('Error executing query'+query, exc_info=True)###
                except:
                        lock.release()
                        logqueryExecuter.error('Error executing query'+query, exc_info=True)###
                if "UPDATE" == flag:
                        return update_res
        elif "SELECT" == flag:
                try:
                        lock.acquire()
                        count = cur_object.execute(query)
                        db_object.commit()
                        logqueryExecuter.info(query)###
                        if (int(count) > 0):
                                queryResult = cur_object.fetchall()
                        lock.release()
                except MySQLdb.InterfaceError:
                        lock.release()
                        logqueryExecuter.error('MySQLdb.InterfaceError : '+query, exc_info=True)####query
                except:
                        lock.release()
                        logqueryExecuter.error('Failed to execute query '+query, exc_info=True)####query
        else:
                return

        return queryResult

def myUnixTime():
        time.sleep(1)
        unixdate = time.time()
        return unixdate

def ticketAssignProcess(user_turn_id, deprt_id, tcket_id, ttAsign_DbObj, ttAsign_CurObj, cid):
        logticketAssign = logging.getLogger('ticketAssignProcess')###
        #print('hi===================================================='+tcket_id)
        #"""
        timestmp = myUnixTime()
        #qry1 = "update ticket_details set assigned_to_user_id=%s ,assigned_to_dept_id=%s where ticket_id=%s" % (user_turn_id,deprt_id,tcket_id)
        qry1 = "update ticket_details set assigned_to_user_id = if(assigned_to_user_id!='',assigned_to_user_id,'"+str(user_turn_id)+"'),assigned_to_dept_id = if(assigned_to_dept_id!='',assigned_to_dept_id,'"+str(deprt_id)+"') where ticket_id = "+str(tcket_id)+"" #% (user_turn_id,deprt_id,tcket_id)
        query_res = queryExecuter(qry1,"UPDATE",ttAsign_DbObj,ttAsign_CurObj)
        if int(query_res) != 0:
                qry2 = "update UsersQue set ticket_queue_count=ticket_queue_count+1, ticket_max_assign_count=ticket_max_assign_count+1 ,ticket_timestamp=%s where user_id=%s and dept_id=%s" % (str(int(timestmp)),user_turn_id,deprt_id)
                res = queryExecuter(qry2,"UPDATE",ttAsign_DbObj,ttAsign_CurObj)
                qry3 = "delete from unprocessed_tickets where ticket_id=%s and type='T'" % (tcket_id)
                res = queryExecuter(qry3,"DELETE",ttAsign_DbObj,ttAsign_CurObj)
        #"""
                url = ""+api_url+"client_id={}&assigned_to_dept_id={}&assigned_to_user_id={}&ticket_id={}".format(cid, deprt_id, user_turn_id, tcket_id)
                logticketAssign.info(url)
                r = requests.get(url)
                ##print('requested: {}, \n STatus: {}----------------------------'.format(url, r.status_code))

def queueRenewer(user_array,dept_id,max_queue,querenw_DbObj,querenw_CurObj):
        logrenew = logging.getLogger('queueRenewer')###
        for user_arr in user_array:
                renewQuery = ""
                user_time = user_arr[-1]
                cmd = "date +%Y-%m-%d"
                dateString = time.strftime('%Y-%m-%d')
                #dateString = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True).communicate()[0]
                time_srt = parse(dateString.strip("'"))
                startDayTime = time.mktime(time_srt.timetuple())
                crnt_time = myUnixTime()
                if (int(user_time) < int(startDayTime)):
                        renewQuery = "update UsersQue set ticket_max_assign_count=0, ticket_timestamp=%s where user_id=%s and dept_id=%s" % (str(int(crnt_time)),user_arr[0],dept_id)
                        logrenew.info('running renew query: '+renewQuery)
                if renewQuery != "":
                        res = queryExecuter(renewQuery,"UPDATE",querenw_DbObj,querenw_CurObj)
                        logrenew.info('Ran success renew')
        return

def DeptMngr(dept_id,dept_email,max_queue_limit,queue_limit,critical_limit,policy,dept_phone,dept_DbObj_write,dept_CurObj_write,dept_DbObj_read,dept_CurObj_read, cid,clientDbName):
        global allDept_EmailArr
        global allDept_PhoneArr
        logdept = logging.getLogger('DeptMngr')###
        logdept.info('started deptmgr')
        while 1:
                try:
                        dept_DbObj_write.ping()
                        dept_DbObj_read.ping()
                except:
                        dept_DbObj_write,dept_CurObj_write, dept_DbObj_read, dept_CurObj_read = dbHandler(""+str(clientDbName)+"",dbport=dbport, dbsocket=dbsocket, conn_flag=None)
                live_users_arr = dept_wise_user_info[cid+'_'+str(int(dept_id))]
                #print(live_users_arr)
                if KillThrID == dept_id :
                        break
                else:
                        if dept_wise_user_info[cid+'_'+str(int(dept_id))] == []:
                                break
                        user_queue_arr = []
                        new_tickets_res = []
                        new_tickets_arr = []
                        user_ticket_limit_hash = {}
                        leastRecent_user_ticket_limit_hash = {}
                        roundRobin_user_ticket_limit_hash = {}
                        rankingBased_user_ticket_limit_hash = {}
                        user_id_list = []
                        user_info = []
                        slquery = "select user_id, ticket_queue_count, ticket_max_assign_count, ticket_critical_queue_count, ticket_timestamp from UsersQue where dept_id=%s" % (dept_id)
                        logdept.info('running: '+str(slquery))
                        user_queue_arr = queryExecuter(slquery,"SELECT",dept_DbObj_read,dept_CurObj_read)
                        logdept.info('result: ' + str(user_queue_arr))
                        if user_queue_arr != []:
                                for user_id_info in user_queue_arr:
                                        user_id_list.append(str(int(user_id_info[0])))
                                        if str(user_id_info[0]) in live_users_arr:
                                                #print('inside', user_id_info[0])
                                                if ((int(user_id_info[1]) < int(queue_limit)) and (int(user_id_info[2]) < int(max_queue_limit))):
                                                        if policy.lower() == "round robin":
                                                                #print("ROUND")
                                                                logdept.info('rr policy')
                                                                user_info = user_id_info[0],user_id_info[1],user_id_info[2],user_id_info[3]
                                                                user_ticket_limit_hash[user_id_info[-1]] = user_info
                                                                ##print user_ticket_limit_hash
                                                                user_info = []
                                                        elif policy.lower() == "ltr":
                                                                #print("LEAST")
                                                                user_info = user_id_info[0],user_id_info[-1],user_id_info[2],user_id_info[3]
                                                                user_ticket_limit_hash[tuple(user_info)] = user_id_info[1]
                                                                #print(user_ticket_limit_hash)
                                                                user_info = []

                        queueRenewer(user_queue_arr,dept_id,max_queue_limit,dept_DbObj_write,dept_CurObj_write)
                        logdept.info("{} {} {} {}".format(user_ticket_limit_hash, dept_id, cid, 'done------------------------------'))
                        Inquery = ""
                        if live_users_arr != ():
                                for live_user_id in live_users_arr:
                                        if live_user_id not in user_id_list:


                                                sltcktquery = "select * from ticket_details where assigned_to_user_id=%s and assigned_to_dept_id=%s and ticket_status_id !=2" % (live_user_id,dept_id)
                                                logdept.info('running ' + sltcktquery)
                                                #print(sltcktquery)
                                                try:
                                                        lock.acquire()
                                                        ticket_count = dept_CurObj_read.execute(sltcktquery)
                                                        dept_DbObj_read.commit()
                                                        timestamp = myUnixTime()
                                                        lock.release()
                                                        if (int(ticket_count) != 0):
                                                                Inquery = "insert into UsersQue (user_id,dept_id, ticket_queue_count, ticket_max_assign_count, ticket_critical_queue_count, ticket_timestamp, lead_timestamp) values(%s,%s,%s,%s,%s,%s, %s)" % (live_user_id,dept_id,str(int(ticket_count)),str(int(ticket_count)),0,str(int(timestamp)) ,str(int(timestamp)))
                                                        elif (int(ticket_count) == 0):
                                                                Inquery = "insert into UsersQue (user_id,dept_id, ticket_queue_count, ticket_max_assign_count, ticket_critical_queue_count, ticket_timestamp, lead_timestamp) values(%s,%s,%s,%s,%s,%s, %s)" % (live_user_id,dept_id,0,0,0,str(int(timestamp)),str(int(timestamp)))
                                                except MySQLdb.InterfaceError:
                                                        lock.release()
                                                        logdept.error('Failed to execute query '+sltcktquery, exc_info=True)####
                                                except:
                                                        lock.release()
                                                        logdept.error('Failed to execute query '+sltcktquery, exc_info=True)####
                                                if (Inquery != ""):
                                                        logdept.info('running ' + Inquery)
                                                        queryExecuter(Inquery,"INSERT",dept_DbObj_write,dept_CurObj_write)
                        if user_ticket_limit_hash != {}:
                                slqry = "select default_dept_id from config_setting where activation=1"
                                default_dept_id = queryExecuter(slqry,"SELECT",dept_DbObj_read,dept_CurObj_read)
                                if default_dept_id != []:
                                        if int(default_dept_id[0][0]) == int(dept_id):
                                                if dept_email not in allDept_EmailArr and dept_phone not in allDept_PhoneArr :
                                                        allDept_EmailArr.append(dept_email)
                                                        #print(allDept_EmailArr)
                                                       	allDept_PhoneArr.append(dept_phone)
                                                        #print(allDept_PhoneArr)

                                                        allDept_EmailArr_b = str(tuple(list(map(str, allDept_EmailArr)))).rstrip(')').rstrip(',') + ")"
                                                        allDept_PhoneArr_b = str(tuple(list(map(str, allDept_PhoneArr)))).rstrip(')').rstrip(',') + ")"

                                                        #print allDept_EmailArr_b
                                                        #print allDept_PhoneArr_b

                                                #if bool(allDept_EmailArr_b) == False or bool(allDept_PhoneArr_b) == False:
                                                #       new_tickets_query = "select ticket_id, source, source_value from unprocessed_tickets where (source_value='"+dept_email+"' and type='T') or (source_value='"+dept_phone+"' and type='T') or (assigned_to_dept_id = '%s' and type='T') order by id ASC limit 10" % (str(int(dept_id)))
                                                #else:
                                                        new_tickets_query = "select ticket_id, source, source_value from unprocessed_tickets where (source_value in %sand type='T') or (source_value in %s and type='T') or (assigned_to_dept_id = '%s' and type = 'T') AND ticket_id !=0 order by id ASC limit 10" % (allDept_EmailArr_b, allDept_PhoneArr_b,str(int(dept_id)))
                                                else:
                                                        new_tickets_query = "select ticket_id, source, source_value from unprocessed_tickets where type='T' and (source_value='"+dept_email+"' or source_value='"+dept_phone+"' or assigned_to_dept_id = '"+str(int(dept_id))+"') AND ticket_id !=0 order by id ASC limit 10"
                                        else:
                                                new_tickets_query = "select ticket_id, source, source_value from unprocessed_tickets where type='T' and(source_value='"+dept_email+"' or source_value='"+dept_phone+"' or assigned_to_dept_id = '"+str(int(dept_id))+"') AND ticket_id !=0 order by id ASC limit 10"
                                else:
                                        new_tickets_query = "select ticket_id, source, source_value from unprocessed_tickets where type='T' and (source_value='"+dept_email+"' or source_value='"+dept_phone+"' or assigned_to_dept_id = '"+str(int(dept_id))+"') AND ticket_id !=0 order by id ASC limit 10"
                                #print new_tickets_query
                                new_tickets_res = queryExecuter(new_tickets_query,"SELECT",dept_DbObj_read,dept_CurObj_read)
                                if new_tickets_res != []:
                                        for new_tickets in new_tickets_res:
                                                if str(new_tickets[0])+"__"+str(new_tickets[1])+"__"+str(new_tickets[2]) not in new_tickets_arr:
                                                        new_tickets_arr.append(str(new_tickets[0])+"__"+str(new_tickets[1])+"__"+str(new_tickets[2]))
                                if (new_tickets_arr != []):
                                        time.sleep(1)
                                        #count = 0
                                        #print (new_tickets_arr)
                                        for new_tkts in new_tickets_arr:
                                                ##print count
                                                ##print new_tickets_arr
                                                ticket_id,Source,Source_value = new_tkts.split("__")
                                                #print(Source)
                                                if policy.lower() == "round robin":
                                                        #print("ROUND")
                                                        user_turn = min(user_ticket_limit_hash.keys()) # agent replace by user
                                                        user_turn_info = user_ticket_limit_hash[user_turn]
                                                        #print user_turn_info
                                                        #if "email" == Source.lower():
                                                        if Source.lower() in source_arr:
                                                                #if Source_value in allDept_EmailArr:
                                                                #       #print "Email Exist"
                                                                #       if Source_value == dept_email:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj_write,dept_CurObj_write, cid)
                                                                ##print user_turn_info
                                                                if user_turn in user_ticket_limit_hash.keys():
                                                                        #if (len(user_ticket_limit_hash.keys()) == 1):
                                                                        #       user_ticket_limit_hash = {}
                                                                        del user_ticket_limit_hash[user_turn]
                                                                #if new_tkts in new_tickets_arr:
                                                                new_tickets_arr.remove(new_tkts)
                                                                ##print "==============================="
                                                                ##print user_ticket_limit_hash
                                                                ##print "==============================="
                                                        """
                                                        elif ("sms" == Source.lower() or "call" == Source.lower()):
                                                                #if Source_value in allDept_PhoneArr:
                                                                #       if Source_value == dept_phone:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj,dept_CurObj, cid)
                                                                if user_turn in user_ticket_limit_hash.keys():
                                                                        #if (len(user_ticket_limit_hash.keys()) == 1):
                                                                        #       user_ticket_limit_hash = {}
                                                                        del user_ticket_limit_hash[user_turn]
                                                                if new_tkts in new_tickets_arr:
                                                                        new_tickets_arr.remove(new_tkts)
                                                        elif ("ivr" == Source.lower()):
                                                                #if Source_value in allDept_PhoneArr:
                                                                #       if Source_value == dept_phone:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj,dept_CurObj, cid)
                                                                if user_turn in user_ticket_limit_hash.keys():
                                                                        #if (len(user_ticket_limit_hash.keys()) == 1):
                                                                        #       user_ticket_limit_hash = {}
                                                                        del user_ticket_limit_hash[user_turn]
                                                                if new_tkts in new_tickets_arr:
                                                                        new_tickets_arr.remove(new_tkts)
                            """
                                                        if user_ticket_limit_hash == {}:
                                                                break
                                                elif policy.lower() == "ltr":
                                                        #print("LEAST")
                                                        user_turn = min(user_ticket_limit_hash.values())
                                                        for key,value in user_ticket_limit_hash.items():
                                                                if value == user_turn:
                                                                        user_turn_info = key
                                                                        break
                                                        if Source.lower() in source_arr:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj_write,dept_CurObj_write, cid)
                                                                if user_turn in user_ticket_limit_hash.keys():
                                                                        del user_ticket_limit_hash[user_turn]
                                                                new_tickets_arr.remove(new_tkts)    
                                                        '''if "email" == Source.lower():
                                                                #if Source_value in allDept_EmailArr:
                                                                #       if Source_value == dept_email:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj_write,dept_CurObj_write, cid)
                                                                if user_turn_info in user_ticket_limit_hash.keys():
                                                                        #if (len(user_ticket_limit_hash.keys()) == 1):
                                                                        #       user_ticket_limit_hash = {}
                                                                        del user_ticket_limit_hash[user_turn_info]
                                                                if new_tkts in new_tickets_arr:
                                                                        new_tickets_arr.remove(new_tkts)

                                                        elif ("sms" == Source.lower() or "call" == Source.lower()):
                                                                #if Source_value in allDept_PhoneArr:
                                                                        #if Source_value == dept_phone:
                                                                ticketAssignProcess(user_turn_info[0],dept_id,ticket_id,dept_DbObj_write,dept_CurObj_write, cid)
                                                                if user_turn_info in user_ticket_limit_hash.keys():
                                                                        if (len(user_ticket_limit_hash.keys()) == 1):
                                                                                user_ticket_limit_hash = {}
                                                                        del user_ticket_limit_hash[user_turn_info]
                                                                if new_tkts in new_tickets_arr:
                                                                        new_tickets_arr.remove(new_tkts)'''
                                                        if user_ticket_limit_hash == {}:
                                                                logdept.info('No free agents are available.')###
                                                                break
                                                #new_tickets_arr.remove(new_tkts)
                                                #count +=1
                                                if len(new_tickets_arr) == 0:
                                                        break
                                else:
                                        logdept.info('NO NEW TICKETS AVAILABEL.')###

                        else:
                                logdept.info('NO FREE AGENTS AVAILABLE')###

                time.sleep(5)

#======= This Function manage all client thread and it is used for opening department wise threads for all client ==========

def clientMngr(clientId , clientDbName, dbport):
        alive_thr_arr = []
        id_arr = []
        global clientThrdKillId
        logClientMgr = logging.getLogger('clientMngr')
        db_obj_write,cur_obj_write, db_obj_read, cur_obj_read = dbHandler(""+str(clientDbName)+"",dbport=dbport, dbsocket=dbsocket, conn_flag=None)
        while 1:
                try:
                        db_obj_write.ping()
                        db_obj_read.ping()
                except:
                        db_obj_write,cur_obj_write, db_obj_read, cur_obj_read = dbHandler(""+str(clientDbName)+"",dbport=dbport, dbsocket=dbsocket, conn_flag=None)
                if clientThrdKillId != "":
                        #print('kill')
                        break
                else:
                        global live_users_info
                        global KillThrID
                        global rmvAgntId
                        live_users_info = []
                        KillThrID = ""
                        rmvAgntId = ""
                        thr_arr = []
                        tmp_live_thr_arr = []
                        dept_info = []
                        users_info = []
                        tmpUsersInfo = []
                        dept_query = "select dept_id, dept_email, max_assign_limit,queue_limit,critical_override_limit,policy,dept_phone from departments"
                        logmain.info(str(dept_query))
                        dept_info = queryExecuter(dept_query,"SELECT",db_obj_read,cur_obj_read)
                        logmain.info('result: ' + str(dept_info) + ';  for ' + str(clientId))
                        if dept_info != []:
                                for i in dept_info:
                                        #if (str(i[1]) not in allDept_EmailArr and str(i[-1]) not in allDept_PhoneArr):
                                        #       allDept_EmailArr.append(str(i[1]))
                                        #       allDept_PhoneArr.append(str(i[-1]))
                                        loggedinQry = "select user_id from loggedin_live where dept_id=%s and user_type='agent' and (assign_type='both'or assign_type='ticket')" % (i[0])
                                        logmain.info(loggedinQry)
                                        users_info = queryExecuter(loggedinQry,"SELECT",db_obj_read,cur_obj_read)
                                        logmain.info(str(users_info) + '; for ' + str(clientId))
                                        if users_info != []:
                                                if (i[2] is not None and i[3] is not None and i[4] is not None):
                                                        for live_user in users_info:
                                                                if str(live_user[0]) not in live_users_info:
                                                                        live_users_info.append(str(live_user[0]))
                                                                        tmpUsersInfo.append(str(live_user[0]))
                                                        dept_wise_user_info[str(clientId)+'_'+str(int(i[0]))] = live_users_info
                                                        #thr_arr.append(str(clientId)+'_'+str(int(i[0])))
                                                        if (str(clientId)+'_'+str(int(i[0])) not in alive_thr_arr):
                                                                alive_thr_arr.append(str(clientId)+'_'+str(int(i[0])))
                                                                try:
                                                                        # Added client id to args
                                                                        thr = threading.Thread(target=DeptMngr,args=(i[0],i[1],i[2],i[3],i[4],i[5],i[6],db_obj_write,cur_obj_write, db_obj_read,cur_obj_read,clientId,clientDbName))
                                                                        thr.daemon = True
                                                                        thr.setName(str(clientId)+'_'+str(int(i[0])))
                                                                        thr.start()
                                                                except:
                                                                        logClientMgr.error('Error in starting thread', exc_info=True)
                                                for user_id in live_users_info:
                                                        #print "user_id ...................................... :       "+str(user_id)
                                                        if user_id not in tmpUsersInfo:
                                                                dept_wise_user_info[str(clientId)+'_'+str(int(i[0]))].remove(user_id)

                                                live_users_info=[]
                                        elif users_info == []:
                                                dept_wise_user_info[str(clientId)+'_'+str(int(i[0]))] = []
                                        if bool(dept_wise_user_info[str(clientId)+'_'+str(int(i[0]))]) == False:
                                                if (str(i[1]) not in allDept_EmailArr and str(i[-1]) not in allDept_PhoneArr):
                                                        allDept_EmailArr.append(str(i[1]))
                                                        allDept_PhoneArr.append(str(i[-1]))
                                        else:
                                                if str(i[1]) in allDept_EmailArr:
                                                        allDept_EmailArr.remove(str(i[1]))
                                                if str(i[-1]) in allDept_PhoneArr:
                                                        allDept_PhoneArr.remove(str(i[-1]))
                        z = threading.enumerate()
                        for realThrd in z:
                                if "MainThread" != realThrd.name:
                                        thr_arr.append(str(realThrd.name))
                        if alive_thr_arr != "":
                                for thrName in alive_thr_arr:
                                        if thrName not in thr_arr:
                                                KillThrID = thrName
                                                alive_thr_arr.remove(thrName)
                                                if thrName in dept_wise_user_info.keys():
                                                        if dept_wise_user_info[thrName] == []:
                                                                if thrName in alive_thr_arr:
                                                                        alive_thr_arr.remove(thrName)
                        """
                        for l in range(len(z)):
                                if (z[l].name != "MainThread" and "Client" not in z[l].name):
                                        if z[l].name not in tmp_live_thr_arr:
                                                tmp_live_thr_arr.append(z[l].name)
                        ##print "\n==========="
                        ##print tmp_live_thr_arr
                        ##print "===========\n"
                        if len(tmp_live_thr_arr) == 0:
                                alive_thr_arr = []
                        elif len(tmp_live_thr_arr) != 0:
                                for liveThr in alive_thr_arr:
                                        if liveThr not in tmp_live_thr_arr:
                                                alive_thr_arr.remove(liveThr)
                        ##print "\nHelo"
                        ##print alive_thr_arr
                        ##print "hii\n"
                        ##print thr_arr
                        if alive_thr_arr != "":
                                for l in range(len(alive_thr_arr)):
                                        if alive_thr_arr[l] not in thr_arr:
                                                KillThrID = alive_thr_arr[l]
                                                id_arr.append(alive_thr_arr[l])

                        for ids in id_arr:
                                if ids in alive_thr_arr:
                                        alive_thr_arr.remove(ids)
                        """
                        time.sleep(5)


#=================================== Main Function for ticket assignment ============================
def main():
        cntrl_db,cntrl_cur = dbHandler(""+centralDb+"", dbport=dbport, dbsocket=dbsocket, conn_flag="READ")
        finalClientIDArr = []
        global clientThrdKillId
        while 1:
                        try:
                                        cntrl_db.ping()
                        except:
                                        cntrl_db,cntrl_cur = dbHandler(""+centralDb+"", dbport=dbport, dbsocket=dbsocket, conn_flag="READ")
                        clientThrdKillId = ""
                        dbarr = []
                        allDbArr = []
                        clientInfo = ""
                        tmpClientDbArr = ""
                        tmpClientIDArr = []
                        try:
                                        db_count = cntrl_cur.execute("show databases")
                                        cntrl_db.commit()
                                        if db_count > 0:
                                                        dbarr = cntrl_cur.fetchall()
                                        logmain.info("Running Query show databases")
                        except:
                                        logmain.error("Failed Query show databases", exc_info=True)
                        for alldb in dbarr:
                                        if clientDb in str(alldb[0]):
                                                        allDbArr.append(str(alldb[0]))
                        slquery = "select registration_id from clientRegistrationBasic where status=1 and verificationStatus='VERIFIED' and auto_assign_flag=1";
                        try:
                                        client_count = cntrl_cur.execute(slquery)
                                        cntrl_db.commit()
                                        if client_count > 0:
                                                        clientInfo = cntrl_cur.fetchall()
                                                        print(clientInfo)
                                        logmain.info("Running Query "+slquery)
                        except:
                                        #cntrl_db,cntrl_cur = dbHandler(""+centralDb+"", dbport=dbport, dbsocket=dbsocket, conn_flag="READ")
                                        logmain.error("Failed Query "+slquery, exc_info=True)
                        if clientInfo != "":
                                        for clientinfo in clientInfo:
                                                        clientdb = ""+clientDb+"_"+str(clientinfo[0])
                                                        if clientdb in allDbArr:
                                                                        tmpClientIDArr.append(str(clientinfo[0]))
                                                                        if str(clientinfo[0]) not in finalClientIDArr:
                                                                                        finalClientIDArr.append(str(clientinfo[0]))
                                                                                        #print(finalClientIDArr)
                                                                                        thrd = threading.Thread(target=clientMngr, args=(str(clientinfo[0]),clientdb,dbport,))
                                                                                        thrd.daemon = True
                                                                                        thrd.setName("Client_"+str(clientinfo[0])+"")
                                                                                        thrd.start()
                                                                        else:
                                                                                        ''#logmain.error("Error in Opening Client Manager Thread "+str(clientinfo[0]), exc_info=True)
                                        logmain.info(str(threading.enumerate()))
                                        print (str(threading.enumerate()))
                                        for finalClientId in finalClientIDArr:
                                                        if finalClientId not in tmpClientIDArr:
                                                                        clientThrdKillId = "Client_"+finalClientId
                                                                        finalClientIDArr.remove(finalClientId)
                        time.sleep(5)

if __name__ == "__main__":
                main()

