# -*- coding: iso-8859-1 -*-
"""
    dbAlerter MySQL module

    @copyright: 2008-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""

import math, MySQLdb, re, time, sys
import checkos, config, notify
from string import lower

__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2008-2011 Wave2 Limited"

__revision__ = "$Id$"
__version__ = "0.2.0"

last_error = time.localtime()
last_slow_query = time.localtime()

db = None
auto_increment_state = {}
process_list_state = {}
security_state = {'ANONACCOUNT' : 0, 'EMPTYPASS' : 0}
warning_state = {'CONNECTIONS' : 0, 'BASEDIR_USAGE' : 0, 'BASEDIR_IUSAGE' : 0, \
'CPU_USAGE' : 0, 'DATADIR_USAGE' : 0, 'DATADIR_IUSAGE' : 0, 'OPEN_FILES' : 0, \
'PLUGIN_DIR_USAGE' : 0, 'PLUGIN_DIR_IUSAGE' : 0, 'SLAVEIO' : 0, 'SLAVESQL' : 0, \
'SLAVEPOS' : 0, 'TMPDIR_USAGE' : 0, 'TMPDIR_IUSAGE' : 0}
global_status = {'SLOW_QUERIES' : 0, 'MAX_USED_CONNECTIONS' : 0, 'UPTIME' : 0}
statistics = {'ERROR' : 0, 'WARNING' : 0, 'INFO' : 0, 'SLOWQ' : 0, 'MAXCONN' : 0, 'MAX_OPEN_FILES' : 0}
variables = {'BASEDIR' : '', 'DATADIR' : '', 'HOSTNAME' : '', 'LOG_ERROR' : '', 'PID' : 0, 'PID_FILE' : '', \
'PLUGIN_DIR' : '', 'OPEN_FILES_LIMIT' : 0, 'SERVERID' : 0, 'SLOW_QUERY_LOG' : '','SLOW_QUERY_LOG_FILE' : '', \
'TMPDIR' : '', 'VERSION' : '', 'LOG_OUTPUT' : '', 'VERSION_COMMENT' : '', 'MAX_CONNECTIONS' : 0}


def initialise():
    """Initialise MySQL module"""

    global db, global_status, statistics, variables

    db = connect()
    cursor=db.cursor(MySQLdb.cursors.DictCursor)

    #Obtain list of MySQL server variables
    cursor.execute("""SELECT * FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES""")
    rows = cursor.fetchall ()
    for row in rows:
        #Store Basedir Path
        if (row['VARIABLE_NAME'] == 'BASEDIR'):
            variables['BASEDIR'] = row['VARIABLE_VALUE']
        #Store Datadir Path
        if (row['VARIABLE_NAME'] == 'DATADIR'):
            variables['DATADIR'] = row['VARIABLE_VALUE']
        #Store Hostname
        if (row['VARIABLE_NAME'] == 'HOSTNAME'):
            variables['HOSTNAME'] = row['VARIABLE_VALUE']
        #Store Error Log Path
        if (row['VARIABLE_NAME'] == 'LOG_ERROR'):
            variables['LOG_ERROR'] = row['VARIABLE_VALUE']
        #Store Open Files Limit
        if (row['VARIABLE_NAME'] == 'OPEN_FILES_LIMIT'):
            variables['OPEN_FILES_LIMIT'] = row['VARIABLE_VALUE']
        #Store Pid_file Path
        if (row['VARIABLE_NAME'] == 'PID_FILE'):
            variables['PID_FILE'] = row['VARIABLE_VALUE']
        #Store Plugin_dir Path
        if (row['VARIABLE_NAME'] == 'PLUGIN_DIR'):
            variables['PLUGIN_DIR'] = row['VARIABLE_VALUE']
        #Store Tmpdir Path
        if (row['VARIABLE_NAME'] == 'TMPDIR'):
            variables['TMPDIR'] = row['VARIABLE_VALUE']
        #Store MySQL version
        if (row['VARIABLE_NAME'] == 'VERSION'):
            variables['VERSION'] = row['VARIABLE_VALUE']
        #Store MySQL version comment
        if (row['VARIABLE_NAME'] == 'VERSION_COMMENT'):
            variables['VERSION_COMMENT'] = row['VARIABLE_VALUE']

    #Update MySQL pid
    variables['PID'] = int(file(variables['PID_FILE'],'r').read().strip())


    #initialise Statistics
    cursor.execute("""SHOW GLOBAL STATUS""")
    rows = cursor.fetchall ()
    for row in rows:
        #Current Connection Usage
        if (row['Variable_name'] == 'Threads_connected'):
            statistics['MAXCONN'] = int(row['Value'])

        #Current Open Files Usage
        if (row['Variable_name'] == 'Open_files'):
            statistics['MAX_OPEN_FILES'] = int(row['Value'])

        #Max Used Connections (Since server start)
        if (row['Variable_name'] == 'Max_used_connections'):
            global_status['MAX_USED_CONNECTIONS'] = int(row['Value'])

        #Slow Queries (Since server start)
        if (row['Variable_name'] == 'Slow_queries'):
            global_status['SLOW_QUERIES'] = int(row['Value'])

    cursor.close()
    return db

def cleanup():
    """Close connections and perform any cleanup"""

    global db

    db.close()


def check(stats=False):
    """Perform MySQL checks.

    Keyword arguments:
    stats -- produce statistics report post check

    """

    global db, statistics
    try:
        db.ping()
        #OS Checks
        if (checkos.supported):
            check_cpu_usage()
            check_disk_usage()
        #MySQL Checks
        update_variables()
        check_status()
        check_auto_increment()
        check_anonymous_accounts()
        check_empty_passwords()
        check_functionality()
        check_error_log()
        check_process_list()
        check_slow_query_log()
        check_slave_status()
        #Produce statistics report?
        if (stats):
            statistics_report()
    except MySQLdb.Error, (error,description):
        if error == 2006:
            notify.notify('Error','(' + str(error) + ') ' + description, 'Error (' + str(error) + ') - ' + description)
            retry_count=0
            while 1:
                try:
                    #Create connection to database
                    db=MySQLdb.connect(host=config.get('dbAlerter', 'mysql_hostname'), port=int(config.get('dbAlerter', 'mysql_port')), user=config.get('dbAlerter', 'mysql_username'), passwd=config.get('dbAlerter', 'mysql_password'))
                    #Update MySQL pid
                    variables['PID'] = int(file(variables['PID_FILE'],'r').read().strip())
                    notify.notify('Info', 'MySQL Server Back Online', 'MySQL Server Back Online')
                    statistics['INFO'] += 1
                    break
                except MySQLdb.Error, (error,description):
                    if (error == 2003 or error == 2002):
                        if (retry_count == 5):
                            notify.log_notify('Error', '(' + str(error) + ') ' + description)
                            retry_count=0
                        else:
                            retry_count+=1
                        time.sleep(int(config.get('dbAlerter','check_interval')))
        else:
            notify.notify('Error','(' + str(error) + ') ' + description, 'Error (' + str(error) + ') - ' + description + "\n\ndbAlerter Shutdown")
            notify.log_notify('Shutdown')
            time.sleep(5)
            sys.exit(1)
    except:
        notify.log_notify('Shutdown')
        raise
        time.sleep(5)
        sys.exit(1)



def connect():
    """Connect to MySQL server"""

    try:
        #Create connection to database
        db=MySQLdb.connect(host=config.get('dbAlerter', 'mysql_hostname'), port=int(config.get('dbAlerter', 'mysql_port')), user=config.get('dbAlerter', 'mysql_username'), passwd=config.get('dbAlerter', 'mysql_password'))
    except MySQLdb.Error, (error,description):
        #Access denied error
        if (error==1045):
            notify.notify('Error','(' + str(error) + ') - ' + description, 'Error (' + str(error) + ') - ' + description + "\n\n\nDid you remember to grant the correct privileges?\n\nGRANT PROCESS, SELECT, REPLICATION CLIENT, SHOW DATABASES, SUPER ON *.* TO  'mysqluser'@'localhost' IDENTIFIED BY 'mysqluser_password';\n\nGRANT CREATE, INSERT, DELETE, DROP ON dbAlerter.* TO 'mysqluser'@'localhost' IDENTIFIED BY 'mysqluser_password';")
            notify.log_notify('Shutdown')
            sys.exit(1)
        #No database selected error
        elif (error==1046):
            pass
        else:
            notify.notify('Error','(' + str(error) + ') - ' + description, 'Error (' + str(error) + ') - ' + description + "\n\ndbAlerter Shutdown")
            notify.log_notify('Shutdown')
            sys.exit(1)
    return db


def check_anonymous_accounts():
    """Check for anonymous accounts"""

    global db, security_state, statistics
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT User, Host FROM mysql.user WHERE User=''""")
    rows = cursor.fetchall ()
    if (cursor.rowcount > 0):
        anonymous_accounts = "The following anonymous accounts were detected:\n\n"
        for row in rows:
            anonymous_accounts += row['User'] + "'@'" + row['Host'] + "\n"
        notify.stateful_notify(True, security_state, 'ANONACCOUNT', 'Security', 'Anonymous accounts detected', anonymous_accounts + "\nPlease see: http://dev.mysql.com/doc/refman/5.1/en/default-privileges.html (Securing the initial MySQL Accounts) for details on how to secure these accounts.")
    else:
        notify.stateful_notify(False, security_state, 'ANONACCOUNT', 'Info', 'No anonymous accounts detected', "No anonymous accounts detected.")
    cursor.close()


def check_cpu_usage():
    """Check MySQL CPU usage"""

    global variables

    cpu_usage = float(checkos.get_cpu_usage(variables['PID']))
    threshold = float(config.get('dbAlerter','mysql_cpu_usage_threshold'))
    if (cpu_usage > threshold):
        notify.stateful_notify(True, warning_state, 'CPU_USAGE', 'Warning', 'CPU utilisation threshold crossed', 'CPU utilisation for MySQL process (' + str(variables['PID']) + ') is currently ' + str(cpu_usage) + '% (Threshold currently set to ' + str(threshold) + '%)')
        statistics['WARNING'] += 1
    else:
        notify.stateful_notify(False, warning_state, 'CPU_USAGE', 'Info', 'CPU utilisation returned below threshold', 'CPU utilisation for MySQL process (' + str(variables['PID']) + ') is currently ' + str(cpu_usage) + '% (Threshold currently set to ' + str(threshold) + '%)')


def check_disk_usage():
    """Check MySQL disk usage"""

    global variables

    mount_usage = checkos.get_mount_usage([variables['BASEDIR'], variables['DATADIR'], variables['PLUGIN_DIR'], variables['TMPDIR']])
    for mount in mount_usage.keys():
        mount_capacity = int(mount_usage[mount]['capacity'].replace('%',''))
        inode_capacity = int(mount_usage[mount]['icapacity'].replace('%',''))
        if (mount == variables['BASEDIR']):
            threshold = int(config.get('dbAlerter', 'mysql_basedir_threshold'))
            params = ['BASEDIR_', 'Installation directory']
        elif (mount == variables['DATADIR']):
            threshold = int(config.get('dbAlerter', 'mysql_datadir_threshold'))
            params = ['DATADIR_', 'Data directory']
        elif (mount == variables['PLUGIN_DIR']):
            threshold = int(config.get('dbAlerter', 'mysql_plugindir_threshold'))
            params = ['PLUGIN_DIR_', 'Plugin directory']
        elif (mount == variables['TMPDIR']):
            threshold = int(config.get('dbAlerter', 'mysql_tmpdir_threshold'))
            params = ['TMPDIR_', 'Temporary directory']
        #Check mount capacity
        if (mount_capacity > threshold):
            notify.stateful_notify(True, warning_state, params[0] + 'USAGE', 'Warning', params[1] + ' usage threshold crossed', params[1] + ' (' + mount + ') usage is currently ' + str(mount_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')
            statistics['WARNING'] += 1
        else:
            notify.stateful_notify(False, warning_state, params[0] + 'USAGE', 'Info', params[1] + ' usage returned below threshold', params[1] + ' (' + mount + ') usage is currently ' + str(mount_capacity) +'% (Threshold currently set to ' + str(threshold) + '%)')
        #Check inode capacity
        if (inode_capacity > threshold):
            notify.stateful_notify(True, warning_state, params[0] + 'IUSAGE', 'Warning', params[1] + ' inode usage threshold crossed', params[1] + ' (' + mount + ') inode usage is currently ' + str(mount_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')
            statistics['WARNING'] += 1
        else:
            notify.stateful_notify(False, warning_state, params[0] + 'IUSAGE', 'Info', params[1] + ' inode usage returned below threshold', params[1] + ' (' + mount + ') inode usage is currently ' + str(mount_capacity) +'% (Threshold currently set to ' + str(threshold) + '%)')


def check_empty_passwords():
    """Check for empty passwords"""

    global db, security_state, statistics
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT User, Host FROM mysql.user WHERE Password=''""")
    rows = cursor.fetchall ()
    if (cursor.rowcount > 0):
        emptyPasswords = "The following accounts do not have passwords configured:\n\n"
        for row in rows:
            emptyPasswords += row['User'] + "'@'" + row['Host'] + "\n"
        notify.stateful_notify(True, security_state, 'EMPTYPASS', 'Security', 'Empty passwords detected', emptyPasswords + "\nPlease see: http://dev.mysql.com/doc/refman/5.1/en/default-privileges.html (Securing the initial MySQL Accounts) for details on how to secure these accounts.")
    else:
        notify.stateful_notify(False, security_state, 'EMPTYPASS', 'Info', 'No Empty Passwords Detected', "No more empty passwords detected.")
    cursor.close()


def check_functionality():
    """Check basic functionality"""

    global db, statistics
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""CREATE DATABASE IF NOT EXISTS dbAlerter""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS dbAlerter.insertcheck (ID int)""")
    cursor.execute("""INSERT INTO dbAlerter.insertcheck VALUES (112233)""")
    cursor.execute("""SELECT * FROM dbAlerter.insertcheck""")
    cursor.execute("""DROP TABLE dbAlerter.insertcheck""")
    cursor.execute("""DROP DATABASE dbAlerter""")
    cursor.execute("""SHOW ERRORS""")
    row = cursor.fetchone ()
    if row:
        notify.notify(row["Level"],  ' (' + str(row["Code"]) + ') - ' + row["Message"], 'Error (' + str(row["Code"]) + ') - ' + row["Message"])
        statistics['ERROR'] += 1
    cursor.close()


def check_error_log():
    """Check MySQL Error Log"""

    re_error = '^(\d\d\d\d\d\d \d\d:\d\d:\d\d) (\[ERROR\]) (.*)$'

    global last_error, statistics, variables
    try:
        log = open(variables['LOG_ERROR'], 'r')
        for line in log:
            error_pattern = re.compile(re_error).match(line)
            if error_pattern:
                if (time.strptime(error_pattern.group(1),"%y%m%d %H:%M:%S") > last_error):
                    last_error=time.strptime(error_pattern.group(1), "%y%m%d %H:%M:%S")
                    notify.notify('Error', error_pattern.group(3), line)
                    statistics['ERROR'] += 1
        log.close()
    except IOError, ioe:
        notify.notify("Error", ioe.str + ' : ' + ioe.filename + "\n")
        statistics['ERROR'] += 1


def check_slow_query_log():
    """Check slow query log - if enabled"""

    re_slow = '^(# Time:) (\d\d\d\d\d\d \d\d:\d\d:\d\d)$'
  
    global db, last_slow_query, statistics, variables
    if (variables['SLOW_QUERY_LOG'] == "ON"):
        slowquery = ''
        if (variables['LOG_OUTPUT'] == "FILE"):
            #File based logging
            try:
                slowquerylog=open(variables['SLOW_QUERY_LOG_FILE'], 'r')
                for line in slowquerylog:
                    slowpattern = re.compile(re_slow).match(line)
                    if slowpattern:
                        if (time.strptime(slowpattern.group(2),"%y%m%d %H:%M:%S") > last_slow_query):
                            last_slow_query=time.strptime(slowpattern.group(2), "%y%m%d %H:%M:%S")
                            slowquery += line
                            continue
                    if (slowquery != ''):
                        slowquery += line
                if (slowquery != ''):
                    notify.notify('Info','Slow Query Encountered',slowquery)
                    statistics['INFO'] += 1
                slowquerylog.close()
            except IOError, ioe:
                notify.notify("Error", ioe.str + ' : ' + ioe.filename, ioe.str + ' : ' + ioe.filename)
                statistics['ERROR'] += 1

        #Table based logging
        if (variables['LOG_OUTPUT'] == "TABLE"):
            cursor=db.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute("""SELECT * FROM mysql.slow_log WHERE start_time > '""" + time.strftime("%Y-%m-%d %H:%M:%S", last_slow_query) + "' ORDER BY start_time ASC LIMIT 100;")
            rows = cursor.fetchall ()
            for row in rows:
                slowquery += '# Time: ' + str(row['start_time']) + "\n# User@Host: " + row['user_host'] + "\n# Query_time: " + str(row['query_time']) + "\n" + row['sql_text'] + "\n\n"
                if (time.strptime(str(row['start_time']),"%Y-%m-%d %H:%M:%S") > last_slow_query):
                    last_slow_query=time.strptime(str(row['start_time']), "%Y-%m-%d %H:%M:%S")
            if (slowquery != ''):
                notify.notify('Info','Slow Query Encountered',slowquery)
                statistics['INFO'] += 1
            cursor.close()


def check_process_list():
    """Check process list for long running commands"""

    global db, process_list_state, statistics
    processes = []
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SHOW FULL PROCESSLIST""")
    rows = cursor.fetchall()
    for row in rows:
        #Notify on commands taking longer than 2 minutes
        if (row['Command'] != 'Sleep' and row['User'] != 'system user' and row['User'] != 'event_scheduler' and row['Time'] > 120):
            processes.append(row['Id'])
            notify.stateful_notify(True, process_list_state, row['Id'], 'Warning', 'Long running process with ID (' + str(row['Id']) + ') detected ', "The following command has been running for over 2 minutes:\n\nId: " + str(row['Id']) + "\nUser: " + row['User'] + "\nHost: " + row['Host'] + "\nSchema: " + (row['db'] or 'NULL') + "\nCommand: " + row['Command'] + "\nTime: " + str(row['Time']) + "\nState: " + row['State'] + "\nInfo: " + row['Info'])
            statistics['WARNING'] += 1
    cursor.close()
    #Cleanup state variable
    for key in process_list_state.keys():
        if not key in processes:
            notify.stateful_notify(False, process_list_state, key, 'Info', 'Long running process with ID (' + str(key) + ') has completed', 'Long running process with ID (' + str(key) + ') has completed.')
            del process_list_state[key]

                
def check_slave_status():
    """Check replication slave status"""
    
    global db, statistics, warning_state
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SHOW SLAVE STATUS""")
    row = cursor.fetchone()
    if row:
        #Check Slave IO
        if row["Slave_IO_Running"]=="No":
            notify.stateful_notify(True, warning_state, 'SLAVEIO', "Warning", "Slave IO has stopped", "Warning - Slave IO has stopped")
            statistics['WARNING'] += 1
        elif row["Slave_IO_Running"]=="Yes":
            notify.stateful_notify(False, warning_state, 'SLAVEIO', "Info", "Slave IO has started", "Info - Slave IO has started")

            #Check Slave SQL
            if row["Slave_SQL_Running"]=="No":
                notify.stateful_notify(True, warning_state, 'SLAVESQL', "Warning", "Slave SQL has stopped", "Warning - Slave SQL has stopped")
                statistics['WARNING'] += 1
            elif row["Slave_SQL_Running"]=="Yes":
                notify.stateful_notify(False, warning_state, 'SLAVESQL', "Info", "Slave SQL has started", "Info - Slave SQL has started")

                #Check Slave Position
                if row["Seconds_Behind_Master"] > 60:
                    notify.stateful_notify(True, warning_state, 'SLAVEPOS', "Warning", "Slave is currently " + str(row["Seconds_Behind_Master"]) + " seconds behind the Master", "Warning - Slave is currently " + str(row["Seconds_Behind_Master"]) + " seconds behind the Master")
                    statistics['WARNING'] += 1
                elif row["Seconds_Behind_Master"] == 0:
                    notify.stateful_notify(False, warning_state, 'SLAVEPOS', "Info", "Slave has caught up with Master", "Info - Slave has caught up with Master")
    cursor.close()


def check_auto_increment ():
    '''Check all auto_increment counters'''

    global auto_increment_state, db, statistics

    threshold = 50

    aitables = []
    cursor1 = db.cursor(MySQLdb.cursors.DictCursor)
    cursor2 = db.cursor(MySQLdb.cursors.DictCursor)
    cursor1.execute("""SELECT TABLE_SCHEMA, TABLE_NAME, AUTO_INCREMENT FROM INFORMATION_SCHEMA.TABLES WHERE AUTO_INCREMENT > 0""")
    tables = cursor1.fetchall ()
    for table in tables:
        cursor2.execute("""SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '""" + table['TABLE_SCHEMA'] + """' AND TABLE_NAME = '""" + table['TABLE_NAME'] + """' AND EXTRA = 'auto_increment'""")
        columns = cursor2.fetchall ()
        for column in columns:
            above_threshold = False
            if (lower(column['COLUMN_TYPE']).find('unsigned') == -1):
                #Process signed data types
                if (lower(column['DATA_TYPE']) == 'tinyint'):
                    currentValue = int((float(100) / float(127))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'smallint'):
                    currentValue = int((float(100) / float(32767))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'mediumint'):
                    currentValue = int((float(100) / float(8388607))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'int'):
                    currentValue = int((float(100) / float(2147483647))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'bigint'):
                    currentValue = int((float(100) / float(9223372036854775807))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
            else:
                #process unsigned data types
                if (lower(column['DATA_TYPE']) == 'tinyint'):
                    currentValue = int((float(100) / float(255))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'smallint'):
                    currentValue = int((float(100) / float(65535))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'mediumint'):
                    currentValue = int((float(100) / float(16777215))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'int'):
                    currentValue = int((float(100) / float(4294967295))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
                elif (lower(column['DATA_TYPE']) == 'bigint'):
                    currentValue = int((float(100) / float(18446744073709551615))*float(table['AUTO_INCREMENT']))
                    if (currentValue > threshold):
                        above_threshold = True
            if (above_threshold):
                aitables.append(table['TABLE_SCHEMA'] + '.' + table['TABLE_NAME'] + '.' + column['COLUMN_NAME'])
                notify.stateful_notify(True, auto_increment_state, table['TABLE_SCHEMA'] + '.' + table['TABLE_NAME'] + '.' + column['COLUMN_NAME'], 'Warning', 'Auto increment threshold crossed on column [' + table['TABLE_SCHEMA'] + '.' + table['TABLE_NAME'] + '.' + column['COLUMN_NAME'] + ']', 'The column [' + column['COLUMN_NAME'] + '] within the table [' + table['TABLE_SCHEMA'] + '.' + table['TABLE_NAME'] + '] crossed the ' + str(threshold) +'% threshold.')
                statistics['WARNING'] += 1
    cursor1.close()
    cursor2.close()
    #Cleanup state variable
    for key in auto_increment_state.keys():
        if not key in aitables:
            notify.stateful_notify(False, auto_increment_state, key, 'Info', 'Auto increment returned below threshold for column [' + key + ']', 'The column [' + key.split('.')[2] + '] within the table [' + key.split('.')[0] + '.' + key.split('.')[1] + '] returned below the ' + str(threshold) + '% threshold.')
            del auto_increment_state[key]


def get_fragmented_tables():
    """Return fragmented tables"""

    global db

    fragmented_tables = {}
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME, DATA_FREE, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('information_schema','mysql') AND DATA_LENGTH > 0 AND DATA_FREE > 0;""")
    rows = cursor.fetchall ()
    for row in rows:
        fragmented_tables[row['TABLE_SCHEMA'] + '.' + row['TABLE_NAME']] = int(math.ceil(row['DATA_FREE'] * (100 / float(row['DATA_LENGTH']))))
    return fragmented_tables


def get_unused_engines():
    """Return unused storage engines"""

    global db

    plugins = {}
    unused_engines = []
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SELECT PLUGIN_NAME, PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_TYPE='STORAGE ENGINE';""")
    rows = cursor.fetchall ()
    for row in rows:
        plugins[row['PLUGIN_NAME']]=row['PLUGIN_STATUS']
    cursor.execute("""SELECT DISTINCT(ENGINE) FROM INFORMATION_SCHEMA.TABLES WHERE ENGINE IS NOT NULL;""")
    rows = cursor.fetchall ()
    for row in rows:
        del plugins[row['ENGINE']]
    for plugin, status in plugins.iteritems():
        if (status=='ACTIVE'):
            unused_engines.append(lower(plugin))
    return unused_engines


def format_uptime(uptime):
    """Convert uptime into Years / Months / Days / Minutes / Seconds"""

    days = int(math.floor(uptime/86400))
    hours = int(math.floor(uptime/3600)%24)
    minutes = int(math.floor(uptime/60)%60)

    return str(days) + ' Days, ' + str(hours) + ' Hours, ' + str(minutes) + ' Minutes'


def reset_statistics():
    """Reset MySQL statistics"""

    global statistics

    statistics['MAXCONN'] = 0
    statistics['MAX_OPEN_FILES'] = 0
    statistics['ERROR'] = 0
    statistics['WARNING'] = 0
    statistics['INFO'] = 0
    statistics['SLOWQ'] = 0


def statistics_report():
    """MySQL server statistics"""

    global auto_increment_state, db, global_status, security_state, statistics, variables
    stats_report = ''
    cursor = db.cursor(MySQLdb.cursors.DictCursor)

    #Auto increment threshold
    if (len(auto_increment_state) > 0):
        stats_report += "=== Auto Incrememnt Threshold Crossed ===\n"
        for key in auto_increment_state.keys():
            stats_report += ' ' + key + "\n"

    #Anonymous Accounts
    if (security_state['ANONACCOUNT'] == 1):
        stats_report += "=== Anonymous Accounts Detected ===\n"
        cursor.execute("""SELECT User, Host FROM mysql.user WHERE User=''""")
        rows = cursor.fetchall ()
        if (cursor.rowcount > 0):
            for row in rows:
                stats_report += ' ' + row['User'] + "'@'" + row['Host'] + "\n"

    #Empty passwords
    if (security_state['EMPTYPASS'] == 1):
        stats_report += "=== Accounts Without Passwords Detected ===\n"
        cursor.execute("""SELECT User, Host FROM mysql.user WHERE Password=''""")
        rows = cursor.fetchall ()
        if (cursor.rowcount > 0):
            for row in rows:
                stats_report += ' ' + row['User'] + "'@'" + row['Host'] + "\n"

    if (stats_report != ''):
        stats_report = "== Outstanding Issues ==\n" + stats_report + "\n"

    #Server Information
    stats_report += "== Server Information ==\n"
    #Uptime
    stats_report += 'Uptime: ' + format_uptime(global_status['UPTIME']) + "\n"
    #MySQL Version
    stats_report += 'MySQL Version: ' + variables['VERSION'] + ' (' + variables['VERSION_COMMENT'] + ")\n"
    #MySQL Server ID
    stats_report += 'Server ID: ' +  str(variables['SERVERID']) + "\n"
    #Base Directory
    stats_report += 'Basedir: ' +  variables['BASEDIR'] + "\n"
    #Data Directory
    stats_report += 'Datadir: ' + variables['DATADIR'] + "\n"
    #Plugin Directory
    stats_report += 'Plugindir: ' + variables['PLUGIN_DIR'] + "\n"
    #Tmp Directory
    stats_report += 'Tmpdir: ' + variables['TMPDIR'] + "\n"
    #Error Log
    stats_report += 'Error Log: ' +  variables['LOG_ERROR'] + "\n"
    #Slow Query Log
    stats_report += 'Slow Query Log: ' +  variables['SLOW_QUERY_LOG_FILE']
    if (variables['SLOW_QUERY_LOG'] == "OFF"):
        stats_report += " (Slow Query Logging Disabled)\n"
    else:
        stats_report += "\n"
    stats_report += "\n"

    #Server Statistics
    stats_report += "== Server Statistics ==\n"
    #Max Connections
    stats_report += 'Max Connections Encountered: ' + str(statistics['MAXCONN']) + "\n"
    #Max Connections (Since server start)
    stats_report += 'Max Connections (Since server start): ' + str(global_status['MAX_USED_CONNECTIONS']) + "\n"
    #Max Open Files
    stats_report += 'Max Open Files Encountered: ' + str(statistics['MAX_OPEN_FILES']) + "\n"
    #Total Info
    stats_report += 'Info Encountered: ' + str(statistics['INFO']) + "\n"
    #Total Warnings
    stats_report += 'Warnings Encountered: ' + str(statistics['WARNING']) + "\n"
    #Total Errors
    stats_report += 'Errors Encountered: ' + str(statistics['ERROR']) + "\n"
    #Slow Queries
    stats_report += 'Slow Queries Encountered: ' + str(statistics['SLOWQ'])
    if (variables['SLOW_QUERY_LOG'] == "OFF"):
        stats_report += " (Slow Query Logging Disabled)\n"
    else:
        stats_report += "\n"
    #Slow Queries (Since server start)
    stats_report += 'Slow Queries (Since server start): ' + str(global_status['SLOW_QUERIES'])
    if (variables['SLOW_QUERY_LOG'] == "OFF"):
        stats_report += " (Slow Query Logging Disabled)\n"
    else:
        stats_report += "\n"
    stats_report += "\n"

    stats_report += "== Metadata Information ==\n"
    #Schemata Count
    cursor.execute("""SELECT count(*) AS SCOUNT FROM information_schema.SCHEMATA""")
    schemas = cursor.fetchall ()
    for schema in schemas:
        stats_report += 'Schemata: ' + str(schema['SCOUNT']) + "\n"
    #Table Count
    cursor.execute("""SELECT count(*) AS TCOUNT FROM information_schema.TABLES""")
    tables = cursor.fetchall ()
    for table in tables:
        stats_report += 'Tables: ' + str(table['TCOUNT']) + "\n"
    #Column Count
    cursor.execute("""SELECT COUNT(*) AS CCOUNT FROM information_schema.COLUMNS""")
    columns = cursor.fetchall ()
    for column in columns:
        stats_report += 'Columns: ' + str(column['CCOUNT']) + "\n"
    #Event Count
    cursor.execute("""SELECT COUNT(*) AS ECOUNT FROM information_schema.EVENTS""")
    events = cursor.fetchall ()
    for event in events:
        stats_report += 'Events: ' + str(event['ECOUNT']) + "\n"
    #Routine Count
    cursor.execute("""SELECT COUNT(*) AS RCOUNT FROM information_schema.ROUTINES""")
    routines = cursor.fetchall ()
    for routine in routines:
        stats_report += 'Routines: ' + str(routine['RCOUNT']) + "\n"
    #Trigger Count
    cursor.execute("""SELECT COUNT(*) AS TCOUNT FROM information_schema.TRIGGERS""")
    triggers = cursor.fetchall ()
    for trigger in triggers:
        stats_report += 'Triggers: ' + str(trigger['TCOUNT']) + "\n"
    #View Count
    cursor.execute("""SELECT COUNT(*) AS VCOUNT FROM information_schema.VIEWS""")
    views = cursor.fetchall ()
    for view in views:
        stats_report += 'Views: ' + str(view['VCOUNT']) + "\n"
    #View Count
    cursor.execute("""SELECT COUNT(*) AS UCOUNT FROM mysql.user""")
    views = cursor.fetchall ()
    for view in views:
        stats_report += 'Users: ' + str(view['UCOUNT']) + "\n"

    #Recommendations
    recommendations=''
    #Fragmented Tables
    fragmented_tables=get_fragmented_tables()
    if (fragmented_tables):
        recommendations += "=== Fragmented Tables ===\nThe following tables have been identified as being fragmented:\n"
        for table, percent in sorted(fragmented_tables.iteritems()):
            recommendations += ' ' + table + ' (' + str(percent) + "%)\n"
        recommendations += "Consider running OTIMIZE TABLE to reclaim unused space.  See http://dev.mysql.com/doc/refman/5.1/en/optimize-table.html for details.\n"

    #Unused Engines
    unused_engines=get_unused_engines()
    if (unused_engines):
        recommendations += "=== Unused Storage Engines ===\nThe following storage engines are not in use and could be disabled:\n"
        for engine in unused_engines:
            recommendations += ' ' + engine + "\n"
        recommendations += "See http://dev.mysql.com/doc/refman/5.1/en/server-plugin-options.html for details on how to disable plugins.\n"

    if (recommendations != ''):
        stats_report += "\n== Recommendations ==\n" + recommendations + "\n"

    cursor.close()

    notify.notify('Stats', 'dbAlerter Statistics for ' + variables['HOSTNAME'], stats_report)



def update_variables():
    """Update server variables"""

    global db, variables
    cursor = db.cursor(MySQLdb.cursors.DictCursor)

    #Obtain list of dynamic MySQL server variables
    cursor.execute("""SELECT * FROM INFORMATION_SCHEMA.GLOBAL_VARIABLES""")
    rows = cursor.fetchall ()
    for row in rows:
        #Store Log Output
        if (row['VARIABLE_NAME'] == 'LOG_OUTPUT'):
            variables['LOG_OUTPUT'] = row['VARIABLE_VALUE']
        #Store Max Connections
        if (row['VARIABLE_NAME'] == 'MAX_CONNECTIONS'):
            variables['MAX_CONNECTIONS'] = row['VARIABLE_VALUE']
        #Store Slow Query Log Status
        if (row['VARIABLE_NAME'] == 'SLOW_QUERY_LOG'):
            variables['SLOW_QUERY_LOG'] = row['VARIABLE_VALUE']
        #Store Slow Query Log Path
        if (row['VARIABLE_NAME'] == 'SLOW_QUERY_LOG_FILE'):
            variables['SLOW_QUERY_LOG_FILE'] = row['VARIABLE_VALUE']

    cursor.close()


def check_status():
    """Check server statistics"""

    global db, global_status, statistics, warning_state
    cursor = db.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("""SHOW GLOBAL STATUS""")
    rows = cursor.fetchall ()
    for row in rows:
        #Check Open File Usage
        if (row['Variable_name'] == 'Open_files'):
            if (int(row['Value']) > statistics['MAX_OPEN_FILES']):
                statistics['MAX_OPEN_FILES'] = int(row['Value'])
            connpct = int(((100 / float(variables['OPEN_FILES_LIMIT'])) * float(row['Value'])))
            if (connpct > int(config.get('dbAlerter','mysql_open_files_threshold'))):
                notify.stateful_notify(True, warning_state, 'OPEN_FILES', 'Warning', 'Open file usage crossed ' + config.get('dbAlerter','mysql_open_files_threshold') + '% threshold', 'Open file crossed ' + config.get('dbAlerter','mysql_open_files_threshold') + '% threshold and is currently ' + str(connpct) + '%')
                statistics['WARNING'] += 1
            else:
                notify.stateful_notify(False, warning_state, 'OPEN_FILES', 'Info', 'Open file usage fell below ' + config.get('dbAlerter','mysql_open_files_threshold') + '% threshold', 'Open file usage fell below ' + config.get('dbAlerter','mysql_open_files_threshold') + '% threshold and is currently ' + str(connpct) + '%')

        #Check Current Connection Usage
        if (row['Variable_name'] == 'Threads_connected'):
            if (int(row['Value']) > statistics['MAXCONN']):
                statistics['MAXCONN'] = int(row['Value'])
            connpct = int(((100 / float(variables['MAX_CONNECTIONS'])) * float(row['Value'])))
            if (connpct > int(config.get('dbAlerter','mysql_connection_usage_threshold'))):
                notify.stateful_notify(True, warning_state, 'CONNECTIONS', 'Warning', 'Connection usage crossed ' + config.get('dbAlerter','mysql_connection_usage_threshold') + '% threshold', 'Connection usage crossed ' + config.get('dbAlerter','mysql_connection_usage_threshold') + '% threshold and is currently ' + str(connpct) + "%")
                statistics['WARNING'] += 1
            else:
                notify.stateful_notify(False, warning_state, 'CONNECTIONS', 'Info', 'Connection usage fell below ' + config.get('dbAlerter','mysql_connection_usage_threshold') + '% threshold', 'Connection usage fell below ' + config.get('dbAlerter','mysql_connection_usage_threshold') + '% threshold and is currently ' + str(connpct) + '%')

        #Check Slow Queries
        if (row['Variable_name'] == 'Slow_queries'):
            slowqs = (int(row['Value']) - global_status['SLOW_QUERIES'])
            if (slowqs > 5):
                notify.notify('Warning', str(slowqs) + " Slow Queries during last " + config.get('dbAlerter','check_interval') + " seconds.", str(slowqs) + " Slow Queries during last " + config.get('dbAlerter','check_interval') + " seconds.")
                statistics['WARNING'] += 1
            global_status['SLOW_QUERIES'] = int(row['Value'])
            statistics['SLOWQ'] += slowqs

        #Server uptime
        if (row['Variable_name'] == 'Uptime'):
            global_status['UPTIME'] = int(row['Value'])

    cursor.close()
