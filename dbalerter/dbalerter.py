#!/usr/bin/env python
# -*- coding: iso-8859-1 -*-
"""
    dbAlerter Daemon

    @copyright: 2008-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""
import signal, socket, sys, os, time, getopt, warnings
import checkmysql, checkos, config, notify

__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2008-2011 Wave2 Limited"

__revision__ = "$Id$"
__version__ = "0.4.0"

alivestamp = time.time()
laststats = time.strftime("%d", time.localtime())

def sighandler (signum, frame):
    shutdown()

def daemonize (stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
    """This forks the current process into a daemon.
    The stdin, stdout, and stderr arguments are file names that
    will be opened and be used to replace the standard file descriptors
    in sys.stdin, sys.stdout, and sys.stderr.
    These arguments are optional and default to /dev/null.
    Note that stderr is opened unbuffered, so
    if it shares a file with stdout then interleaved output
    may not appear in the order that you expect.
    """

    # Do first fork.
    try: 
        pid = os.fork() 
        if pid > 0:
            sys.exit(0) # Exit first parent.
    except OSError, e: 
        sys.stderr.write ("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror)    )
        sys.exit(1)
        
    # Decouple from parent environment.
    os.chdir("/") 
    os.umask(0) 
    os.setsid() 
    
    # Do second fork.
    try: 
        pid = os.fork() 
        if pid > 0:
            sys.exit(0) # Exit second parent.
    except OSError, e: 
        sys.stderr.write ("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror)    )
        sys.exit(1)
        
    # Now I am a daemon!

    #Write pidfile
    if (config.has_option('dbAlerter', 'pid_file')):
        file(config.get('dbAlerter', 'pid_file'),'w+').write(str(os.getpid()))
    
    # Redirect standard file descriptors.
    si = file(stdin, 'r')
    so = file(stdout, 'a+')
    se = file(stderr, 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())

    # Handle SIGTERM
    signal.signal(signal.SIGTERM, sighandler)


def usage ():
    """Print dbAlerter Daemon usage information."""

    print "Usage: dbAlerter [-hpv] [-c Config file] [-p pid file]\r\n"
    print "Options: --help -h                Displays this usage."
    print "         --config -c Path to config file" 
    print "         --pid-file -p Path to save pid file"
    sys.exit(0)

def main ():
    """This is the main function run by the daemon."""

    #Import Global Vars
    global alivestamp
    global laststats

    #Filter Warnings
    warnings.filterwarnings('ignore')

    #Initialise notification module
    notify.initialise()

    #Initialise mysql module
    checkmysql.initialise()

    notify.log_notify('Startup')
    
    while 1:
        #Write to logfile once an hour
        if (time.time() - alivestamp) > 3600:
            alivestamp = time.time()
            notify.log_notify('Info', 'Alive')

        #Send Statistics once a day
        if (time.strftime("%d", time.localtime()) != laststats):
            #Process MySQL Checks
            checkmysql.check(True)
            checkmysql.reset_statistics()
            laststats = time.strftime("%d", time.localtime())
        else:
            #Process OS Checks
            checkos.check()
            #Process MySQL Checks
            checkmysql.check()

        time.sleep(int(config.get('dbAlerter','check_interval')))

    #Cleanup
    checkmysql.cleanup()

def bootup ():
    try:
        opts, args = getopt.getopt (sys.argv[1:], "hc:p:", ["config=", "pid-file="])
    except getopt.GetoptError:
        # print help information and exit:
        usage()
        sys.exit(2)
    verbose = False
    config_file = None
    pidfile = None
    for o, a in opts:
        if o == "-v":
            verbose = True
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        if o in ("-c", "--config"):
            config_file = a
        if o in ("-p", "--pid-file"):
            pidfile = a
    if config_file == None:
        # print help information and exit:
        usage()
        sys.exit(2)

    config.initialise(config_file)

    if pidfile is not None:
        #Command line override
        config.set('dbAlerter', 'pid_file', pidfile)

    if (config.has_option('dbAlerter', 'pid_file')):
        pidfile = config.get('dbAlerter', 'pid_file')
        try:
            pf = file(pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exists. dbAlerter may already be running,\ncheck the process / task list and dbAlerter log file for any\nouststanding issues before removing the existing pidfile.\n"
            sys.stderr.write(message % pidfile)
            sys.exit(1)

    daemonize('/dev/null', config.get('dbAlerter','dbalerter_log'), config.get('dbAlerter','dbalerter_log'))

    main()


def shutdown():
    """Perform any cleanup code before we cease to exist"""

    notify.log_notify('Shutdown')
    #Remove pidfile if exists
    if (config.has_option('dbAlerter', 'pid_file')):
        pidfile = config.get('dbAlerter', 'pid_file')
        if os.path.exists(pidfile): 
            os.remove(pidfile)

    sys.exit(0)
    
if __name__ == "__main__":
    bootup()
