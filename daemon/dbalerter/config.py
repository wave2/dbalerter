# -*- coding: iso-8859-1 -*-
"""
    dbAlerter configuration module

    @copyright: 2008-2010 Wave2 Limited. All rights reserved.
    @license: BSD License
"""

import ConfigParser, sys, time
import notify

__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2008-2010 Wave2 Limited"

__revision__ = "$Id$"
__version__ = "0.1.1"

cfg = None
required_options = ['dbalerter_log', 'mysql_username', 'mysql_password']
default_options = {'check_interval' : '42', 'smtp_server' : 'localhost', \
'mysql_hostname' : 'localhost', 'mysql_port' : '3306', 'mysql_basedir_threshold' : '75', \
'mysql_connection_usage_threshold' : '75',  'mysql_cpu_usage_threshold' : '75', \
'mysql_datadir_threshold' : '75', 'mysql_open_files_threshold' : '75', \
'mysql_plugindir_threshold' : '75', 'mysql_tmpdir_threshold' : '75', \
'os_swap_threshold' : '75', 'os_memory_threshold' : '75'}

def initialise(config_file):
    """Initialise configuration module"""

    global cfg, required_options
    try:
        cfg = ConfigParser.ConfigParser()
        cfg.readfp(open(config_file))

        #Check for required section
        if (not cfg.has_section('dbAlerter')):
            raise ConfigParser.NoSectionError('dbAlerter')

        #Initialise configuration defaults
        for option, value in default_options.items():
            if (not cfg.has_option('dbAlerter', option)):
                cfg.set('dbAlerter', option, value)

        #Check for required options
        for option in required_options:
            if (not cfg.has_option('dbAlerter', option)):
                raise ConfigParser.NoOptionError(option, 'dbAlerter')
    except IOError:
        notify.notify("Error", 'Failed to open config file: ' + config_file, 'Failed to open config file: ' + config_file)
        sys.exit(1)
    except ConfigParser.NoSectionError:
        sys.exit(1)
    except ConfigParser.NoOptionError:
        sys.exit(1)


def get (section, option):
    """Return option from config file"""

    global cfg

    try:
        return cfg.get(section, option)
    except ConfigParser.NoOptionError:
        notify.notify('Error','Missing option in dbAlerter config file: ' + option, 'Option missing from dbAlerter config: ' + option + "\n\nPlease fix and restart dbAlerter daemon.")
        time.sleep(5)
        sys.exit(1)


def set (section, option, value):
    """Set option within config"""

    global cfg

    return cfg.set(section, option, value)


def has_option (section, option):
    """Check if config file has option"""

    global cfg
    return cfg.has_option(section, option)
