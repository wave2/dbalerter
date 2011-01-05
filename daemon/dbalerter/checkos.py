# -*- coding: iso-8859-1 -*-
"""
    dbAlerter OS module

    @copyright: 2010-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""

import os
import config, notify

__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2010-2011 Wave2 Limited"

__revision__ = "$Id$"
__version__ = "0.1.0"

supported = False
warning_state = {'SWAP_USAGE' : 0}

sysname = os.uname()[0]
#FreeBSD?
if (sysname == 'FreeBSD'):
    from checkfreebsd import *
    supported = True
#Linux?
if (sysname == 'Linux'):
    from checklinux import *
    supported = True


def check():
    """Perform OS related checks"""

    global supported

    if (supported):
        check_memory_usage()
        check_swap_usage()

def check_swap_usage():
    """Check host swap usage"""

    swap_usage = get_swap_usage()
    if (swap_usage['total']):
        threshold = int(config.get('dbAlerter', 'os_swap_threshold'))
        swap_capacity = int((100/swap_usage['total']) * swap_usage['used'])
        if (swap_capacity > threshold):
            notify.stateful_notify(True, warning_state, 'SWAP_USAGE', 'Warning', 'OS Swap usage threshold crossed', 'OS Swap usage is currently ' + str(swap_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')
        else:
            notify.stateful_notify(False, warning_state, 'SWAP_USAGE', 'Info', 'OS Swap usage returned below threshold', 'OS Swap usage is currently ' + str(swap_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')

def check_memory_usage():
    """Check host memory usage"""

    memory_usage = get_memory_usage()
    threshold = int(config.get('dbAlerter', 'os_memory_threshold'))
    memory_capacity = int((100/float(memory_usage['total'])) * float(memory_usage['used']))
    if (memory_capacity > threshold):
        notify.stateful_notify(True, warning_state, 'MEMORY_USAGE', 'Warning', 'OS Memory usage threshold crossed', 'OS Memory usage is currently ' + str(memory_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')
    else:
        notify.stateful_notify(False, warning_state, 'MEMORY_USAGE', 'Info', 'OS Memory usage returned below threshold', 'OS Memory usage is currently ' + str(memory_capacity) + '% (Threshold currently set to ' + str(threshold) + '%)')

