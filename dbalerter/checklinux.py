# -*- coding: iso-8859-1 -*-
"""
    dbAlerter Linux module
 
    @copyright: 2010-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""
 
import os, subprocess
 
__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2010-2011 Wave2 Limited"
 
__revision__ = "$Id$"
__version__ = "0.1.0"

def get_memory_usage():
    """Return memory usage in megabytes"""

    memory_usage = {'total' : 0, 'used' : 0}
    meminfo = subprocess.Popen(['free', '-m'], shell=False, stdout=subprocess.PIPE)
    meminfo.stdout.readline()
    total_used = meminfo.stdout.readline()
    memory_usage['total'] = total_used.split()[1]
    memory_usage['used'] = total_used.split()[2]
    return memory_usage


def get_swap_usage():
    """Return swap usage dictionary in megabytes"""

    swap_usage = {'total' : 0, 'used' : 0}
    swapinfo = subprocess.Popen(['free', '-m'], shell=False, stdout=subprocess.PIPE)
    swapinfo.stdout.readline()
    swapinfo.stdout.readline()
    swapinfo.stdout.readline()
    total_used = swapinfo.stdout.readline()
    swap_usage['total'] += float(total_used.split()[1])
    swap_usage['used'] += float(total_used.split()[2])
    return swap_usage


def get_mount_point(path):
    """Strip mount point from path"""

    path = os.path.abspath(path)
    while path != os.path.sep:
      if os.path.ismount(path):
        return path
      path = os.path.abspath(os.path.join(path, os.pardir))
    return path


def get_disk_usage():
    """Return disk usage in dictionary"""

    disk_usage = {}
    diskinfo = subprocess.Popen(['df','-P'], shell=False, stdout=subprocess.PIPE)
    diskinfo.stdout.readline()
    for line in diskinfo.stdout:
        disk_usage[line.split()[5]] = { 'filesystem' : line.split()[0], 'size' : int(line.split()[1]), \
'used' : int(line.split()[2]), 'avail' : int(line.split()[3]), 'capacity' : line.split()[4] }
    diskinfo = subprocess.Popen(['df','-i','-P'], shell=False, stdout=subprocess.PIPE)
    diskinfo.stdout.readline()
    for line in diskinfo.stdout:
        disk_usage[line.split()[5]].update( { 'iused' : int(line.split()[2]), 'ifree' : int(line.split()[3]), 'icapacity' : line.split()[4] } )
    return disk_usage


def get_mount_usage(paths):
    """Check mount point disk usage"""

    mount_usage = {}
    for mount, stats in get_disk_usage().items():
        for path in paths:
            if (mount == get_mount_point(path)):
                mount_usage[path] = stats
    return mount_usage


def get_cpu_usage(pid):
    """Return process cpu usage"""

    cpuusage = subprocess.Popen(['ps','-o', 'pcpu', '-p', str(pid)], shell=False, stdout=subprocess.PIPE)
    cpuusage.stdout.readline()
    return float(cpuusage.stdout.readline().rstrip())
