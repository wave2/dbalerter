# -*- coding: iso-8859-1 -*-
"""
    dbAlerter FreeBSD module
 
    @copyright: 2010-2011 Wave2 Limited. All rights reserved.
    @license: BSD License
"""
 
import ctypes, os, subprocess
 
__author__ = "Alan Snelson"
__copyright__ = "Copyright (c) 2010-2011 Wave2 Limited"
 
__revision__ = "$Id$"
__version__ = "0.1.0"

libc = ctypes.CDLL("libc.so")

def get_memory_usage():
    """Return memory usage in megabytes"""

    memory_usage = {'total' : 0, 'used' : 0}
    size = ctypes.c_size_t()
    buf = ctypes.c_int()
    size.value = ctypes.sizeof(buf)
    libc.sysctlbyname("vm.stats.vm.v_free_count", ctypes.byref(buf), ctypes.byref(size), None, 0) 
    v_free_count=buf.value
    libc.sysctlbyname("vm.stats.vm.v_page_count", ctypes.byref(buf), ctypes.byref(size), None, 0)
    v_page_count=buf.value
    libc.sysctlbyname("hw.pagesize", ctypes.byref(buf), ctypes.byref(size), None, 0)
    page_size=buf.value
    memory_usage['total'] = ((v_page_count * page_size) / (1024*1024))
    memory_usage['used'] = memory_usage['total'] - ((v_free_count * page_size) / (1024*1024))
    return memory_usage

   
def get_swap_usage():
    """Return swap usage dictionary in megabytes"""

    swap_usage = {'total' : 0, 'used' : 0}
    swapinfo = subprocess.Popen(['swapinfo', '-m'], shell=False, stdout=subprocess.PIPE)
    swapinfo.stdout.readline()
    for line in swapinfo.stdout:
        swap_usage['used'] += float(line.split()[2])
        swap_usage['total'] += float(line.split()[3])
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
    diskinfo = subprocess.Popen(['df','-i'], shell=False, stdout=subprocess.PIPE)
    diskinfo.stdout.readline()
    for line in diskinfo.stdout:
        disk_usage[line.split()[8]] = { 'filesystem' : line.split()[0], 'size' : int(line.split()[1]), \
'used' : int(line.split()[2]), 'avail' : int(line.split()[3]), 'capacity' : line.split()[4], 'iused' : int(line.split()[5]), 'ifree' : int(line.split()[6]), 'icapacity' : line.split()[7] }
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
