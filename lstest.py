import subprocess
import time
import sys

bands1 = 'i,z'
bands2 = 'i'
bands3 = 'all'

bands = bands2

if bands=='all':
    bands = None
else:
    bands = bands.split(',')

print bands
if bands:
    print 'True'


sys.exit()

t1 = time.time()
cmd_out = subprocess.check_output('ls /pnfs/des/persistent/gw/exp/20151227/506423/dp208', shell=True).splitlines()
#cmd = subprocess.Popen('ls /pnfs/des/persistent/gw/exp/20151227/506423/dp208', shell=True, stdout=subprocess.PIPE)
#cmd_out, cmd_err = cmd.communicate()
print cmd_out
for i in cmd_out:
    print i
t2 = time.time()
print
print t2-t1
