import subprocess
import time
import os
import signal

a = ['1','2','3','4']
b = []

for i in a:
    s1 = subprocess.Popen('echo print '+i+'; sleep 10; echo print again '+i, shell=True)
    b.append(s1)

time.sleep(1)

for j in b:
    print j.pid
    #j.send_signal(signal.SIGTERM)
