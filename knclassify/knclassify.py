# A function to use the start-up script

import os
import subprocess

def source(script, update=1):
    pipe = subprocess.Popen(". %s > /dev/null; env" % script, stdout=subprocess.PIPE, shell=True)
    data = pipe.communicate()[0]
    env={}
    for line in data.splitlines():
        #print(line)                                                                                                         
        splt1=line.split('=',1)
        if splt1[0] == 'BASH_FUNC_setup()':
            splt1[1] = '() {  eval `$EUPS_DIR/bin/eups_setup           "$@"`  \n}'
            #print(splt1[1])                                                                                                 
        if splt1[0] == 'BASH_FUNC_unsetup()':
            splt1[1] = '() {  eval `$EUPS_DIR/bin/eups_setup --unsetup "$@"`  \n}'
            #print(splt1[1])                                                                                                 
        if splt1[0] == 'BASH_FUNC_module()':
            splt1[1]='() {  eval `/usr/bin/modulecmd bash $*`   \n}'
            #print(splt1[1])                                                                                                 
        if splt1[0] =='}':
            continue

        #print(splt1)                                                                                                        
        env[splt1[0]]=splt1[1]

    if update:
        os.environ.update(env)

source('./setup_env.sh')

os.system('python classifier.py')
