def updateStatus(statuslist,season):
    stat=open('status'+season+'.txt','w+')
    for status in statuslist:
        if status == False:
            stat.write('0 \n')
        elif status == True:
            stat.write('1 \n')
        else:
            stat.write(str(status)+'\n')
    stat.close()
    return 'Status updated!'
