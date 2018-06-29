
def findHostGala(GalFile):
    def getKey(item): ###Sort funtion courtesy of someone on stackexchange
        return item[1]
    snidDict={}
    hostTxt=open(GalFile)
    HostLines=hostTxt.readlines()
    hostTxt.close()
    for line in HostLines:
        newLine=line.split(' ')
        if newLine[0]=='SN:':
            bestLine=[]
            for element in newLine:
                if element!='':
                    bestLine.append(element)
            if bestLine[1] not in list(snidDict.keys()):
                snidDict[bestLine[1]]=[]
            if bestLine[4].split('-')[0]==bestLine[4]:
                bestie=(bestLine[3],bestLine[-2],bestLine[4],bestLine[5])
            else:
                bestie=(bestLine[3],bestLine[-2],bestLine[4].split('-')[0],'-'+bestLine[4].split('-')[1])
            if bestie==('N/A', 'N/A','N/A', 'N/A'):
                bestie=('N/A', '0','N/A', 'N/A')
            snidDict[bestLine[1]].append(bestie)
            sorted(snidDict[bestLine[1]], key=getKey)
    return snidDict
