def MakeDictforObjidsHere(tarFileFoundforDat,ObjidList):
    ObjidDict={}
    for objid in ObjidList:
        ObjidDict[objid]=[]
    for File in tarFileFoundforDat:
        preNumID=File.split('.')[-2].split('/')[-1]
        NumID=''
        for char in File:
            try:
                char=int(char)
            except:
                pass
            if isinstance(char,int):
                NumID+=str(char)
            NumID=int(NumID)
            if any(list(ObjidDict.keys())):
                preHappy=[]
                preHappy.append(File)
                happyList=Objid[NumID]+preHappy
                ObjidDict[NumID]=happyList

    return ObjidDict
        
