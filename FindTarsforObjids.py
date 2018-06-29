def FindTarsforObjids(ListOtarFiles,ListOobjids): #Takes a list of all the tars found for dat file and the list of objids for the dat file
    ListOunusedTarFiles=[] #List of tarFiles not pertaining to an observation in current data file
    ListOTarsforDat=[] #List of tarFiles corresponding to observations in data file
    for File in ListOtarFiles:
        preNumID=File.split('.')[-1].split('/')[0]
        NumID=''
        for char in file:
            try:
                char=int(char)
            except:
                pass
            if isinstance(char,int):
                value+=str(char)
        NumID=int(NumID) #Find the number 
        if any(objid)==NumID:
            ListOTarsforDat.append(File)
        else:
            ListOunusedTarFiles
            
    return ListOTarsforDat
    
