def ExtracTarFiles(tar):
    ###get tar files                                                           
    ####Get the distinguishing number at the end of the tar file               
    tarsplit=tar.split('/')
    tarlen=len(tarsplit)
    quality=tarsplit[tarlen-1]
    definingQuality=quality.split('.')[0] #stamp                               
    specificGifAndFitsDir='GifAndFits'+definingQuality+'/'
    ####Use or make a dir in which to put the tar files                        
    if not os.path.isdir(specificGifAndFitsDir):
        os.makedirs(specificGifAndFitsDir)
    lilTar=tarfile.open(tar)
    lilTar.extractall(members=gif_files(lilTar), path = specificGifAndFitsDir)

    return lilTar
