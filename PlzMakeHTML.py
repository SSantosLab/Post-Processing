for datafile in dats:
    tarFilesList=[]
    theDat=datfile.split('/')[-1].split('.')[0]
    Name='theProtoATC'+theDat+'.html'
    htmlYeah=open(Name,'w+')
    topLines=['<!DOCTYPE HTML>\n','<html>\n','<div menuBar="menuBar.html"></div>','<head>','<title> Plots from'+theDat+'</title>\n','<h1>This is the title for'+theDat+'</h1>','\n','</head>\n','<b
\ody>','<p> This is what it is about </p>','<div class="nav-wrapper">','<nav class="nav-menu">','<ul class="clearfix">']
    bottomLines=['</body>\n','</head>']
    for tag in topLines:
        htmlYeah.write(tag)
    htmlYeah.close()
    menuBarTopText=['<nav class="nav-menu">','<ul class="clearfix">']
    menuBar=open('menuBar.html','w+')
    for component in menuBarTopText:
        menuBar.write(component)
    menuBar.close()
    #Do a bunch of stuff 
    tarFiles=glob('/pnfs/des/persistent/gw/exp/'+nitek+'/'+expnumk+'/dp'+mySEASON+'/'+bandk+'_'+ccdnumk+'/stamps_'+nitek+'_*_'+bandk+'_'+ccdnumk+'/*.tar.gz')
    print('Type of tarFiles',type(tarFiles))
    if tarFiles not in tarFilesList:
        tarFilesList.append(tarFiles)
        try:
            tarFile=tarFiles[0]
            anHTML=makeHTML(tarFile,Name)
        except IndexError:
            print('The tarfile you tried to look at does not exist! Maybe you should go and make it.')
    for line in bottomLines:
        htmlYeah.write(line)
    htmlYeah.close()
