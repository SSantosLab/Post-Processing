from glob import glob
import ConfigParser
import os
###Style sheet for this takes many cues from w3school
##'<link rel="stylesheet" type="text/css" href="masterHTMLCSS.css">'
def WholeHTML(MLScoreFake,RADEC,season,masterTableInfo):
    config = ConfigParser.ConfigParser()
    if os.path.isfile('./postproc_'+season+'.ini'):
        inifile = config.read('./postproc_'+season+'.ini')[0]
    triggerid=config.get('general','triggerid')

    MajorPlots=['<!DOCTYPE HTML>\n','<html>\n','<link rel="stylesheet" type="text/css" href="masterHTMLCSS.css">','<body>','<button onclick="topFunction()" id="myBtn" title="Go to top"><img id ="button" src="Arrow.png"></button>','<h1>Season '+season+'</h1>','<p><a id = "Dillion" href="http://des-ops.fnal.gov:8080/desgw/Triggers/'+triggerid+'/'+triggerid+'_trigger.html"><p></p>LIGO GW Triggers<p>DESGW EM Followup</p></a></p>','<div id="majorPlots">','<span title="Fakes: ML Score">','<img src='+MLScoreFake+' width="583" height="350"></span>','<span title="RA and DEC Plot">','<img src ='+RADEC+' width"583" height="350"></span></div>','<ul>']
    masterHTML=open('masterHTML'+season+'.html','w+')
    for line in MajorPlots:
        masterHTML.write(line)
    masterHTML.close()
    
    htmls=glob('theProtoATC_'+season+'*.html')

    masterHTML=open('masterHTML'+season+'.html','a')
    
    candInfoTableheaders=['<table id = "candidateTable" align="center"><caption>Click on the header by which you want to sort</caption>','<tr>','<th onclick="sortTable(0)">SNID</th>','<th onclick="sortTable(1)">RA and DEC</th>','<th onclick="sortTable(2)">Probability</th>','<th onclick="sortTable(3)">Host Galaxy Distance</th>','</tr>']
    
    for header in candInfoTableheaders:
        masterHTML.write(header)
    masterHTML.close()

    masterHTML=open('masterHTML'+season+'.html','a')
    for html in htmls:
        
        name=html.split('.')[0].split('_')[-1]
        miniName=name[2:]
        print(miniName)
        
        try:
            if miniName not in list(masterTableInfo.keys()):
                continue

        except AttributeError:
            continue
        
        if html =='./masterHTML.html':
            continue
        if html=='./statusPage.html':
            continue
        if html=='./statusPage'+season+'.html':
            continue
        if html=='./masterHTML'+season+'.html':
            continue
        if html=='./masterHTMLspecial.html':
            continue
        

        ###Making a sortable Table
        if masterTableInfo != None:
            
            RAandDEC=str(masterTableInfo[miniName][0])
            prob=str(masterTableInfo[miniName][1])
            galDist=str(masterTableInfo[miniName][2])
            row=['<tr>','<td><a href='+html+'>'+name+'</a></td>','<td>'+RAandDEC+'</td>','<td>'+prob+'</td>','<td>'+galDist+'</td>']
            for part in row:
                masterHTML.write(part)
        ###Done with a sortable Table
        else:
            link=['<li><a href='+html+'>'+name+'</a></li>']
            masterHTML.write(link[0])
            
    if masterTableInfo != None:
        lilLine=['</table>','<div align="right"><p>Image credit: Dark Energy Survey</p></div>']
    else:
        lilLine=['</ul>','<div align="right"><p>Image credit: Dark Energy Survey</p></div>']
    masterHTML.write(lilLine[0])
    masterHTML.write(lilLine[1])
    masterHTML.close()

    f=open('tableSortTest.html','r')
    lines=f.readlines()
    f.close()
    masterHTML=open('masterHTML'+season+'.html','a')
    for line in lines[1:]:
        masterHTML.write(line)
    masterHTML.close()

    statusLines=['<a id="status" href="statusPage'+season+'.html"><font size="10">Status Page</font></a>','</body>','</html>']
    masterHTML=open('masterHTML'+season+'.html','a')
    masterHTML.write(statusLines[0])
    masterHTML.write(statusLines[1])
    masterHTML.close()
    
    return 'Functional'
