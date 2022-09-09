from glob import glob
import ConfigParser
import os
import pandas as pd
import sys
###Style sheet for this takes many cues from w3school
##'<link rel="stylesheet" type="text/css" href="masterHTMLCSS.css">'
def WholeHTML(MLScoreFake,RADEC,season,masterTableInfo, outdir):
    config = ConfigParser.ConfigParser()
    if os.path.isfile('./postproc_'+season+'.ini'):
        inifile = config.read('./postproc_'+season+'.ini')[0]

    MajorPlots=['<!DOCTYPE HTML>\n','<html>\n','<link rel="stylesheet" type="text/css" href="../masterHTMLCSS.css">','<body>','<button onclick="topFunction()" id="myBtn" title="Go to top"><img id ="button" src="Arrow.png"></button>','<h1>Season '+season+'</h1>','<div id="majorPlots">','<span title="Fakes: ML Score">','<img src='+MLScoreFake+' width="583" height="350"></span>','<span title="RA and DEC Plot">','<img src ='+RADEC+' width"583" height="350"></span></div>','<ul>']
    masterHTML=open('masterHTML'+season+'.html','w+')
    for line in MajorPlots:
        masterHTML.write(line)
    masterHTML.close()

    statusLines=['<a id="status" href="statusPage'+season+'.html"><font size="10">Status Page</font></a>','</body>','</html>']
    masterHTML=open('masterHTML'+season+'.html','a')
    masterHTML.write(statusLines[0])
    masterHTML.write(statusLines[1])
    masterHTML.close()
    
    #htmls=glob('theProtoATC_'+season+'*.html')
#    htmls=glob(str(outdir)+'/htmls/candidate_*_dp'+str(season)+'.html')

    masterHTML=open('masterHTML'+season+'.html','a')
    
    candInfoTableheaders=['<table id = "candidateTable" align="center"><caption>Click on the header by which you want to sort</caption>','<tr>','<th onclick="sortTable(0)">SNID</th>','<th onclick="sortTable(1)">RA and DEC</th>','<th onclick="sortTable(2)">max ML score</th>','<th onclick="sortTable(3)">First Mag</th>','<th onclick="sortTable(4)">Path to .fits</th>','</tr>']
    #candInfoTableheaders=['<table id = "candidateTable" align="center"><caption>Click on the header by which you want to sort</caption>','<tr>','<th onclick="sortTable(0)">SNID</th>','<th onclick="sortTable(1)">RA and DEC</th>','<th onclick="sortTable(2)">max ML score</th>','<th onclick="sortTable(3)">Path to .fits</th>','</tr>']
    
    for header in candInfoTableheaders:
        masterHTML.write(header)
    masterHTML.close()
    
    # print masterTableInfo keys
    #1/23/20 ag: convert dict to pandas df to sort
    masterdf = pd.DataFrame(masterTableInfo).transpose().reset_index()
    masterdf.columns = ['snid','radec', 'ml', 'mag', 'path']
#    masterdf = masterdf.sort_values('ml',ascending=False)
    masterdf = masterdf.sort(columns=['ml', 'snid'],ascending=False)

    print('Keys for masterTableInfo:')
#    print(str(len(masterTableInfo.keys())))
    print(str(len(masterdf['snid'].values)))

    masterHTML=open('masterHTML'+season+'.html','a')
 #   for html in htmls:
    check = 0.
    check2 = 0.
    check3 = 0.
    for index, row in masterdf.iterrows():
        check += 1.0
        snid = int(row['snid'])
        prob = row['ml']
        mag = row['mag']
        rawImagePath = row['path']
        RAandDEC = row['radec']
        
        html = glob(outdir+'/htmls/candidate_'+str(snid)+'_dp'+str(season)+'.html')
        if not html:
            name = "n/a"
            forpathhtml = 'n/a'
            miniName = 'n/a'
            continue
        else:
            check2 += 1.
            forpathhtml = html[0].split('/')[-1]
            name=forpathhtml.split('.')[0].split('_')[1]
            miniName=int(name[0:]) # snid, so treat as int
            #print("miniName ",miniName)
        #try:
            #if miniName not in list(masterTableInfo.keys()):
        #    if miniName not in masterdf['snid'].values:
        #        print(str(miniName)+' not in masterTableInfo keys.')
        #        continue

        #except AttributeError:
        #    continue
        
        if html =='./masterHTML.html':
            continue
        if html=='./statusPage.html':
            continue
        #if html=='./statusPage'+season+'.html':
        if html=='./PostProc_statusPage'+str(season)+'.html':
            continue
        if html=='./masterHTML'+season+'.html':
            continue
        if html=='./masterHTMLspecial.html':
            continue
        

        ###Making a sortable Table
#        if masterTableInfo != None:
#        if not masterdf.empty:

            #RAandDEC=str(masterTableInfo[miniName][0])
            #prob=str(masterTableInfo[miniName][1])
            #mag = str(masterTableInfo[miniName][2])
            #galDist=str(masterTableInfo[miniName][3])                
        
        row=['<tr>','<td><a href=htmls/'+str(forpathhtml)+'>'+str(name)+'</a></td>','<td>'+str(RAandDEC)+'</td>','<td>'+str(prob)+'</td>','<td>'+ str(mag) +'</td>','<td>'+str(rawImagePath)+'</td>']
        check3 += 1.
        for part in row:
            masterHTML.write(part)
        ###Done with a sortable Table
            
#    if masterTableInfo != None:
    if not masterdf.empty:
        link=['<li><a href='+str(html)+'>'+str(name)+'</a></li>']
        masterHTML.write(link[0])
        lilLine=['</table>','<div align="right"><p>Image credit: Dark Energy Survey</p></div>']
    else:
        lilLine=['</ul>','<div align="right"><p>Image credit: Dark Energy Survey</p></div>']
    masterHTML.write(lilLine[0])
    masterHTML.write(lilLine[1])
    masterHTML.close()

    f=open('tableSortTest.html','r')
    lines=f.readlines()
    f.close()
    masterHTML=open('masterHTML'+str(season)+'.html','a')
    for line in lines[1:]:
        masterHTML.write(line)
    masterHTML.close()

    return 'Functional'
