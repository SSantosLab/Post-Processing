from glob import glob
###Style sheet for this takes many cues from w3school
##'<link rel="stylesheet" type="text/css" href="masterHTMLCSS.css">'
def WholeHTML(MLScoreFake,RADEC):
    MajorPlots=['<!DOCTYPE HTML>\n','<html>\n','<link rel="stylesheet" type="text/css" href="masterHTMLCSS.css">','<body>','<h1>Exposure List BLAH [Insert Info Here]</h1>','<p>Here, a sensible person would understand that this is a valuable description of lots of information</p>','<div id="majorPlots">','<span title="Fakes: ML Score">','<img src='+MLScoreFake+' width="583" height="350"></span>','<span title="RA and DEC Plot">','<img src ='+RADEC+' width"583" height="350"></span></div>','<ul>']
    masterHTML=open('masterHTML.html','w+')
    for line in MajorPlots:
        masterHTML.write(line)
    masterHTML.close()
    
    htmls=glob('./*.html')

    masterHTML=open('masterHTML.html','a')
    for html in htmls:
        if html =='./masterHTML.html':
            continue
        if html=='./statusPage.html':
            continue
        name=html.split('.')[1].split('_')[-1]
        link=['<li><a href='+html+'>'+name+'</a></li>']
        masterHTML.write(link[0])

    lilLine=['</ul>','<div align="right"><p>Image credit: Dark Energy Survey</p></div>']
    masterHTML.write(lilLine[0])
    masterHTML.write(lilLine[1])
    masterHTML.close()
    
    statusLines=['<a id="status" href="statusPage.html">Status Page</a>','</body>']
    masterHTML=open('masterHTML.html','a')
    masterHTML.write(statusLines[0])
    masterHTML.write(statusLines[1])
    masterHTML.close()
    
    return 'Magic!'
