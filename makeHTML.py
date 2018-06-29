def makeHTML(location_of_youLonelyGifsAndFits):
    htmlYeah=open('theProtoATC.html','w+')
    location_of_youLonelyGifsAndFits=str(location_of_youLonelyGifsAndFits)
    allTheGifs=glob.glob(location_of_youLonelyGifsAndFits+'/*.gif')
    #allTheFits=glob.glob('/yourLonelyGifsAndFits/*.fits')
    topLines=['<!DOCTYPE HTML>\n','<html>\n','<head>','<title> Plots from GW170814\n, Season 416 </title>\n','</head>\n','<body>']
    bottomLines=['</body>\n','</head>']
    written=htmlYeah(topLines)
    for gif in allTheGifs:
        imgLocation=location_of_youLonelyGifsAndFits+gif
        lines=['<h1>What Is Going on Here?</h1>\n','<p>Description of what is going on here.</p>\n','<h2>'+gif+'</h2>\n','<p>\n','< img src='+imgLocation+'/>\n','</p>\n','<p>Description of it</p>\n','</body>\n','</head>']
        write = written.writelines(lines)
    wrote=write.writelines(bottomLines)
    return "A html file with lots of gifs has been created. You should check it out."
