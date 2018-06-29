def ZapHTML(Dict,OMDict,theDat): #list of tar files that correspond to observations                                                                           
    Name='theProtoATC'+theDat+'.html'
    htmlYeah=open(Name,'w+')
    topLines=['<!DOCTYPE HTML>\n','<html>\n','<head>','<link rel="stylesheet" type="text/css" href="theProtoAtCStyleSheet.css">','<title> Plots from '+theDat+'</title>\n','<h1>This is the title for '+theDat+'</h1>','\n','</head>\n','<body>','<p> This is what it is about </p>']
    for tag in topLines:
        htmlYeah.write(tag)
    htmlYeah.close()
    for key in list(Dict.keys()):
        mjd=OMDict[key]
        Dict[key].sort
        
        for i in range(0,len(Dict[key]),3):
            keyHole=key[16:-1]
            IdentifyingInfo=Dict[key][i].split('/')[0].split('_')[0]+Dict[key][i].split('/')[0].split('_')[1]+Dict[key][i].split('/')[0].split('_')[2]+Dict[key][i].split('/')[0].split('_')[3]
            Info=IdentifyingInfo[16:-1]
            htmlYeah=open(Name,'a')
            lines=['<div id="gifs">','<span title='+Dict[key][i]+'>','<img src=\''+Dict[key][i]+'\' width="200" height="200"/></span>\n','<span title='+Dict[key][i+1]+'>','<img src=\''+Dict[key][i+1]+'\' width="200" height="200"/></span>\n','<span title='+Dict[key][i+2]+'><img src=\''+Dict[key][i+2]+'\' width="200" height="200"/></span>','</div>']
            for line in lines:
                htmlYeah.write(line)
            htmlYeah.close()
    htmlYeah=open(Name,'a')
    bottomLines=['</body>\n','</head>']
    for line in bottomLines:
        htmlYeah.write(line)
    htmlYeah.close()

    return
