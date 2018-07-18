import time

def statusPage(ListStatuses,season):
    Header=['<!DOCTYPE HTML>\n','<html>\n','<head>','<link rel="stylesheet" type="text/css" href="statusPageCss.css">','<title> Status </title>\n','</head>\n','<body>','<table align="center">','<th bgcolor="#bfbfbf">Step</th>','<th bgcolor = "#bfbfbf">Status</th>']
    stepList=['Setup Environment','Initialize Master List','Finalize Master List','Force Photometry','Host Match','Make Truth Tables','Combine Data Files','Make Plots','Make Proto ATC']
    Date=time.strftime("%Y-%m-%d-%H-%M")
    moreInfo=['<div id="info">','<p>Last Run:     '+Date+'</p>','<p><a id="status" href="output'+season+'.txt">Log</a></p>','</div>']
    
    statusPage=open('statusPage'+season+'.html','w+')
    for line in Header:
        statusPage.write(line)
    statusPage.close()

    statusPage=open('statusPage'+season+'.html','a')
    for x in moreInfo:
        statusPage.write(x)
    statusPage.close()

    ListStatuses=ListStatuses[:-1]

    statusPage=open('statusPage'+season+'.html','a')
    for i in range(len(ListStatuses)):
        status=ListStatuses[i]
        step=stepList[i]
        if status==True:
            stat="Success"
            color="#00FF00"
        else:
            stat="Failed"
            color="#FF0000"
        data=["<tr>","<th>"+step+"</th>","<td bgcolor="+color+"><font color='#000000'>"+stat+"</font></td>","</tr>"]
        for line in data:
            statusPage.write(line)
    statusPage.close()
    closingLines=['</tr>']
    statlins=['</body>','</html>']
    stausPage=open('statusPage'+season+'.html','a')
    for lin in statlins:
        stausPage.write(lin)
    statusPage.close()
    

