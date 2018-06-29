def statusPage(ListStatuses):
    Header=['<!DOCTYPE HTML>\n','<html>\n','<head>','<link rel="stylesheet" type="text/css" href="statusPageCss.css">','<title> Status </title>\n','</head>\n','<body>','<table align="center">','<th bgcolor="#bfbfbf">Step</th>','<th bgcolor = "#bfbfbf">Status</th>']
    stepList=['Setup Environment','Initialize Master List','Finalize Master List','Force Photometry','Host Match','Make Truth Tables','Combine Data Files','Make Plots','Make Proto ATC']

    statusPage=open('statusPage.html','w+')
    for line in Header:
        statusPage.write(line)
    statusPage.close()
    
    statusPage=open('statusPage.html','a')
    for i in range(len(ListStatuses)):
        status=ListStatuses[i]
        step=stepList[i]
        if status==True:
            stat="Success"
            color="#00FF00"
        else:
            stat="Failed"
            color="#FF0000"
        data=["<tr>","<th>"+step+"</th>","<td bgcolor="+color+">"+stat+"</td>","</tr>"]
        for line in data:
            statusPage.write(line)
    statusPage.close()
    closingLines=['</tr>','</body>','</html>']


