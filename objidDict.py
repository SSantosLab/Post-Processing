def getBandsandField(lines):
    band=[]
    field=[]
    for line in lines:
        if line.split(' ')[0]=='OBS:':
            ##get bands
            bandy=str(line.split(' ')[5])
            band.append(bandy)
            ##get field
            fieldy=str(line.split(' ')[6])
            field.append(fieldy)

def objidDict(objid,mjd,band,field,flux,fluxer,photflag,photprob,zpflux,psf,skysig,skysig_t,gain,xpix,ypix,nite,expnum,ccdnum):
    objidDict={}
    for i in range(len(objid)):
        objidDict[str(int(objid[i]))]=[str(int(mjd[i])),band[i],field[i],str(int(flux[i])),str(int(fluxer[i])),str(int(photflag[i])),str(int(photprob[i])),str(int(zpflux[i])),str(int(psf[i])),str(int(skysig[i])),str(int(skysig_t[i])),str(int(gain[i])),str(int(xpix[i])),str(int(ypix[i])),str(int(nite[i])),str(int(expnum[i])),str(int(ccdnum[i]))]
