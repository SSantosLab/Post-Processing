def getInfo(???):
    SEASON=os.environ.get('SEASON')
    NITE=os.environ.get('NITE')
    EXPNUM=os.environ.get('EXPNUM')
    BAND=os.environ.get('BAND')
    CCDNUM=os.environ.get('CCDNUM')
    FIELD=os.environ.get('FIELD')
    Alist=[NITE, SEASON, EXPNUM, BAND, CCDNUM, FIELD]
    return Alist
