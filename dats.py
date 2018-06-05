import os

path = '/data/des41.b/data/rbutler/sb/bench/PostProcessing/makedatatest/makedatafiles/LightCurvesReal'

dats = os.listdir(path)

print len(dats)

dats = [x for x in dats if '.dat' in x]

print len(dats)
