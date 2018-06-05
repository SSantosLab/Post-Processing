import numpy as np

sides = '658115.sides'
dist = '658115.dist'
#sides = '506432.sides'

#a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8

a,b,c,d = np.genfromtxt(sides,usecols=(0,1,2,3),unpack=True)
e,f,g,h = np.genfromtxt(dist,usecols=(0,1,2,3),unpack=True)

print len(a),len(e)

print 'sum\t\t0.93*2pi\t0.95*2pi\t'
for i in range(len(a)):
    atana = np.arctan2(np.sqrt(1-a[i]*a[i]),a[i])
    atanb = np.arctan2(np.sqrt(1-b[i]*b[i]),b[i])
    atanc = np.arctan2(np.sqrt(1-c[i]*c[i]),c[i])
    atand = np.arctan2(np.sqrt(1-d[i]*d[i]),d[i])
    print str(atana+atanb+atanc+atand)+'\t'+str(2*np.pi*0.93)+'\t'+str(2*np.pi*0.95)
