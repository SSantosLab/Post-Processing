f = open('steplist.txt','r')
a = f.readlines()
f.close()

print a
print

a = map(lambda x: x.strip(), a)

print a
