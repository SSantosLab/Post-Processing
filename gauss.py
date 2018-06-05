import numpy as np
import matplotlib.pyplot as plt

mu = 0.025
sigma = 0.7258

g = np.random.normal(mu, sigma, 51208)

print np.mean(g)
print np.std(g)

obins = np.linspace(-3,3,100)

print obins
#count, bins, ignored = plt.hist(g, obins)
print plt.hist(g, bins=obins)

#plt.plot(bins, 1/(sigma * np.sqrt(2 * np.pi)) * np.exp( - (bins - mu)**2 / (2 * sigma**2) ),linewidth=2, color='r')

plt.show()
