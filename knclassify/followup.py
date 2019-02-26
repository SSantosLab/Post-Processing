#Describes properties of the follow-up observations
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time

from events import event

class followup:
    def __init__(self, alert_name):
        self.name = alert_name
        self.lc_list = self.get_lc_list()
        self.events = self.make_events()
        self.season = alert_name[-3:]
        self.trigger_mjd = 0.0
        self.ra = 0.0
        self.dec = 0.0
        self.ra_minus = 0.0
        self.ra_plus = 0.0
        self.dec_minus = 0.0
        self.dec_plus = 0.0
        self.obs_nights = []
        self.field = ''
        self.schema = ''
        self.decam_ra = 0.0
        self.decam_dec = 0.0

    def get_lc_list(followup):
        listfile = open('%s/%s.LIST' %(followup.name, followup.name), 'r')
        lc_list = [x[0:-1] for x in listfile.readlines()]
        listfile.close()
        return lc_list

    def make_events(followup):
        datadir = '%s/data' %followup.name
        event_list = []
        counter = 0.
        for f in followup.lc_list:
            counter += 1
            progress = counter / len(followup.lc_list) * 100
            sys.stdout.write('\rReading Data Files: %.2f / 100.00' %progress) 
            event_list.append(event('%s/%s' %(datadir, f)))
        print ' Done!'
        return event_list


"""
start = time.time()

n = followup('IC170922_dp802')
e = n.events[5]
print e.phot_data

end = time.time()
print 'Run time: ', end - start, 'sec'
"""
