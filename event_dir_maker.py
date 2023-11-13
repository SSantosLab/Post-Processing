from argparse import ArgumentParser
import os

def parser():

    p = ArgumentParser()
    p.add_argument("-n", "--name",
                   help="Event ID",
                   type = str,
                   default=None)
    
    p.add_argument("-a", "--alert_type",
                   help="Event alert type (PRELIMINARY, INITIAL, ...)",
                   type = str,
                   default="Initial")

    p.add_argument("-s", "--season",
                   help="Processing season",
                   type = str,
                   default=None)
    return p

if __name__ == "__main__":

    p = parser()
    options = p.parse_args()

    event_name = options.name
    alert_type = options.alert_type
    season = options.season
    current_directory = os.getcwd()

    dirpath = os.path.join(current_directory,event_name, alert_type, season)
    if not os.path.exists(dirpath):
        print(dirpath)
        os.makedirs(dirpath)