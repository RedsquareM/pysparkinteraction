from datahandler import *
from model import *

if __name__ == '__main__':

    # only 1000 rows 
    path = '../data/data.csv'

    DH = DataHandler()

    DH.readData(path)

    DH.buildDB()

    Model = Model()

    Model.main()