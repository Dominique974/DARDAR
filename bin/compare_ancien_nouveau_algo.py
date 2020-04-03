#!/home/dmekies/anaconda2/bin/python
# -*- coding: utf-8 -*-

#===========================================================
# imports
#===========================================================

from netCDF4 import Dataset
from os import listdir
from os.path import isfile, join
import sys
import numpy as np
import ConfigParser
import collections
import datetime 
import calendar
import psycopg2
import datetime

#===========================================================
# Compare Class
#===========================================================

class Compare:
    def __init__(self,configFile):
        self.configData = self.config_reader(configFile)
        self.cx = self.open_database_connexion()
        self.compare()
        self.cx.close()

    #-----------------------------------------------------------
    # Config reader
    #-----------------------------------------------------------

    def config_reader(self, configFile):
        Conf   = collections.namedtuple('Conf',['host','database','user','password','cyc_interval','cyc_dist_max'])
        Config = ConfigParser.ConfigParser()
        Config.read(configFile)

        cyc_interval      = float(Config.get('NetCDFSection', 'cyc_interval'))
        cyc_dist_max      = float(Config.get('NetCDFSection', 'cyc_dist_max'))
        host              = Config.get('PostgresSection', 'host')
        database          = Config.get('PostgresSection', 'database')
        user              = Config.get('PostgresSection', 'user')
        password          = Config.get('PostgresSection', 'password')

        return Conf(host, database, user, password, cyc_interval, cyc_dist_max)

    #-----------------------------------------------------------
    #  Open DataBase Connexion
    #-----------------------------------------------------------

    def open_database_connexion(self):
        connection = None
        try:
            connection = psycopg2.connect(host=self.configData.host, database=self.configData.database, user=self.configData.user, password=self.configData.password)
            return connection

        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

    #===========================================================
    # Main loop for validating preselections
    #===========================================================

    def compare(self):

        cursor = self.cx.cursor()
        cursor.execute("SELECT distinct transect, saison, num_depr FROM transect_besttrack_matching_table")
        #cursor.execute("SELECT distinct ncfile, saison, num_depr FROM matching5_dardar_cyc")
        rows = cursor.fetchall()
        #f = open("/home/dmekies/DARDAR.v2/data/old.data","w")
        i = 0

        for row in rows:
            print(row)
            i = i+1
            transect = row[0].split('/')[4]
            #transect = row[0]
            saison   = row[1]
            num_depr = row[2]

            cursor2 = self.cx.cursor()
            sql = "INSERT INTO transect_besttrack_matching_verif (transect,saison,num_depr) VALUES ('" + transect + "'," + str(saison) + "," + str(num_depr) + ")"
            cursor2.execute(sql)
            self.cx.commit()
            cursor2.close()
            print(str(i) + " " + transect + " " + str(saison) + " " + str(num_depr))
            #f.write(transect + " " + str(saison) + " " + str(num_depr) + "\n")

        #f.close()

        cursor.close()
        return

#===========================================================
# main
#===========================================================

if __name__ == "__main__":

     #configFile = sys.argv[1]
     configFile = "/home/dmekies/DARDAR.v2/conf/matching.conf"
     #myTransect = sys.argv[1]
     #mySaison   = sys.argv[2]
     #myDepr     = sys.argv[3]
     myCompare  = Compare(configFile)
