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
# AddIsobare Class
#===========================================================

class AddIsobare:
    def __init__(self,configFile):
        self.configData   = self.config_reader(configFile)
        self.cx = self.open_database_connexion()
        self.principal()
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
    # Main loop 
    #===========================================================

    def principal(self):

        cursor = self.cx.cursor()
        cursor.execute("SELECT transect, saison, num_depr, transect_point_index, transect_timestamp, transect_date,besttrack_date0, besttrack_date1, besttrack_latitude, besttrack_longitude, besttrack_rventmax, distance, extract(epoch from(besttrack_date0)), extract(epoch from(transect_date)), extract(epoch from(besttrack_date1)) FROM transect_besttrack_matching_table ORDER BY saison, num_depr")
        rows = cursor.fetchall()

        nb_insertions = 0

        for row in rows:
            transect             = row[0]
            saison               = row[1]
            num_depr             = row[2]
            transect_point_index = row[3]
            transect_timestamp   = row[4]
            transect_date        = row[5]
            besttrack_date_inf   = row[6]
            besttrack_date_sup   = row[7]
            besttrack_latitude   = row[8]
            besttrack_longitude  = row[9]
            besttrack_rventmax   = row[10]
            distance             = row[11]
            ts_inf               = row[12]
            ts                   = row[13]
            ts_sup               = row[14]


            isobare_inf = self.get_isobare(saison, num_depr, besttrack_date_inf)
            isobare_sup = self.get_isobare(saison, num_depr, besttrack_date_sup)
            roci        = self.get_roci(isobare_inf, isobare_sup, ts_inf, ts_sup, ts)

            # insertion des données dans la base
            #-------------------------------------------------------------------------------

            nb_insertions = nb_insertions + 1
            cur = self.cx.cursor()
            sql = "INSERT INTO transect_besttrack_matching_with_isobare (transect, saison, num_depr, transect_point_index, transect_timestamp, transect_date, besttrack_date0, besttrack_date1, besttrack_latitude, besttrack_longitude, besttrack_rventmax,distance,roci) VALUES ('" + transect + "'," + str(saison) + "," + str(num_depr) + "," + str(transect_point_index) + "," + str(transect_timestamp) + ",'" + str(transect_date) + "','" + str(besttrack_date_inf) + "','" + str(besttrack_date_sup) + "'," + str(besttrack_latitude) + "," + str(besttrack_longitude) + "," + str(besttrack_rventmax) + "," + str(distance) + "," + str(roci) + ")"

            cur.execute(sql)
            self.cx.commit()
            cur.close()

        cursor.close()
        return


    #-----------------------------------------------------------
    # Interpolations temporelles
    #-----------------------------------------------------------

    def get_roci(self, iso0, iso1, t0, t1, t):
      
        try:
           c = (t - t0)/(t1-t0)

           if iso0>-999 and iso1>-999:
              iso = c*(iso1 - iso0) + iso0
           else:
              iso = -999

           return iso

        except:
           return -999

    #-----------------------------------------------------------
    # Get isobare
    #-----------------------------------------------------------

    def get_isobare(self, saison, num_depr, d):

        connection = self.cx
        retour = []

        try:
            cur = connection.cursor()
            sql = "SELECT COALESCE(diam_der_isobare,-999) FROM vcycmdl WHERE saison = " + str(saison) + " AND num_depr = " + str(num_depr) + " AND dat = '" + str(d) + "'"
            cur.execute(sql)

            rows = cur.fetchall()

            for row in rows:
                if row[0] < 0:
                   retour = -999
                else:
                   retour = row[0]/2.0

            return retour
      
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

#===========================================================
# main
#===========================================================

if __name__ == "__main__":

     configFile = "/home/dmekies/DARDAR.v2/conf/matching.conf"
     myAddIsobare = AddIsobare(configFile)
