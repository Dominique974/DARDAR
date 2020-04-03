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
# Matching Class
#===========================================================

class Matching:
    def __init__(self,configFile):

        self.configData   = self.config_reader(configFile)
        self.cx = self.open_database_connexion()
        self.preselection_1000km_from_3H()
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

    #-----------------------------------------------------------
    # Get geometric linestring representing Besttrack
    #-----------------------------------------------------------

    def get_linestring_besttrack(self, saison, num_depr):

        connection = self.cx
        linestring = "ST_GeomFromText('LINESTRING("

        try:
            cur = connection.cursor()
            sql = "SELECT COALESCE(lat,-999), COALESCE(lon,-999) FROM vcycmdl WHERE saison = " + str(saison) + " AND num_depr = " + str(num_depr)
            cur.execute(sql)

            rows = cur.fetchall()

            for row in rows:
                lat = row[0]
                lon = row[1]
                if lat > -999 and lon > -999:
                    linestring = linestring + str(lon) + " " + str(lat) + ","

            linestring = linestring + str(lon) + " " + str(lat) + ")',4326)::geography"
                
            return linestring 
      
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

    #-----------------------------------------------------------
    # Get geometric linestring representing Transect
    #-----------------------------------------------------------

    def get_linestring_transect(self, transect):

        fh = Dataset(transect)
        linestring = "ST_GeomFromText('LINESTRING("

        for i in range(len(fh.variables['latitude'])):
            lat = fh.variables['latitude'][i]
            lon = fh.variables['longitude'][i]
            linestring = linestring + str(lon) + " " + str(lat) + ","

        linestring = linestring + str(lon) + " " + str(lat) + ")',4326)::geography"

        return linestring

    #===========================================================
    # Main loop for validating preselections
    #===========================================================

    def preselection_1000km_from_3H(self):

        cursor = self.cx.cursor()
        cursor.execute("SELECT transect, saison, num_depr FROM transect_besttrack_matching_3H")
        rows = cursor.fetchall()
        x = 0
        for row in rows:
            transect = row[0]
            saison   = row[1]
            num_depr = row[2]

            print "------------------------------------------" 
            print transect,saison, num_depr
            print "------------------------------------------" 

            linestring_besttrack = self.get_linestring_besttrack(saison, num_depr)
            linestring_transect  = self.get_linestring_transect(transect)

            cursor2 = self.cx.cursor()
            sql2 = "SELECT ST_Distance(" + linestring_besttrack + "," + linestring_transect + ")"
            cursor2.execute(sql2)
            distance = int(cursor2.fetchone()[0]/1000)
            cursor2.close()

            if distance <= 1000:
                x = x + 1
                cursor3 = self.cx.cursor()
                sql3 = "INSERT INTO transect_besttrack_matching_1000km_from_3H (transect, saison, num_depr, distance) VALUES ('" + transect + "'," + str(saison) + "," + str(num_depr) + "," + str(distance) + ")"
                cursor3.execute(sql3)
                self.cx.commit()
                cursor3.close()

        cursor.close()

#===========================================================
# main
#===========================================================

if __name__ == "__main__":

     configFile = sys.argv[1]
     myMatching = Matching(configFile)
