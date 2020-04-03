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
    def __init__(self,configFile, myTransect, mySaison, myDepr):
        self.myTransect = myTransect
        self.mySaison   = mySaison
        self.myDepr     = myDepr
        self.configData = self.config_reader(configFile)
        self.cx = self.open_database_connexion()
        self.valid_preselection()
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

    def valid_preselection(self):

        cursor = self.cx.cursor()
        cursor.execute("SELECT transect, saison, num_depr, distance FROM transect_besttrack_matching_1000km_from_3H where transect = '" + self.myTransect + "' and saison = " + str(self.mySaison)+ " and num_depr = " + self.myDepr )
        rows = cursor.fetchall()
        nb_insertions = 0
        for row in rows:
            transect = row[0]
            saison   = row[1]
            num_depr = row[2]
            distance_besttrack_transect = row[3]

            min_max_cyclone_timestamps = self.get_min_max_cyclone_timestamps(saison, num_depr)
            transect_timestamps        = self.get_transect_timestamps(transect)

            index_transect_point = -1
            distance1000_atteinte = 0
            for t in transect_timestamps:
                index_transect_point = index_transect_point + 1
                if min_max_cyclone_timestamps[0] <= t <= min_max_cyclone_timestamps[1]:

                   
                   # dates en clair du début et de fin de la besttrack
                   #-------------------------------------------------------------------------------

                   d_min_max_cyclone_timestamps0 = datetime.datetime.fromtimestamp(min_max_cyclone_timestamps[0]).strftime('%Y-%m-%d %H:%M:%S')
                   d_min_max_cyclone_timestamps1 = datetime.datetime.fromtimestamp(min_max_cyclone_timestamps[1]).strftime('%Y-%m-%d %H:%M:%S')

                   # le point transect de timestamp t est encadré par les valeurs +- 6h ci-dessous
                   #-------------------------------------------------------------------------------

                   ts_borne_inf_cyc = self.configData.cyc_interval*int(t/self.configData.cyc_interval)
                   ts_borne_sup_cyc = self.configData.cyc_interval*int(t/self.configData.cyc_interval) + self.configData.cyc_interval

                   # dates en clair du timestamp du transect et des timestamp besttrack qui l'encadrent
                   #-------------------------------------------------------------------------------

                   d_t= datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
                   d_ts_borne_inf_cyc= datetime.datetime.fromtimestamp(ts_borne_inf_cyc).strftime('%Y-%m-%d %H:%M:%S')
                   d_ts_borne_sup_cyc= datetime.datetime.fromtimestamp(ts_borne_sup_cyc).strftime('%Y-%m-%d %H:%M:%S')

                   # récupération des lat/lon/rventmax des 2 points besttrack encadrant le timestamp du transect
                   #-------------------------------------------------------------------------------

                   [lat_cyc_inf, lon_cyc_inf, rventmax_inf] = self.get_lat_lon_vmax(saison, num_depr, d_ts_borne_inf_cyc)
                   [lat_cyc_sup, lon_cyc_sup, rventmax_sup] = self.get_lat_lon_vmax(saison, num_depr, d_ts_borne_sup_cyc)

                   # récupération du couple lat/lon du transect correspondant au timestamp t
                   #-------------------------------------------------------------------------------

                   [lat_transect, lon_transect] = self.get_latlon_transect(transect, index_transect_point)

                   # distance du point du transect au segment de la besttrack concerné
                   #-------------------------------------------------------------------------------

                   distance = self.get_distance(lat_transect, lon_transect, lat_cyc_inf, lon_cyc_inf, lat_cyc_sup, lon_cyc_sup)

                   if distance <= 1000:
                      distance1000_atteinte = 1
                      [lat_cyc_interp, lon_cyc_interp, rventmax_interp] = self.get_cylone_interpolation(lat_cyc_inf, lat_cyc_sup, lon_cyc_inf, lon_cyc_sup, rventmax_inf, rventmax_sup, ts_borne_inf_cyc, ts_borne_sup_cyc, t)

                      # insertion des données dans la base
                      #-------------------------------------------------------------------------------

                      nb_insertions = nb_insertions + 1
                      cur = self.cx.cursor()
                      sql = "INSERT INTO transect_besttrack_matching_table (transect, saison, num_depr, transect_point_index, transect_timestamp, transect_date, besttrack_date0, besttrack_date1, besttrack_latitude, besttrack_longitude, besttrack_rventmax,distance) VALUES ('" + transect + "'," + str(saison) + "," + str(num_depr) + "," + str(index_transect_point) + "," + str(t) + ",'" + d_t + "','" + d_ts_borne_inf_cyc + "','" + d_ts_borne_sup_cyc + "'," + str(lat_cyc_interp) + "," + str(lon_cyc_interp) + "," + str(rventmax_interp) + "," + str(distance) + ")"
                      cur.execute(sql)
                      self.cx.commit()
                      cur.close()
                   else:
                      if distance1000_atteinte > 0 :
                          break


        cursor.close()
        return


    #-----------------------------------------------------------
    # Interpolations temporelles
    #-----------------------------------------------------------

    def get_cylone_interpolation(self, lat0, lat1, lon0, lon1, r0, r1, t0, t1, t):

        c = (t - t0)/(t1-t0)
        la = c*(lat1 - lat0) + lat0
        lo = c*(lon1 - lon0) + lon0

        if r0>-999 and r1>-99:
           r = c*(r1 - r0) + r0
        else:
           r = -999

        return (la, lo, r)


    #-----------------------------------------------------------
    # distance entre un point du transect et le segment bestrack 
    #-----------------------------------------------------------

    def get_distance (self, lat_transect, lon_transect, lat_cyc_inf, lon_cyc_inf, lat_cyc_sup, lon_cyc_sup):

        try:
            cur = self.cx.cursor()
            sql = "SELECT ST_Distance(ST_GeomFromText('POINT(" + str(lon_transect) + " " + str(lat_transect) + ")',4326)::geography, ST_GeomFromText('LINESTRING(" + str(lon_cyc_inf) + " " + str(lat_cyc_inf) + "," + str(lon_cyc_sup) + " " + str(lat_cyc_sup) + ")',4326)::geography)"
            cur.execute(sql)

            return int(cur.fetchone()[0]/1000)

        except psycopg2.DatabaseError,e:

            print 'Error %s' % e
            sys.exit(1)

    #-----------------------------------------------------------
    # Get lat, lon, of transect point corresponding to t 
    #-----------------------------------------------------------

    def get_latlon_transect(self, transect, idx):
        fh = Dataset(transect)
        retour=[]

        lat = fh.variables['latitude'][idx]
        lon = fh.variables['longitude'][idx]
        retour.append(lat)
        retour.append(lon)

        return retour


    #-----------------------------------------------------------
    # Get lat, lon, ventmax
    #-----------------------------------------------------------

    def get_lat_lon_vmax(self, saison, num_depr, d):

        connection = self.cx
        retour = []

        try:
            cur = connection.cursor()
            sql = "SELECT COALESCE(lat,-999), COALESCE(lon,-999), COALESCE(rayon_ventmax,-999) FROM vcycmdl WHERE saison = " + str(saison) + " AND num_depr = " + str(num_depr) + " AND dat = '" + d + "'"
            cur.execute(sql)

            rows = cur.fetchall()

            if rows:
                for row in rows:
                    retour.append(row[0])
                    retour.append(row[1])
                    retour.append(row[2])
            else:
                retour.append(-999)
                retour.append(-999)
                retour.append(-999)

            return retour
      
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

    #-----------------------------------------------------------
    # Get TimeStamps of transect
    #-----------------------------------------------------------

    def get_transect_timestamps(self, transect):

        timestamps  = []
        fh = Dataset(transect)
        jour = getattr(fh,'day')
        sec_offset = fh.variables['time'][:]

        [a,m,j] = jour.split('-')
        dt = datetime.datetime(int(a),int(m),int(j))
        for s in sec_offset:
            timestamp = (dt - datetime.datetime(1970,1,1)).total_seconds() + s
            timestamps.append(timestamp)

        fh.close()

        return timestamps

    #-----------------------------------------------------------
    # Get min and max TimeStamps of saison/numdepr besttrack
    #-----------------------------------------------------------

    def get_min_max_cyclone_timestamps(self, saison, num_depr):

        cur = self.cx.cursor()
        sql = "SELECT extract(epoch from min(dat)), extract(epoch from max(dat)) FROM vcycgeom_mdl WHERE saison=" + str(saison) + " AND num_depr=" + str(num_depr)
        cur.execute(sql)
        row = cur.fetchone()
        return (row[0],row[1])
        cur.close()
       
#===========================================================
# main
#===========================================================

if __name__ == "__main__":

     configFile = "./conf/matching.conf"
     myTransect = sys.argv[1]
     mySaison   = sys.argv[2]
     myDepr     = sys.argv[3]
     myMatching = Matching(configFile, myTransect, mySaison, myDepr)
