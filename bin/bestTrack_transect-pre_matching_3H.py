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
# preSelect Class
#===========================================================

class preMatching:
    def __init__(self,configFile):
        self.configData   = self.config_reader(configFile)
        self.config_viewer()
        self.cx = self.open_database_connexion()
        self.ncfiles      = self.list_files()
        self.ncfilesData  = self.ncfiles_reader()
        self.ncfiles_viewer()
        self.cycloneData = self.cyclones_reader()
        self.cyclones_viewer()

        self.selection()
        self.cx.close()

    #-----------------------------------------------------------
    # Config reader
    #-----------------------------------------------------------

    def config_reader(self, configFile):
        Conf   = collections.namedtuple('Conf',['ncfiles_directory','host','database','user','password','saisons','cyc_interval','cyc_dist_max','outputs_directory'])
        Config = ConfigParser.ConfigParser()
        Config.read(configFile)

        outputs_directory = Config.get('GlobalSection', 'outputs_directory')
        ncfiles_directory = Config.get('NetCDFSection', 'ncfiles_directory')
        cyc_interval      = float(Config.get('NetCDFSection', 'cyc_interval'))
        cyc_dist_max      = float(Config.get('NetCDFSection', 'cyc_dist_max'))
        host              = Config.get('PostgresSection', 'host')
        database          = Config.get('PostgresSection', 'database')
        user              = Config.get('PostgresSection', 'user')
        password          = Config.get('PostgresSection', 'password')
        saisons           = Config.get('PostgresSection', 'saisons')

        return Conf(ncfiles_directory, host, database, user, password, saisons, cyc_interval, cyc_dist_max, outputs_directory)

    #-----------------------------------------------------------
    # Config viewer
    #-----------------------------------------------------------

    def config_viewer(self):
        print "-----------------------------------------"
        print "Configuration is:                        "
        print "                                         "
        print "ncfiles_directory:        ",self.configData.ncfiles_directory
        print "host:                     ",self.configData.host
        print "database:                 ",self.configData.database
        print "user:                     ",self.configData.user
        print "saisons:                  ",self.configData.saisons
        print "-----------------------------------------"

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
    # List DARDAR nc files
    #-----------------------------------------------------------

    def list_files(self):
        mypath = self.configData.ncfiles_directory
        return [f for f in listdir(mypath) if isfile(join(mypath, f))]

    #-----------------------------------------------------------
    # nc files reader
    #-----------------------------------------------------------

    def ncfiles_reader(self):
        Conf   = collections.namedtuple('Conf',['ncfiles_names','day','timestamp_mini','timestamp_maxi'])
        ncfiles_names = self.ncfiles
        day = []
        timestamp_mini = []
        timestamp_maxi = []

        for f in ncfiles_names:
            print f
            netcdf_file = self.configData.ncfiles_directory + f
            fh  = Dataset(netcdf_file)
            jour=getattr(fh,'day')
            sec_offset = fh.variables['time'][:]

            day.append(jour)
            [a,m,j]=jour.split('-')
            dt =datetime.datetime(int(a),int(m),int(j))
            timestampmin = (dt - datetime.datetime(1970,1,1)).total_seconds() + sec_offset.min()
            timestampmax = (dt - datetime.datetime(1970,1,1)).total_seconds() + sec_offset.max()
            timestamp_mini.append(timestampmin)
            timestamp_maxi.append(timestampmax)

            fh.close()
        return Conf(ncfiles_names,day,timestamp_mini,timestamp_maxi)

    #-----------------------------------------------------------
    # nc files viewer
    #-----------------------------------------------------------

    def ncfiles_viewer(self):
        print "-----------------------------------------"
        print "nc files Data                            "
        print "                                         "
        for i in range(len(self.ncfilesData.ncfiles_names)):
            print self.ncfilesData.ncfiles_names[i],self.ncfilesData.day[i],self.ncfilesData.timestamp_mini[i],self.ncfilesData.timestamp_maxi[i]
        print "-----------------------------------------"

    #-----------------------------------------------------------
    # Cyclones viewer
    #-----------------------------------------------------------

    def cyclones_viewer(self):
        print "-----------------------------------------"
        print "Cyclones Data   :                        "
        print "                                         "
        print "saison:                          ",self.cycloneData.saison
        print "num_depr:                        ",self.cycloneData.num_depr
        print "nom_cyc:                         ",self.cycloneData.nom_cyc
        print "timestamp_mini:                  ",self.cycloneData.timestamp_mini
        print "timestamp_maxi:                  ",self.cycloneData.timestamp_maxi
        print "-----------------------------------------"

    #-----------------------------------------------------------
    # cyclones reader
    #-----------------------------------------------------------

    def cyclones_reader(self):
        Conf   = collections.namedtuple('Conf',['saison','num_depr','nom_cyc','timestamp_mini','timestamp_maxi'])
        saison         = []
        num_depr       = []
        nom_cyc        = []
        timestamp_mini = []
        timestamp_maxi = []
        connection = self.cx

        try:
            cur = connection.cursor()
            query = "SELECT saison, extract(epoch from min(dat)), extract(epoch from max(dat)), num_depr FROM vcycgeom_mdl WHERE saison IN (" + self.configData.saisons + ") GROUP BY saison, num_depr"
            print query
            cur.execute(query)            
            
            rows = cur.fetchall()

            for row in rows:
                saison.append(row[0])
                timestamp_mini.append(row[1])
                timestamp_maxi.append(row[2])
                num_depr.append(row[3])

            return Conf(saison, num_depr, nom_cyc, timestamp_mini,timestamp_maxi)

        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            sys.exit(1)

    #-----------------------------------------------------------
    # pre selection par recoupement de dates
    #-----------------------------------------------------------

    def selection(self):
        
        connection = self.cx

        for f in range(len(self.ncfilesData.ncfiles_names)):
            print "--------------------------------------------------------------------------------------------------"
            print "BEGIN :", self.ncfilesData.ncfiles_names[f] 
            for c in range(len(self.cycloneData.num_depr)):
                db0 = datetime.datetime.fromtimestamp(self.cycloneData.timestamp_mini[c]).strftime('%Y-%m-%d %H:%M:%S')
                db1 = datetime.datetime.fromtimestamp(self.cycloneData.timestamp_maxi[c]).strftime('%Y-%m-%d %H:%M:%S')
                dt0 = datetime.datetime.fromtimestamp(self.ncfilesData.timestamp_mini[f]).strftime('%Y-%m-%d %H:%M:%S')
                dt1 = datetime.datetime.fromtimestamp(self.ncfilesData.timestamp_maxi[f]).strftime('%Y-%m-%d %H:%M:%S')
                tb0 = self.cycloneData.timestamp_mini[c]
                tb1 = self.cycloneData.timestamp_maxi[c]
                tt0 = self.ncfilesData.timestamp_mini[f]
                tt1 = self.ncfilesData.timestamp_maxi[f]
                transect = self.configData.ncfiles_directory + self.ncfilesData.ncfiles_names[f]
                saison   = self.cycloneData.saison[c]
                num_depr = self.cycloneData.num_depr[c]
                if (tb0<=tt0<=tt1<=tb1) or (tb0<=tt0<=tb1<=tt1) or (tt0<=tb0<=tb1<=tt1) or (tt0<=tb0<=tt1<=tb1):
                    print "PRESELECT: ",self.configData.ncfiles_directory + self.ncfilesData.ncfiles_names[f],self.cycloneData.saison[c],self.cycloneData.num_depr[c], '|', db0, '|', dt0, '|', dt1, '|', db1
                    sql = "INSERT INTO transect_besttrack_matching_3h (transect,saison,num_depr,date_besttrack_0,date_transect_0, date_transect_1, date_besttrack_1) VALUES ( '" + transect + "'," + str(saison) + "," + str(num_depr) + ",'" + db0 + "','" + dt0 + "','" + dt1 + "','" + db1 + "')"
                    print sql
                    cur = connection.cursor()
                    cur.execute(sql)
            print "END :", self.ncfilesData.ncfiles_names[f] 
            print "--------------------------------------------------------------------------------------------------"

        connection.commit()
        cur.close()

#===========================================================
# main
#===========================================================

if __name__ == "__main__":

     configFile = sys.argv[1]
     myPreMatching  = preMatching(configFile)
