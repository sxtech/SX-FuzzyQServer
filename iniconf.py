#-*- encoding: gb2312 -*-
import ConfigParser
import string, os, sys
import datetime
import time

class fuzzyQSerIni:
    def __init__(self,confpath = 'fuzzyqserver.conf'):
        self.path = ''
        self.confpath = confpath
        self.cf = ConfigParser.ConfigParser()
        self.cf.read(confpath)

    def getSysConf(self):
        sysconf = {}
        sysconf['listen_port'] = self.cf.getint('SYSSET','listen_port')
        sysconf['charset']     = self.cf.get('SYSSET','charset')
        return sysconf
        
    def getOrcConf(self):
        orcconf = {}
        orcconf['host']   = self.cf.get('ORCSET','host')
        orcconf['user']   = self.cf.get('ORCSET','user')
        orcconf['passwd'] = self.cf.get('ORCSET','passwd')
        orcconf['port']   = self.cf.get('ORCSET','port')
        orcconf['sid']    = self.cf.get('ORCSET','sid')
        orcconf['mincached']      = self.cf.getint('ORCSET','mincached')
        orcconf['maxcached']      = self.cf.getint('ORCSET','maxcached')
        orcconf['maxshared']      = self.cf.getint('ORCSET','maxshared')
        orcconf['maxconnections'] = self.cf.getint('ORCSET','maxconnections')
        orcconf['maxusage']       = self.cf.getint('ORCSET','maxusage')
        return orcconf

    def getMysqlConf(self):
        mysqlconf = {}
        mysqlconf['host']    = self.cf.get('MYSQLSET','host')
        mysqlconf['user']    = self.cf.get('MYSQLSET','user')
        mysqlconf['passwd']  = self.cf.get('MYSQLSET','passwd')
        mysqlconf['port']    = self.cf.getint('MYSQLSET','port')
        mysqlconf['mincached']      = self.cf.getint('MYSQLSET','mincached')
        mysqlconf['maxcached']      = self.cf.getint('MYSQLSET','maxcached')
        mysqlconf['maxshared']      = self.cf.getint('MYSQLSET','maxshared')
        mysqlconf['maxconnections'] = self.cf.getint('MYSQLSET','maxconnections')
        mysqlconf['maxusage']       = self.cf.getint('MYSQLSET','maxusage')
        return mysqlconf

     
if __name__ == "__main__":

    try:
        ftpini = fuzzyQServerIni()
        s= ftpini.getMysqlConf()
        print s
        #s = imgIni.getPlateInfo(PATH2)
        #i = s['host'].split(',')
        #print s
        #disk = s['disk'].split(',')
        #print disk
        #del i
    except ConfigParser.NoOptionError,e:
        print e
        time.sleep(10)
