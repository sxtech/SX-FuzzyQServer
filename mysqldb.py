# -*- coding: cp936 -*-
import MySQLdb
import gl

class FMysql:
    def __init__(self):
        self.conn = gl.mysqlpool.connection()
        self.cur  = self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        
    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception,e:
            pass

    def getHphmBySql(self,sql):
        try:
            #print "%s"%sql
            self.cur.execute("%s"%sql)
            s = self.cur.fetchall()
        except MySQLdb.Error,e:
            #print e
            raise
        else:
            self.conn.commit()
            #print s
            return s

    def getHphmInList(self,hphm=[]):
        try:
            strhphm = "','".join(hphm)
            self.cur.execute("select id,p1 from platecode where p1 in('%s')"%strhphm)
            s = self.cur.fetchall()
        except MySQLdb.Error,e:
            #print e
            raise
        else:
            self.conn.commit()
            return s

    def getTest(self):
        self.cur.execute("select p1 from platecode where p3 = '24'")
        s = self.cur.fetchall()
        self.conn.commit()
        return s

        
    def endOfCur(self):
        self.conn.commit()
        
    def sqlCommit(self):
        self.conn.commit()
        
    def sqlRollback(self):
        self.conn.rollback()
            
if __name__ == "__main__":
    from DBUtils.PooledDB import PooledDB
    import datetime
    import threading
    
    def mysqlPool(h,u,ps,pt,minc=5,maxc=20,maxs=10,maxcon=100,maxu=1000):
        gl.mysqlpool = PooledDB(
            MySQLdb,
            host = h,
            user = u,
            passwd = ps,
            db = "fuzzyq",
            charset = "gbk",
            mincached = minc,        #启动时开启的空连接数量
            maxcached = maxc,        #连接池最大可用连接数量
            maxshared = maxs,        #连接池最大可共享连接数量
            maxconnections = maxcon, #最大允许连接数量
            maxusage = maxu)

    mysqlPool('localhost','root','',3306)

    def getPlatecode(startid,endid):
        #start = datetime.datetime.now()
        mysql = Mysql()
        start = datetime.datetime.now()
        sql2 = "select p1 from platecode8 where id >%s and id <=%s and p1 like '__L8B58'"%(startid,endid)
        sql = "select distinct p.p1 from (select id,p1 from platecode8 where id >%s and id <=%s and p1 like '__L8B58')as p left join traffic as t on p.id=t.platecode_id where t.passtime>='2013-01-01' and t.passtime<='2013-01-02'"%(startid,endid)
        s = mysql.getHphmBySql(sql)
        print datetime.datetime.now()-start
        print s
        del mysql
        

    start = datetime.datetime.now()

    t1 = threading.Thread(target=getPlatecode, args=(0,1600000),kwargs={})
    #t2 = threading.Thread(target=getPlatecode, args=(400000,800000),kwargs={})
    #t3 = threading.Thread(target=getPlatecode, args=(800000,1200000),kwargs={})
    #t4 = threading.Thread(target=getPlatecode, args=(1200000,1600000),kwargs={})
    #t5 = threading.Thread(target=getPlatecode, args=(1600000,2000000),kwargs={})
    t1.start()
    #t2.start()
    #t3.start()
    #t4.start()
    #t5.start()
    t1.join()
    #t2.join()
    #t3.join()
    #t4.join()
    #t5.join()
    
    print datetime.datetime.now()-start
    

