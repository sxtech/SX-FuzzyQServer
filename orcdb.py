# -*- coding: cp936 -*-
import cx_Oracle
import gl

class Orc:
    def __init__(self):
        self.conn  = gl.orcpool.connection()
        self.cur   = self.conn.cursor()
        
        self.table = 'fuzzy_hphm'
        
    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception,e:
            pass

    def addFuzzyHphm(self,time_,hphm,t1,t2,t3):
        try:
            self.cur.execute("insert into %s(TIMEFLAG,HPHM,STARTDATE,ENDDATE,ENDTIME) values(to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%(self.table,time_,hphm,t1,t2,t3))
            self.cur.execute("select FUZZY_HPHM_ID.currval from dual")
            s = self.cur.fetchone()
        except Exception,e:
            raise
        else:
            self.conn.commit()
            return s

    def setFuzzyID(self,time_,hphm,t1,t2,t3,h_l):
        try:
            self.cur.execute("insert into %s(TIMEFLAG,HPHM,STARTDATE,ENDDATE,ENDTIME) values(to_date('%s','yyyy-mm-dd hh24:mi:ss'),'%s',to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'),to_date('%s','yyyy-mm-dd hh24:mi:ss'))"%(self.table,time_,hphm,t1,t2,t3))
            self.cur.execute("select FUZZY_HPHM_ID.currval from dual")
            s = self.cur.fetchone()
            values = []
            for i in h_l:
                values.append((s[0],i['p1']))
            self.cur.executemany("insert into hphm_list(FUZZY_ID,HPHM) values(:1,:2)",values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()
            return s[0]

    def getFuzzyID(self,hphm,t1,t2):
        try:
            self.cur.execute("select ID from %s where hphm='%s' and startdate=to_date('%s','yyyy-mm-dd hh24:mi:ss') and enddate=to_date('%s','yyyy-mm-dd hh24:mi:ss') and flag = 1"%(self.table,hphm,str(t1),str(t2)))
            s = self.cur.fetchone()
        except Exception,e:
            raise
        else:
            self.conn.commit()
            return s

    def getFuzzyHphm(self,hphm,t1,t2):
        try:
            #print "select * from fuzzy_hphm where hphm='%s' and starttime=to_date('%s','yyyy-mm-dd hh24:mi:ss') and endtime=to_date('%s','yyyy-mm-dd hh24:mi:ss') and flag = 1"%(hphm,str(t1),str(t2))
            self.cur.execute("select * from %s where hphm='%s' and startdate=to_date('%s','yyyy-mm-dd hh24:mi:ss') and enddate=to_date('%s','yyyy-mm-dd hh24:mi:ss')"%(self.table,hphm,str(t1),str(t2)))
            #s = self.cur.fetchone()
        except Exception,e:
            raise
        else:
            self.conn.commit()
            return self.rowsToDictList()

    def addHphmList(self,values):
        try:
            self.cur.executemany("insert into hphm_list(FUZZY_ID,HPHM) values(:1,:2)",values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def setHphmListByFuzzyID(self,f_id,values,now,endtime):
        try:
            self.cur.execute("delete from hphm_list where fuzzy_id=%s"%f_id)
            self.cur.execute("update %s set timeflag=to_date('%s','yyyy-mm-dd hh24:mi:ss'), endtime=to_date('%s','yyyy-mm-dd hh24:mi:ss') where id=%s"%(self.table,now,endtime,f_id))
            self.cur.executemany("insert into hphm_list(FUZZY_ID,HPHM) values(:1,:2)",values)
        except Exception,e:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def getV(self):
        try:
            self.cur.execute("select * from v$version")
            s = self.cur.fetchone()
        except Exception,e:
            raise
        else:
            self.conn.commit()
            return s     
        
    def rowsToDictList(self):
        columns = [i[0] for i in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur]

    def orcCommit(self):
        self.conn.commit()

if __name__ == "__main__":

    import datetime,time
    
    orc = Orc()
    #values = []
    orc.setupOrc()
    time = datetime.datetime(2013,3,3,01,01,01)
    s = orc.addFuzzyHphm(time,'粤L123%',time,time)
    print s

    del orc

