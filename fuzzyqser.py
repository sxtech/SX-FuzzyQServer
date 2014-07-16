# -*- coding: cp936 -*-
import os,sys
import glob
import time,datetime
import MySQLdb
import operator
import itertools
import gl
from orcdb import Orc
from mysqldb import Mysql

class FuzzyQSer:
    def __init__(self):
        self.orc   = Orc()
        self.mysql = Mysql()

        self.table = 'traffic'

    def __del__(self):
        del self.orc
        del self.mysql

    def builtHphm(self,hphm):
        hphm += '_'
        area = 1
        flag = ''  
        count = 0
        _count = 0
        wordflag = True
        plate_l = []

        for i in hphm:
            if i == '%':
                if flag == '':
                    area += 1
                    count +=1
                elif flag == '%':
                    pass
                elif flag == '_':
                    count -= _count
                    _count = 0
                    area += 1
                    if len(plate_l) == 0:
                        count +=1
                else:
                    if area == 1:
                        if count == 1:
                            pass
                        else:
                            plate_l.append((1,area,count,flag+'_',10+count))
                    else:
                        plate_l.append((1,area,count,flag+'_',count))
                    #plate_l.append((1,area,count,flag+'_'))
                    area +=1
                wordflag = False
            elif i == '_':
                if flag == '':
                    count += 1
                    _count += 1
                elif flag == '%':
                    pass
                elif flag == '_':
                    if wordflag:
                        count += 1
                        _count += 1
                else:
                    if area == 1:
                        if count == 1:
                            pass
                        else:
                            plate_l.append((1,area,count,flag+'_',10+count))
                    else:
                        plate_l.append((1,area,count,flag+'_',count))
                    count +=1
                    _count +=1

            else:
                if flag == '':
                    count += 1
                elif flag=='%':
                    count += 1
                elif flag=='_':
                    if wordflag == False:
                        count -= _count
                    count += 1
                else:
                    if area == 1:
                        if count == 1:
                            pass
                        else:
                            plate_l.append((2,area,count,flag+i,count+30))
                    else:
                        plate_l.append((2,area,count,flag+i,count+20))
                    count +=1
                wordflag = True
                _count = 0
            flag = i

        return sorted(plate_l,key=lambda x:x[4],reverse=True)

    def createSql(self,plate_l,hphm,style):
        num_l = []
        if len(plate_l)>=2:
            s_hphm = (plate_l[0][3],plate_l[1][3])
            if plate_l[0][1] == plate_l[1][1]:
                if plate_l[0][0] == 2:   
                    if plate_l[0][1] == 1:
                         num_l.append((plate_l[0][2],plate_l[1][2]))
                    else:
                        for i in itertools.izip(self.num2str(plate_l[0][2],9),self.num2str(plate_l[1][2],9)):
                            num_l.append(i)
                else:
                    if plate_l[0][1] == 1:
                        s_hphm = (plate_l[0][3],)
                        num_l.append((plate_l[0][2],))
                    else:
                        pass
            else:
                #area == 1
                if plate_l[0][1] == 1:
                    if plate_l[0][0] == 2 and plate_l[1][0] == 2:
                        for i in range(plate_l[1][2],9):
                            num_l.append((plate_l[0][2],i))
                    elif plate_l[0][0] == 2 and plate_l[1][0] == 1:
                        num_l.append((plate_l[0][2],))
                    else:
                        s_hphm = (plate_l[0][3],)
                        num_l.append((plate_l[0][2],))
                else:
                    if plate_l[0][0] == 2 and plate_l[1][0] == 2:
                        d = plate_l[0][2] - plate_l[1][2]
                        for i in range(plate_l[1][2],9-d):
                            for j in range(i+d,9):
                                num_l.append((j,i))
                    elif plate_l[0][0] == 2 and plate_l[1][0] == 1:
                        s_hphm = (plate_l[0][3],)
                        for i in range(plate_l[0][2],9):
                            num_l.append((i,))
    ##                    elif plate_l[0][1] == 1:
    ##                        s_hphm = (plate_l[0][3],)
    ##                        num_l.append((plate_l[0][2],))
                    else:
                        s_hphm = ()
                        num_l = ()
            return self.createSql3(num_l,s_hphm,hphm,style)
        elif len(plate_l)==1:
            s_hphm = (plate_l[0][3],)
            if plate_l[0][1] == 1:
                s_hphm = (plate_l[0][3],)
                num_l.append((plate_l[0][2],))
            else:
                s_hphm = ()
                num_l = ()
            return self.createSql3(num_l,s_hphm,hphm,style)
        else:
            return None

    def num2str(self,num1,num2):
        num_str = ''
        for i in range(num1,num2):
            num_str += str(i)
        return num_str

    def createSql3(self,num_l,s_hphm,carnum,style):
        strsql = ""
        col = 'id,p1'
            
        count = 0
        if num_l ==():
            strsql ="select %s from platecode where p1 like '%s%s'"%(col,carnum,'%')
        else:
            for i in num_l:
                if count>0:
                    strsql += ' union all '
                strsql += "select %s from platecode where "%col
                for j in range(len(i)):
                    if s_hphm[j][:1] != '_':
                        if j>0:
                            strsql += ' and '
                        opt = '='
                        if s_hphm[j][1:2] == '_':
                            opt = ' like '
                        strsql += "p%s%s'%s'"%(i[j],opt,s_hphm[j])
                strsql += " and p1 like '%s%s'"%(carnum,'%')
                count += 1
        return strsql

    def getFuzzyID(self,hphm,t1,t2,style=0):
        t1_ = t1[:10]
        t2_ = t2[:10]
        rt1 = datetime.datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
        rt2 = datetime.datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")

        s = self.orc.getFuzzyHphm(hphm,t1_,t2_)

        if s == []:
            sql = self.getSql(hphm,t1,t2,rt1,rt2,t1_,t2_,style)
            if sql == None:
                return -2
            else:
                fuzzy_id = self.setFuzzyID3(sql,time.strftime('%Y-%m-%d %H:%M:%S'),hphm,t1_,t2_,t2)
                return fuzzy_id
        elif s[0]['TIMEFLAG'] > rt2:
            return s[0]['ID']
        elif s[0]['TIMEFLAG'] < (s[0]['ENDDATE']+datetime.timedelta(days=1,minutes=10)):
            sql = self.getSql(hphm,t1,t2,rt1,rt2,t1_,t2_,style)
            if sql == None:
                return -2
            else:
                self.setFuzzyID2(sql,s[0]['ID'],t2)
                return s[0]['ID']
        else:
            return s[0]['ID']

    def getSql(self,hphm,t1,t2,rt1,rt2,t1_,t2_,style):
        hphm_u = hphm.decode('gbk')
        h_l =  self.builtHphm(hphm_u)
        
        if len(h_l)>=3 or (len(h_l)==2 and hphm_u[0]!='%' and hphm_u[0]!='_'):
            if h_l[0][4]>20 or h_l[1][4]>10:
                return self.joinSql(self.createSql(h_l,hphm_u,style),t1_,t2_).encode('gbk')
            elif rt2-rt1<=datetime.timedelta(days=3):
                return self.joinSql2(hphm_u,t1_,t2_).encode('gbk')
            else:
                return self.joinSql(self.createSql(h_l,hphm_u,style),t1_,t2_).encode('gbk')
        else:
            return None

    def getSql2(self,hphm,t1,t2,style=0):
        t1_ = t1[:10]
        t2_ = t2[:10]
        rt1 = datetime.datetime.strptime(t1, "%Y-%m-%d %H:%M:%S")
        rt2 = datetime.datetime.strptime(t2, "%Y-%m-%d %H:%M:%S")
        hphm_u = hphm.decode('gbk')
        s =  builtHphm(hphm_u)
        
        if len(s)>=3 or (len(s)==2 and hphm_u[0]!='%' and hphm_u[0]!='_'):
            if s[0][4]>20 or s[1][4]>10:
                return self.joinSql(self.createSql(s,hphm_u,style),t1_,t2_).encode('gbk')
            elif rt2-rt1<=datetime.timedelta(days=3):
                return self.joinSql2(hphm_u,t1_,t2_).encode('gbk')
            else:
                return self.joinSql(self.createSql(s,hphm_u,style),t1_,t2_).encode('gbk')
        else:
            return None

    def setFuzzyID3(self,sql,time_,hphm,t1_,t2_,t2):
        values = []
        h_l = self.mysql.getHphmBySql(sql)

        return self.orc.setFuzzyID(time_,hphm,t1_,t2_,t2,h_l)

    def setFuzzyID(self,sql,fuzzy_id):
        values = []
        for i in self.mysql.getHphmBySql(sql):
            values.append((fuzzy_id,i['p1']))
        self.orc.addHphmList(values)

    def setFuzzyID2(self,sql,fuzzy_id,endtime):
        values = []
        for i in self.mysql.getHphmBySql(sql):
            values.append((fuzzy_id,i['p1']))
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        self.orc.setHphmListByFuzzyID(fuzzy_id,values,now,endtime)

    def joinSql(self,sql,t1,t2):
        return "select distinct p.p1 from (%s)as p left join %s as t on p.id=t.platecode_id where t.passtime>='%s' and t.passtime<='%s'"%(sql,self.table,t1,t2)

    def joinSql2(self,hphm,t1,t2):
        return "select distinct p.p1 from (select platecode_id from %s where passtime>='%s' and passtime<='%s')as t left join platecode as p on p.id=t.platecode_id where p.p1 like '%s'"%(self.table,t1,t2,hphm+'%')

def getFuzzyID(hphm,t1,t2,style=0):
    try:
        fqs = FuzzyQSer()
        return fqs.getFuzzyID(hphm,t1,t2,style=0)
    except Exception,e:
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_red,str(e))
        logging.error(str(e))
    
    del fqs

if __name__ == "__main__":
    t1 = datetime.datetime.now()
    hphm = '粤%M%3'
    #print hphm
    #print getSql(hphm,'2013-01-01','2014-06-05')
    s = getFuzzyID(hphm,'2013-01-01 00:00:00','2013-01-4 2:04:48')
    print 'cost time:',datetime.datetime.now()-t1
    print s
    #a = createSql(s,hphm,0)
    #print joinSql(a,'2013-01-01 00:00:00','2013-01-08 0:01:38')
