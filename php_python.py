# -*- coding: cp936 -*-
import time,sys,os,socket
import signal
import fuzzyqser
import gl
import MySQLdb
import cx_Oracle
from iniconf import fuzzyQServerIni
from helpfunc import HelpFunc
from DBUtils.PooledDB import PooledDB
#from DBUtils.PersistentDB import PersistentDB

# -------------------------------------------------
# 基本配置
# -------------------------------------------------
CHARSET = 'gbk'

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

def orcPool(h,u,ps,pt,s,minc=5,maxc=20,maxs=10,maxcon=100,maxu=1000):
    gl.orcpool = PooledDB(
        cx_Oracle,
        user = u,
        password = ps,
        dsn = "%s:%s/%s"%(h,pt,s),
        mincached=minc,
        maxcached=maxc,
        maxshared=maxs,
        maxconnections=maxcon,
        maxusage=maxu)   
                
# -------------------------------------------------
# 主程序
#    请不要随意修改下面的代码
# -------------------------------------------------
class PhpPython:
    def main(self):
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,'-------------------------------------------'))
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,"- FuzzyQ Service"))
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,"- Time: %s" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))))
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,'-------------------------------------------'))
        
        self.fqsIni = fuzzyQServerIni()
        sysset = self.fqsIni.getSysConf()

        gl.LISTEN_PORT = sysset['listen_port']
        gl.CHARSET     = sysset['charset']
            
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  #TCP/IP
        sock.bind(('', gl.LISTEN_PORT))
        sock.listen(5)

        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,"Listen port: %d" % gl.LISTEN_PORT))
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,"charset: %s" % gl.CHARSET))
        gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,"Server startup..."))

        self.hf = HelpFunc()

        self.loginMysql()
        self.loginOracle()
        
        import process
        
        while 1:
            connection,address = sock.accept()  #收到一个请求

            #print ("client's IP:%s, PORT:%d" % address)
            if gl.QTFLAG == False:
                gl.DCFLAG = False
                break

            # 处理线程
            try:
                process.ProcessThread(connection).start()
            except:
                pass

        del self.hf
        del self.fqsIni

    #登录mysql
    def loginMysql(self):
        mysqlini = self.fqsIni.getMysqlConf()
        try:
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,self.hf.getTime()+'Start to login mysql...'))
            mysqlPool(mysqlini['host'],mysqlini['user'],mysqlini['passwd'],3306,mysqlini['mincached'],mysqlini['maxcached'],mysqlini['maxshared'],mysqlini['maxconnections'],mysqlini['maxusage'])
            gl.MYSQLLOGIN = True
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,self.hf.getTime()+'Login mysql success!'))
            #logging.info('Login mysql success!')
        except Exception,e:
            gl.MYSQLLOGIN = False
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_red,self.hf.getTime()+str(e)))
            time.sleep(15)


    #登录oracle
    def loginOracle(self):
        orcini = self.fqsIni.getOrcConf()
        try:
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,self.hf.getTime()+'Start to login Oracle...'))
            orcPool(orcini['host'],orcini['user'],orcini['passwd'],1521,orcini['sid'],orcini['mincached'],orcini['maxcached'],orcini['maxshared'],orcini['maxconnections'],orcini['maxusage'])
            gl.ORCLOGIN = True
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_green,self.hf.getTime()+'Login Oracle success!'))
            #logging.info('Login mysql success!')
        except Exception,e:
            gl.ORCLOGIN = False
            gl.TRIGGER.emit("<font %s>%s</font>"%(gl.style_red,self.hf.getTime()+str(e)))
            time.sleep(15)


            
if __name__ == '__main__':
    print ("-------------------------------------------")
    print ("- FuzzyQ Service")
    print ("- Time: %s" % time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) )
    print ("-------------------------------------------")

    fqsIni = fuzzyQServerIni()
    mysqlset = fqsIni.getMysqlConf()
    orcset = fqsIni.getOrcConf()

    mysqlPool(mysqlset['host'],mysqlset['user'],mysqlset['passwd'],mysqlset['port'],mysqlset['mincached'],mysqlset['maxcached'],mysqlset['maxshared'],mysqlset['maxconnections'],mysqlset['maxusage'])
    orcPool(orcset['host'],orcset['user'],orcset['passwd'],orcset['port'],orcset['sid'],orcset['mincached'],orcset['maxcached'],orcset['maxshared'],orcset['maxconnections'],orcset['maxusage'])
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  #TCP/IP
    sock.bind(('', LISTEN_PORT))
    sock.listen(5)

    print ("Listen port: %d" % LISTEN_PORT)
    print ("charset: %s" % CHARSET)
    print ("Server startup...")

    import process
    
    while 1:
        print '123'
        connection,address = sock.accept()  #收到一个请求

        #print ("client's IP:%s, PORT:%d" % address)
        if gl.QTFLAG == False:
            gl.DCFLAG = False
            
        # 处理线程
        try:
            process.ProcessThread(connection).start()
        except:
            pass

