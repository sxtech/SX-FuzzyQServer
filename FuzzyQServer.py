# -*- coding: cp936 -*-
from PyQt4 import QtGui, QtCore
from php_python import PhpPython
import time,sys,gl,os
import logging
import logging.handlers

def initLogging(logFilename):
    """Init for logging"""
    path = os.path.split(logFilename)
    if os.path.isdir(path[0]):
        pass
    else:
        os.makedirs(path[0])
    logging.basicConfig(
                    level    = logging.INFO,
                    format   = '%(asctime)s %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
                    datefmt  = '%Y-%m-%d %H:%M:%S',
                    filename = logFilename,
                    filemode = 'a');

#版本号
def version():
    return 'SX-FuzzyQServer V0.1.0'

 
class MyThread(QtCore.QThread):
    trigger = QtCore.pyqtSignal(str)
 
    def __init__(self, parent=None):
        super(MyThread, self).__init__(parent)
 
    def run(self):
        gl.TRIGGER = self.trigger
        m = dcmain(self.trigger)
        m.mainloop()
        del m

class dcmain:
    def __init__(self,trigger):
        initLogging(r'log\fuzzyqserver.log')

        gl.TRIGGER.emit("<font size=6 font-weight=bold face=arial color=tomato>%s</font>"%('Welcome to use '+version()))
        self.pp = PhpPython()

    def __del__(self):
        del self.pp

    def mainloop(self):
        self.pp.main()

#主窗口程序
class MainWindow(QtGui.QMainWindow):
    def __init__(self, parent=None):  
        super(MainWindow, self).__init__(parent)
        self.resize(650, 450)
        self.setWindowTitle(version())
        
        self.text_area = QtGui.QTextBrowser()
 
        central_widget = QtGui.QWidget()
        central_layout = QtGui.QHBoxLayout()
        central_layout.addWidget(self.text_area)
        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)

        exit = QtGui.QAction(QtGui.QIcon('icons/exit.png'), 'Exit', self)
        exit.setShortcut('Ctrl+Q')
        exit.setStatusTip('Exit application')
        self.connect(exit, QtCore.SIGNAL('triggered()'), QtCore.SLOT('close()'))

        self.statusBar()

        menubar = self.menuBar()
        file = menubar.addMenu('&File')
        file.addAction(exit)
        
        toolbar = self.addToolBar('Exit')
        toolbar.addAction(exit)
        
        self.setWindowIcon(QtGui.QIcon('icons/logo.png'))

        self.count = 0
        
        self.start_threads()
        
    def closeEvent(self, event):
        reply = QtGui.QMessageBox.question(self, 'Message',
            "Are you sure to quit?", QtGui.QMessageBox.Yes |
            QtGui.QMessageBox.No, QtGui.QMessageBox.No)
        if reply == QtGui.QMessageBox.Yes:
            gl.QTFLAG = False
            self.socketClient()
            while gl.DCFLAG == True: 
                time.sleep(1)
            event.accept()
        else:
            event.ignore()
            
    def socketClient(self):
        import socket 
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        sock.connect(('localhost',gl.LISTEN_PORT)) 
        import time 
        #time.sleep(1) 
        sock.send('1')
        #sock.recv(1024) 
        sock.close() 
            
    def start_threads(self):
        self.threads = []              # this will keep a reference to threads
        thread = MyThread(self)        # create a thread
        thread.trigger.connect(self.update_text)  # connect to it's signal
        thread.start()                 # start the thread
        self.threads.append(thread)    # keep a reference         
 
    def update_text(self, message):
        self.count += 1
        if self.count >1000:
            self.text_area.clear()
            self.count = 0
        self.text_area.append(unicode(message, 'gbk'))
        
if __name__ == '__main__':
    app = QtGui.QApplication(sys.argv)
 
    mainwindow = MainWindow()
    mainwindow.show()
 
    sys.exit(app.exec_())
##    pp = PhpPython()
##    try:
##        pp.main()
##    except Exception,e:
##        print e
##        time.sleep(15)
