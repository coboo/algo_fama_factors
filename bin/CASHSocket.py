'''
Created on May 29, 2014

@author: boreaslaw
'''
import datetime
import socket
import threading

class MarketDataSocket(threading.Thread):
    
    def __init__(self, mgr, address, port):
        threading.Thread.__init__(self)
        self.name = "Market Data Socket"
        self.mdSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.mgr = mgr
        self.data = ""
        try:
            self.mdSocket.connect((address, port))
            print "Market Data Connection Established"
        except socket.error, e:
            print e
            print "Market Data Connection Failed"
    
    def extractMessage(self):
        text = self.data.split("\n", 1)
        if len(text) > 1:
            self.data = text[1]
            return text[0]
        else:
            return None
    
    def send(self, request):
        self.mdSocket.send(request + "\n")
        
    def run(self):
        try:
            while True:
                self.data += self.mdSocket.recv(4096)
                while True:
                    message = self.extractMessage()
                    if message is not None:
                        self.mgr._forwardMarketData(message)
                    else:
                        break
        except socket.error, e:
            print "Stop Connection:", e
            
    def destroyConnection(self):
        self.mdSocket.shutdown(2)
        self.mdSocket.close()
            
class FeedDataSocket(threading.Thread):
    
    def __init__(self, mgr, address, port):
        threading.Thread.__init__(self)
        self.name = "Order / Trade Feed Socket"
        self.feedSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.mgr = mgr
        self.data = ""
        try:
            self.feedSocket.connect((address, port))
            print "Feed Connection Established"
        except socket.error, e:
            print e
            print "Feed Connection Failed"
    
    def extractMessage(self):
        text = self.data.split("\n", 1)
        if len(text) > 1:
            self.data = text[1]
            return text[0]
        else:
            return None
        
    def send(self, request):
        self.feedSocket.send(request + "\n")
        
    def run(self):
        try:
            while True:
                self.data += self.feedSocket.recv(4096)
                while True:
                    message = self.extractMessage()
                    if message is not None:
                        self.mgr._forwardFeed(message)
                    else:
                        break
        except socket.error, e:
            print "Stop Connection:", e
    
    def destroyConnection(self):
        self.feedSocket.shutdown(2)
        self.feedSocket.close()
