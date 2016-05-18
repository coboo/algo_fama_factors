'''
Created on May 2, 2014

@author: boreaslaw
'''
import datetime
import threading 
import time
import socket

import CASHSocket
DISABLE_RESET=False

class CASHOrderManager(threading.Thread):
    global mdAddress, mdPort, feedAddress, feedPort
    mdAddress      = "127.0.0.1"
    mdPort         = 55080
    feedAddress    = "127.0.0.1"
    feedPort       = 55081
    
    def __init__(self, name, username, password, beginDate = None, endDate = None):
        self.shutdown = False
        threading.Thread.__init__(self)
        self.name = "CASH Algo API Thread on Strategy " + name
        self.username = username
        self.password = password
        if beginDate == None:
            pass
        else:
            self.beginDate = beginDate
        if endDate == None:
            self.endDate = time.strftime("%Y%m%d")
        else:
            self.endDate = endDate
        self.mdSubscription     = []
        self.oFeedSubscription  = []
        self.tFeedSubscription  = []
        self.pFeedSubscription  = []
        self.pnlperffeedSubscription = []
        self.pendingOrders      = []
        self.inmsgQueue = []
        self.inmsgQueueLock=threading.Lock()
        self._processforwardFeedThread= threading.Thread(target=self._processforwardFeedQueue)
        self._processforwardFeedThread.start()
        self.mdSocket = CASHSocket.MarketDataSocket(self, mdAddress, mdPort)
        self.mdSocket.start()
        self.feedSocket = CASHSocket.FeedDataSocket(self, feedAddress, feedPort)
#        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,loginfeed," + self.username + "," + self.password)
        self.feedSocket.start()

        if not DISABLE_RESET:
          self.mdSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,reset")
          self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,reset")
        
#        self.feedSocket.start()
        print "Manager started"
#       self.inmsgQueue=[] 


    """Method to insert an order
    Caller has to handle when order cannot be placed
    """
    def waitAck(self):
        while(not self.ack):
                time.sleep(0.0001)
#       print "Received Ack"
    def insertOrder(self, order):
        # Save to order array. Send after sending MD
    #    self.pendingOrders.append(order)
        data2 = order.toCSVString()
        self.ack=False
        self.feedSocket.send(data2)
        #self.waitAck()
    def sendACK(self):
        self.mdSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,acknowledgement,0,ihihi")
        return None

    def getWorkingOrders(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_working_orders,today")
        #self.waitAck()
        return None
    
    def getTradeHistory(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_trade_history,today")
        #self.waitAck()
        return None
    
    def getPnL(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_PnL,today")
        #self.waitAck()
        return None
    
    def getDailyPerformance(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_daily_performance,today")
        #self.waitAck()
        return None
    
    def getAccumPerformance(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_accum_performance,20130201,20130205")
        #self.waitAck()
        return None
    
    def getPnLPerformance(self):
        self.ack=False
        self.feedSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,portfolio_get_pnl_performance,today")
        #self.waitAck()
        return None
    
    def subscribeMarketData(self, MarketDataHandler, market, code):
        o = SubscriptionObject(market, code, MarketDataHandler)
        currentTime = datetime.datetime.today()
        try:
            #self.mdSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,subscription," + market + "," + code + "," + self.beginDate + "," + self.endDate+ ",ack_mode=YES")
            self.mdSocket.send(time.strftime("%Y%m%d_%H%M%S_") + "000000,subscription," + market + "," + code + "," + self.beginDate + "," + self.endDate)
            self.mdSubscription.append(o)
            print "Subscription OKAY"
        except socket.error, e:
            print e
            print "Subscription Failed"        
    
    def registerOrderFeed(self, OrderFeedHandler):
        self.oFeedSubscription.append(OrderFeedHandler)
        return
    
    def registerTradeFeed(self, TradeFeedHandler):
        self.tFeedSubscription.append(TradeFeedHandler)
        return
    
    def registerPortfolioFeed(self, PortfolioFeedHandler):
        self.pFeedSubscription.append(PortfolioFeedHandler)
        return
    def registerPnlperffeed(self, PnlPerfHandler):
        self.pnlperffeedSubscription.append(PnlPerfHandler)
    
    def _forwardMarketData(self, data):
#       print data
#       return
        if (data == "ping"):
                return
        if (data == "reset"):
                return

        columns = data.split(",", 1)
        rtmd = MarketData(data)
        for q in self.mdSubscription:
            if rtmd.productCode == q.code:
                q.handler(q.market, q.code, rtmd)
#       self.sendACK()
#         self.feedSocket.send(columns[0] + ",marketfeed," + columns[1])
        for order in self.pendingOrders:
            data = order.toCSVString()
            try:
                self.feedSocket.send(data)
            except socket.error, e:
                print e
                print "Insert Order Failed"
        self.pendingOrders = []
        self.sendACK() 
    def _forwardFeed(self, data):
#       print data
        if (data == "ping"):
                return
        if (data == "reset"):
                return

        columns = data.split(",")
        if (columns[1] == "ack"):
                self.ack=True
                return
        self.inmsgQueueLock.acquire()
        self.inmsgQueue.append(data)
        self.inmsgQueueLock.release()
    def _processforwardFeedQueue(self):
        while (True and not self.shutdown):
                if (len(self.inmsgQueue)>0):
                        self.inmsgQueueLock.acquire()
                        data=self.inmsgQueue.pop(0)
                        self.inmsgQueueLock.release()
                        self._processforwardFeed(data)
                else:
                        time.sleep(0.0001)

    def _processforwardFeed(self, data):
#       print data
        if (data == "ping"):
                return

        columns = data.split(",")
        if columns[1] == "orderfeed":
            of = OrderFeed(data)
            for q in self.oFeedSubscription:
                try:
                    q(of)
                except TypeError, e:
                    print e
         #   self.sendACK()
        elif columns[1] == "tradefeed":
            tf = TradeFeed(data)
            for q in self.tFeedSubscription:
                try:
                    q(tf)
                except TypeError, e:
                    print e
        #    self.sendACK() 
        elif columns[1] == "portfoliofeed":
            pf = PortfolioFeed(data)
            for q in self.pFeedSubscription:
                try:
                    q(pf)
                except TypeError, e:
                    print e
        elif columns[1] == "pnlperffeed":
            pnlf = Pnlperffeed(data)
            for q in self.pnlperffeedSubscription:
                try: 
                   q(pnlf)
                except TypeError, e:
                   print e
        elif columns[1] == "errorfeed":
            pass
        elif columns[1] == "portfolio_end_reply":
            pass
        else:
            columns = data.split(",", 1)
            rtmd = MarketData(data)
            for q in self.mdSubscription:
               if rtmd.productCode == q.code:
                   q.handler(q.market, q.code, rtmd)
#       self.sendACK()
#         self.feedSocket.send(columns[0] + ",marketfeed," + columns[1])
#            for order in self.pendingOrders:
#               data2 = order.toCSVString()
#               try:
#                   self.feedSocket.send(data2)
#               except socket.error, e:
#                   print e
#                   print "Insert Order Failed"
#               time.sleep(0.001)
            self.pendingOrders = []
            self.sendACK()
    
    def run(self):
        pass
    
    def stop(self):
        self.mdSocket.destroyConnection()
        self.feedSocket.destroyConnection()
        self.ack=True
        self.shutdown=True
 
class Order:
    def __init__(self, timestamp, market, productCode, orderID, price, volume, openClose, buySell, action, orderType, orderValidity):
        self.action         = action
        self.buySell        = buySell
        self.market         = market
        self.openClose      = openClose
        self.orderID        = orderID
        self.orderType      = orderType
        self.orderValidity  = orderValidity
        self.price          = price
        self.productCode    = productCode
        self.timestamp      = timestamp
        self.volume         = volume
        
    def toCSVString(self):
        return self.timestamp + ",signalfeed," + self.market + "," + self.productCode + "," \
            + self.orderID + "," + str(self.price) + "," + str(self.volume) + "," + self.openClose + "," \
            + str(self.buySell) + ","+self.action +"," + self.orderType + "," + self.orderValidity 

class SubscriptionObject:
    def __init__(self, market, code, handler):
        self.market     = market
        self.code       = code
        self.handler    = handler

class Queue:
    def __init__(self, price, size):
        self.price = float(price)
        self.size = int(size)

class MarketData:
    def __init__(self, data):
        columns = data.split(",")
#         self.timestamp = datetime.datetime.strptime(columns[0], "%Y%m%d_%H%M%S_%f")
        self.timestamp = columns[0]
        self.productCode = columns[1]
        self.lastPrice = float(columns[2])
        self.lastVolume = float(columns[3])
        self.bidPrice1 = float(columns[5])
        self.bidVol1 = float(columns[6])
        self.bidPrice2 = float(columns[7])
        self.bidVol2 = float(columns[8])
        self.bidPrice3 = float(columns[9])
        self.bidVol3 = float(columns[10])
        self.bidPrice4 = float(columns[11])
        self.bidVol4 = float(columns[12])
        self.bidPrice5 = float(columns[13])
        self.bidVol5 = float(columns[14])

        self.askPrice1 = float(columns[16])
        self.askVol1 = float(columns[17])
        self.askPrice2 = float(columns[18])
        self.askVol2 = float(columns[19])
        self.askPrice3 = float(columns[20])
        self.askVol3 = float(columns[21])
        self.askPrice4 = float(columns[22])
        self.askVol4 = float(columns[23])
        self.askPrice5 = float(columns[24])
        self.askVol5 = float(columns[25])
        self.previous = 0
        self.delta = 0

class TradeFeed:
    def __init__(self, data):
        columns = data.split(",")
        self.buySell            = columns[7]
        self.deleted            = None
        self.errorDescription   = None
        self.market             = columns[2]
        self.orderID            = columns[4]
        self.price              = float(columns[5])
        self.productCode        = columns[3]
        self.source             = int(columns[9])
        self.status             = None
#         self.timestamp          = datetime.datetime.strptime(columns[0], "%Y%m%d_%H%M%S_%f")
        self.timestamp          = columns[0]
        self.volume             = None
        self.volumeFilled       = int(columns[6])
        self.tradeID            = columns[8]
        
class OrderFeed:
    def __init__(self, data):
        columns = data.split(",")
        self.buySell            = columns[8]
        self.deleted            = columns[10] == "1"
        self.errorDescription   = columns[12]
        self.market             = columns[2]
        self.openClose          = columns[7]
        self.orderID            = columns[4]
        self.orderType          = columns[14]
        self.orderValidity      = None
        self.price              = float(columns[5])
        self.productCode        = columns[3]
        self.source             = int(columns[13])
        self.status             = columns[11]
#         self.timestamp          = datetime.datetime.strptime(columns[0], "%Y%m%d_%H%M%S_%f")
        self.timestamp          = columns[0]
        self.volume             = int(columns[6])
        self.volumeFilled       = int(columns[9])

class PortfolioFeed:
    def __init__(self, data):
        columns = data.split(",")
        self.avgOpenPrice       = float(columns[5])
        self.market             = columns[2]
        self.netInvestedAmt     = float(columns[6])
        self.netPos             = int(columns[4])
        self.productCode        = columns[3]
        self.realizedPL         = float(columns[7])
#         self.timestamp          = datetime.datetime.strptime(columns[0], "%Y%m%d_%H%M%S_%f")
        self.timestamp          = columns[0]
        self.totalPL            = float(columns[9])
        self.unrealizedPL       = float(columns[8])
        
class Pnlperffeed:
    def __init__(self, data):
        columns = data.split(",")
        self.timestamp          = columns[0]
        self.dailyPnL       = float(columns[2])
        self.monthlyPnL     = float(columns[3])
        self.yearlyPnL         = float(columns[4])

class OHLCFeed:
    def __init__(self,data):
       columns = data.split(",")
       self.timestamp          = columns[0]
       self.market            = columns[2]
       self.productCode       = columns[3]
       self.open     = float(columns[4])
       self.high         = float(columns[5])
       self.low         = float(columns[6])
       self.close         = float(columns[7])
       self.volume         = float(columns[8])

