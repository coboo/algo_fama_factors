#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cashAlgoAPI
import sys
import math
import time
import ConfigParser
import MySQLdb
import numpy as np
import talib
from datetime import datetime, timedelta
#import IndicatorLibrary
import subprocess


class Strategy:
    # Initialize Strategy
    def init(self):
        self.initiated = False
        self.current_timestamp = None
        file_name_str = "../log/" + datetime.strftime(datetime.now(), "%Y%m%d%H%M%S") + ".txt"
        self.log = open(file_name_str, 'w')
        self.da = DataAccess(self.log)
        self.dp = DataProcessing(self.log, self.da)
        self.ms = ManekiStrategy(self.log, self.dp)
        self.dp.load_historical_daily_stock_price(self.dp.historical_daily_price_start_date)
        self.dp.load_historical_hourly_stock_price(self.dp.load_historic_hourly_price_start_date)
        self.dp.initial_previous_hourly_stock_price(datetime.strptime(self.dp.start_date, "%Y-%m-%d"))
        self.historical_daily_ohlc_np = np.array(self.dp.historical_daily_ohlc_list)
        self.historical_hourly_ohlc_np = np.array(self.dp.historical_hourly_ohlc_list)
        self.daily_adjust_ratio_dict = {}
        self.initiated = True

        signal_list = self.dp.load_signals()
        for signal_feed in signal_list:
            # signal_id,status,timestamp,instrument_id,buy_sell,price,volume
            # ["timestamp", "signalfeed", "HKEX", "Instrument_id", "Order_ID", "price", "quantity", "open", "buy_sell", "insert", "limit_order", "fill_or_kill", ""]
            # trade_feed = self.ms.trade_management(signal_list)
            '''
            return self.timestamp + ",signalfeed," + self.market + "," + self.productCode + "," \
            + self.orderID + "," + str(self.price) + "," + str(self.volume) + "," + self.openClose + "," \
            + str(self.buySell) + ","+self.action +"," + self.orderType + "," + self.orderValidity
            '''
            timestamp, market, product_code, signal_id, price, volume, open_close, buy_sell, action, order_type, order_validity = datetime.strftime(signal_feed[2], "%Y%m%d_%H%M%S_%f"), \
                "SEHK", signal_feed[3], signal_feed[0], float(signal_feed[5]), float(signal_feed[6]), "open", int(signal_feed[4]), "insert", "market_order", "today"
            # md.timestamp, "SEHK", md.productCode, str(self.cnt), md.askPrice1, 1, "open", 1, "insert", "market_order", "today"
            order = cashAlgoAPI.Order(timestamp, market, product_code, signal_id, price, volume, open_close, buy_sell, action, order_type, order_validity)
            self.mgr.insertOrder(order)

            print_save_log(self.log, "======================")
            # print order
            # print_save_log(self.log, order)
            # time.sleep(60)
            # trade_feed = onTradeFeed(signal_list)
            # self.ms.order_management(trade_feed, signal_list, self.historical_daily_ohlc_np)

    def myinit(self):
        pass

    # Process Market Data. Please use onOHLCFeed() in OHLC mode
    def onMarketDataUpdate(self, market, code, md):
        '''
        product, timestamp, price, volume, adjust_ratio = md.productCode, datetime.strptime(md.timestamp, "%Y%m%d_%H%M%S_%f"), md.lastPrice, md.lastVolume, 1
        self.current_timestamp = timestamp
        '''
        pass

    # Used in OHLC mode.
    def onOHLCFeed(self, ohlc_price):
        product, timestamp, open, high, low, close, volume, adjust_ratio = ohlc_price.productCode, datetime.strptime(ohlc_price.timestamp, "%Y%m%d_%H%M%S_%f"), ohlc_price.open, ohlc_price.high, ohlc_price.low, ohlc_price.close, float(ohlc_price.volume), 1
        self.current_timestamp = timestamp

        if len(self.daily_adjust_ratio_dict) == 0:
            self.daily_adjust_ratio_dict = self.dp.get_daily_adjust_ratio(timestamp)

        if product == "00016":
            print_save_log(self.log, "product:" + product + ", timestamp:" + str(timestamp) + ", open:" + str(open) + ", high:" + str(high) + ", low:" + str(low) + ", close:" + str(close) + ", volume:" + str(volume))

        if product in self.daily_adjust_ratio_dict.keys():
            adjust_ratio = self.daily_adjust_ratio_dict[product]
            open, high, low, close = open * adjust_ratio, high * adjust_ratio, low * adjust_ratio, close * adjust_ratio
        market_data_ohlc = [timestamp, product, open, high, low, close, volume]
        is_new_hourly, is_new_daily = self.dp.convert_to_ohlc(market_data_ohlc)

        # 5 min bar check sell signal
        if product in self.dp.daily_ohlc_dict.keys() and product in self.ms.portfolio_dict.keys():
            if is_new_hourly or len(self.historical_hourly_ohlc_np) == 0:
                self.historical_hourly_ohlc_np = np.array(self.dp.historical_hourly_ohlc_list)
            is_sell_bool, sell_signal_feed = self.ms.trigger_sell_signal(self.historical_hourly_ohlc_np, product, timestamp, close)
            for sell_signal in sell_signal_feed:
                trade_feed = self.ms.trade_management(sell_signal)
                self.ms.order_management(trade_feed, sell_signal, self.historical_daily_ohlc_np)

    # Process Order
    def onOrderFeed(self, order_feed):
        print_save_log(self.log, "*********************Start Order Feed**********************")
        '''
        columns = data.split(",")
        self.buySell            = int(columns[8])
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

        if order_feed is not None:
            order_timestamp, product, buy_sell, order_price, avail_volume, order_id, stop_loss_price = order_feed[0], order_feed[3], order_feed[8], order_feed[0], order_feed[0], order_feed[0], order_feed[0]
        '''

        '''
        if len(trade_feed) > 0:
            # timestamp, product, buy_sell, trade_price, trade_volume, order_id
            trade_timestamp, product, buy_sell, order_price, avail_volume, order_id, stop_loss_price = trade_feed[0], \
                                                                                                       trade_feed[1], \
                                                                                                       trade_feed[2], \
                                                                                                       trade_feed[3], \
                                                                                                       trade_feed[4], \
                                                                                                       trade_feed[5], 0

            trade_price, trade_volume = signal_feed[5], signal_feed[6]

            if daily_price_ohlc_np is not None:
                stop_loss_price, stop_loss_atr = self.calculate_stop_loss_price(daily_price_ohlc_np, product, order_price)

            if buy_sell == self.CONST_BUY:
                str_order_query = "insert into " + self.tb_orders + " (instrument_id, net_position, stop_loss_price, buy_timestamp, buy_price, buy_volume, buy_target_price,buy_target_volume) values  \
                                                                ('%s', %s, %s, '%s', %s, %s, %s, %s)" % (
                    product, avail_volume, stop_loss_price, str(trade_timestamp), order_price, avail_volume, order_price, avail_volume)
                self.dp.save_order_to_db(str_order_query)
                self.available_cash += (trade_price * trade_volume)
                self.holding_cash -= (trade_price * trade_volume)
            else:
                order_id = trade_feed[5]
                net_position = avail_volume - avail_volume
                str_order_query = "update %s set net_position=%s, sell_timestamp='%s', sell_price=%s, sell_volume=%s, sell_target_price=%s, sell_target_volume=%s where order_id=%s" \
                                  % (self.tb_orders, net_position, str(trade_timestamp), order_price, avail_volume,
                                     order_price, avail_volume, order_id)
                self.dp.save_order_to_db(str_order_query)
            self.dp.asset_management(self.cash, self.available_cash, self.holding_cash)
            self.orders_np = self.dp.load_orders()
        '''

        return

    # Process Trade
    def onTradeFeed(self, trade_feed):
        print_save_log(self.log, "*********************Start Trade Feed**********************")
        '''
        columns = trade_feed.split(",")

        self.deleted = None
        self.errorDescription = None
        self.volume = None
        self.status = None
        self.timestamp = columns[0]
        self.market = columns[2]
        self.productCode = columns[3]
        self.orderID = columns[4]
        self.price = float(columns[5])
        self.volumeFilled = int(columns[6])
        self.buySell = int(columns[7])
        self.tradeID = columns[8]
        self.source = int(columns[9])

        self.buySell            = int(columns[7])
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

        print_save_log(self.log, columns)
        print_save_log(self.log, "*********************End Trade Feed**********************")
        '''
        order_id, status, timestamp, product, buy_sell, trade_price, trade_volume = \
            trade_feed.orderID, trade_feed.status, trade_feed.timestamp, trade_feed.productCode, trade_feed.buySell, trade_feed.price, trade_feed.volume

        self.dp.update_signal(order_id, status, product, trade_price, trade_volume)

        if buy_sell == self.CONST_SELL:
            self.ms.available_cash = self.ms.available_cash + (trade_price * trade_volume)
            self.ms.cash = self.ms.cash + (trade_price * trade_volume)
            self.dp.asset_management(self.ms.cash, self.ms.available_cash, self.ms.holding_cash)
            pre_realized_pnl = 0
            if product in self.ms.daily_realized_pnl_dict.keys():
                pre_realized_pnl = self.ms.daily_realized_pnl_dict[product]

            individual_order = self.ms.orders_np[np.where(self.ms.orders_np[:, 0] == order_id)].tolist()
            if len(individual_order) > 0:
                order_price = float(individual_order[0][3])
                self.ms.daily_realized_pnl_dict[product] = pre_realized_pnl + ((trade_price - order_price) * trade_volume)
        else:
            self.ms.cash = self.ms.cash - (trade_price * trade_volume)
            self.ms.available_cash = self.ms.available_cash - (trade_price * trade_volume)

        if trade_volume > 0:
            str_trade_query = "insert into " + self.tb_trades + " (timestamp,instrument_id,buy_sell,trade_price,trade_volume) values ('%s','%s',%s,%s,%s)" % (
                str(timestamp), product, buy_sell, trade_price, trade_volume)
            self.da.execute_command(str_trade_query)
            trade_feed = [timestamp, product, buy_sell, trade_price, trade_volume, order_id]
            self.ms.portfolio_management(trade_feed)
        msg = str(datetime.now()) + " Trading management, product: " + product + ", price: " + str(
            trade_price) + ", volume: " + str(trade_volume) + ", buy_sell: " + str(buy_sell)
        print_save_log(self.log, msg)

    # Process Position
    def onPortfolioFeed(self, portfolioFeed):
        return
    
    # Process PnL
    def onPnlperffeed(self, pf):
        return

    def onDayEnd(self):
        print "Entry onDayEnd"
        if len(self.dp.daily_ohlc_dict) > 0:
            for product_code in self.dp.daily_ohlc_dict:
                self.dp.historical_daily_ohlc_list.append([self.dp.daily_ohlc_dict[product_code][0].date(), product_code,
                    self.dp.daily_ohlc_dict[product_code][2],
                    self.dp.daily_ohlc_dict[product_code][3],
                    self.dp.daily_ohlc_dict[product_code][4],
                    self.dp.daily_ohlc_dict[product_code][5],
                    self.dp.daily_ohlc_dict[product_code][6]])
                print "insert into daily ohlc volume: " + str(self.dp.daily_ohlc_dict[product_code][6]) + ", product: " + product_code

            self.historical_daily_ohlc_np = np.array(self.dp.historical_daily_ohlc_list)
            self.dp.daily_ohlc_list_for_db = self.historical_daily_ohlc_np[np.where(self.historical_daily_ohlc_np[:, 0] == self.current_timestamp.date())].tolist()
            self.dp.save_daily_ohlc_to_db()

            # the following is save hourly ohlc
            end_trading_hour = datetime.strptime(datetime.strftime(self.current_timestamp, "%Y-%m-%d " + self.dp.trading_hour_end), "%Y-%m-%d %H:%M:%S")
            for product in self.dp.hourly_ohlc_dict.keys():
                if product not in self.dp.daily_ohlc_dict.keys():
                    continue
                hourly_timestamp, hourly_open_price, hourly_high_price, hourly_low_price, hourly_close_price, hourly_total_volume = \
                    self.dp.hourly_ohlc_dict[product][0], self.dp.hourly_ohlc_dict[product][2], \
                    self.dp.hourly_ohlc_dict[product][3], self.dp.hourly_ohlc_dict[product][4], \
                    self.dp.hourly_ohlc_dict[product][5], self.dp.hourly_ohlc_dict[product][6]

                end_count_hours = (end_trading_hour - hourly_timestamp).total_seconds() / 3600
                end_range_index = int(end_count_hours) + 1

                for i in range(0, end_range_index):
                    if (self.dp.trading_hour_start_int <= hourly_timestamp.hour <= self.dp.trading_hour_lunch_start_int) or (self.dp.trading_hour_lunch_end_int < hourly_timestamp.hour <= self.dp.trading_hour_end_int):
                        self.dp.hourly_ohlc_list_for_db.append(
                            [hourly_timestamp, product, self.dp.hourly_ohlc_dict[product][2],
                             self.dp.hourly_ohlc_dict[product][3], self.dp.hourly_ohlc_dict[product][4],
                             self.dp.hourly_ohlc_dict[product][5], self.dp.hourly_ohlc_dict[product][6]])
                        self.dp.historical_hourly_ohlc_list.append(
                            [hourly_timestamp, product, self.dp.hourly_ohlc_dict[product][2],
                             self.dp.hourly_ohlc_dict[product][3], self.dp.hourly_ohlc_dict[product][4],
                             self.dp.hourly_ohlc_dict[product][5], self.dp.hourly_ohlc_dict[product][6]])
                    hourly_timestamp = self.dp.hourly_ohlc_dict[product][0] + timedelta(hours=1)
                    self.dp.hourly_ohlc_dict[product] = [hourly_timestamp, product, self.dp.hourly_ohlc_dict[product][5],
                                                         self.dp.hourly_ohlc_dict[product][5],
                                                         self.dp.hourly_ohlc_dict[product][5],
                                                         self.dp.hourly_ohlc_dict[product][5], 0]

            if len(self.dp.hourly_ohlc_list_for_db) > 0:
                self.dp.save_hourly_ohlc_to_db()
                self.dp.pre_hourly_ohlc_dict = self.dp.hourly_ohlc_dict.copy()
                self.dp.hourly_ohlc_list_for_db = []
                self.dp.hourly_ohlc_dict.clear()

        # because restart the program, all variable will reset value
        # self.dp.daily_ohlc_list_for_db = []
        # self.dp.daily_ohlc_dict.clear()
        # self.dp.initial_previous_hourly_stock_price(self.current_timestamp)

        for product_daily in self.dp.pre_daily_ohlc_dict:
            if product_daily in self.ms.stock_dict.keys():
                print "daily close: " + product_daily
                price = self.dp.pre_daily_ohlc_dict[product_daily][5]

                today_total_volume = self.dp.pre_daily_ohlc_dict[product_daily][6]
                is_buy_bool, buy_signal_feed = self.ms.trigger_buy_signal(self.historical_daily_ohlc_np, product_daily, self.current_timestamp, price, today_total_volume)

                if is_buy_bool:
                    signal_risk_feed = self.ms.money_management(buy_signal_feed, self.historical_daily_ohlc_np)

        self.ms.signal_risk_market_value = 0
        self.ms.pnl_management(self.current_timestamp, self.dp.pre_daily_ohlc_dict)
        self.ms.daily_realized_pnl_dict.clear()

        print str(self.current_timestamp) + " today is end"

        subprocess.call("/home/kevinli/algo_trade/sample1/bin/force_stop_sample1_strategy.sh", shell=True)


class DataProcessing:
    historical_daily_ohlc_list, historical_hourly_ohlc_list, market_calendar = [], [], []

    def __init__(self, log, da):
        self.log = log
        self.da = da
        self.available_stock_dict, self.daily_ohlc_dict, self.hourly_ohlc_dict, self.daily_ohlc_list_for_db, self.hourly_ohlc_list_for_db, self.timestamp = {}, {}, {}, [], [], None
        self.pre_daily_ohlc_dict, self.pre_hourly_ohlc_dict, self.daily_adjust_ratio_dict = {}, {}, {}
        self.config = ConfigParser.ConfigParser()
        self.config.read("../conf/sample1_strategy_param.ini")
        self.tb_market_data_daily_hk_stock = self.config.get("Database", "tb_market_data_daily_hk_stock")
        self.tb_market_data_hourly_hk_stock = self.config.get("Database", "tb_market_data_hourly_hk_stock")
        self.tb_portfolios = self.config.get("Database", "tb_portfolios")
        self.tb_daily_pnl = self.config.get("Database", "tb_daily_pnl")
        self.tb_orders = self.config.get("Database", "tb_orders")
        self.tb_signals = self.config.get("Database", "tb_signals")
        self.tb_hk_stock_list = self.config.get("Database", "tb_hk_stock_list")
        self.tb_daily_price_adjust_ratio = self.config.get("Database", "tb_daily_price_adjust_ratio")
        self.tb_trading_account = self.config.get("Database", "tb_trading_account")
        self.trading_hour_end = self.config.get("Trading_Environment", "trading_hour_end")
        self.trading_hour_start = self.config.get("Trading_Environment", "trading_hour_start")
        self.trading_hour_lunch_start = self.config.get("Trading_Environment", "trading_hour_lunch_start")
        self.trading_hour_lunch_end = self.config.get("Trading_Environment", "trading_hour_lunch_end")
        self.start_date = self.config.get("Trading_Environment", "start_date")
        self.pre_open_allocation_session = self.config.get("Trading_Environment", "pre_open_allocation_session")
        self.entry_condition_close_ma_change_period = 100
        self.trading_hour_start_int = int(self.trading_hour_start.split(":")[0])
        self.trading_hour_lunch_start_int = int(self.trading_hour_lunch_start.split(":")[0])
        self.trading_hour_lunch_end_int = int(self.trading_hour_lunch_end.split(":")[0])
        self.trading_hour_end_int = int(self.trading_hour_end.split(":")[0])
        self.available_stock_dict = self.get_available_stock_list().copy()
        self.orders_np = self.load_orders()
        self.market_calendar = self.get_market_calendar()
        self.historical_daily_price_start_date = self.market_calendar[self.market_calendar.index(self.start_date) - (self.entry_condition_close_ma_change_period + 50)]
        self.load_historic_hourly_price_start_date = self.market_calendar[self.market_calendar.index(self.start_date) - (int(math.ceil(self.entry_condition_close_ma_change_period / 6)) + 50)]
        self.is_adj_open_hourly_ohlc, self.is_adj_open_daily_ohlc = True, True

    def get_asset_info(self):
        str_asset_query = "select cash,avail_cash,holding_cash from " + self.tb_trading_account
        asset_info = self.da.query_command(str_asset_query)
        msg = str(datetime.now()) + " Get Asset Info: cash:" + str(asset_info[0][0]) + ", available cash:" + str(str(asset_info[0][1])) + ", holding cash:" + str(asset_info[0][2])
        print_save_log(self.log, msg)
        return float(asset_info[0][0]), float(asset_info[0][1]), float(asset_info[0][2])

    def asset_management(self, cash, available_cash, holding_cash):
        msg = str(datetime.now()) + " Asset Management: Cash:" + str(cash) + " available_cash:" + str(
            available_cash) + " holding_cash:" + str(holding_cash)
        print_save_log(self.log, msg)
        str_update_asset_query = "update trading_account set cash = " + str(cash) + ", avail_cash=" + str(
            available_cash) + ", holding_cash=" + str(holding_cash) + ", timestamp = '" + datetime.strftime(
            datetime.now(), "%Y-%m-%d %H:%M:%S") + "'"
        self.da.execute_command(str_update_asset_query)

    def get_market_calendar(self):
        calendar_list = []
        str_calendar_query = "select timestamp from market_calendar where trading_day=1 order by timestamp asc;"
        calendar_tuple = self.da.query_command(str_calendar_query)
        for calendar in calendar_tuple:
            calendar_list.append(datetime.strftime(calendar[0], "%Y-%m-%d"))
        return calendar_list

    def get_available_stock_list(self):
        stock_dict = {}
        stock_list_query = "select instrument_id,board_lot from " + self.tb_hk_stock_list + " where available=1"
        data_stock_list = self.da.query_command(stock_list_query)
        for stock, lot_size in data_stock_list:
            stock_dict[stock] = lot_size
        msg = str(datetime.now()) + " Loading available stock, data length:" + str(len(data_stock_list))
        print_save_log(self.log, msg)
        return stock_dict

    def load_portfolios(self):
        portfolio_dict = {}
        str_portfolio_query = "select instrument_id, volume from " + self.tb_portfolios + ";"
        portfolio_list = self.da.query_command(str_portfolio_query)
        for portfolio in portfolio_list:
            portfolio_dict[portfolio] = [portfolio[1], 0, 0]
            msg = str(datetime.now()) + " Loading portfolio, product: " + portfolio[0] + ", volume: " + str(
                portfolio[1])
            print_save_log(self.log, msg)
        return portfolio_dict

    def load_signals(self):
        signal_list = []
        str_signal_query = "select signal_id,status,timestamp,instrument_id,buy_sell,price,volume from signals where status = 0"
        signals = self.da.query_command(str_signal_query)
        for signal in signals:
            signal_list.append([str(signal[0]), signal[1], signal[2], signal[3], signal[4], signal[5], signal[6]])
            msg = str(datetime.now()) + " Loading signals, id: " + str(signal[0]) + ", status: " + str(
                signal[1]) + ", timestamp: " + str(signal[2]) + ", product: " + str(signal[3]) + ", buy_sell: " + str(
                signal[4]) + ", price: " + str(signal[5]) + ", volume: " + str(signal[6])
            print_save_log(self.log, msg)
        str_signal_update = "update signals set status=1 where status=0"
        self.da.execute_command(str_signal_update)
        msg = str(datetime.now()) + " Loading signals status, data length: " + str(len(signal_list))
        print_save_log(self.log, msg)
        print_save_log(self.log, "=====================================================")
        return signal_list

    def load_orders(self):
        orders_list = []
        str_order_query = "select order_id, instrument_id, net_position, buy_price, stop_loss_price from " + self.tb_orders + " where net_position > 0"
        orders = self.da.query_command(str_order_query)
        for order in orders:
            orders_list.append([order[0], order[1], order[2], order[3], order[4]])
            msg = str(datetime.now()) + " Loading orders, order_id: " + str(order[0]) + ", product: " + str(
                order[1]) + ", net_position: " + str(order[2]) + ", buy_price: " + str(
                order[3]) + ", stop_loss_price: " + str(order[4])
            print_save_log(self.log, msg)
        return np.array(orders_list)

    def load_historical_hourly_stock_price(self, start_timestamp):
        str_query = "select timestamp,instrument_id,open,high,low,close,volume from " + self.tb_market_data_hourly_hk_stock + " where from_days(to_days(timestamp)) >= '" + str(
            start_timestamp) + "' order by timestamp asc;"
        historical_hourly_price = self.da.query_command(str_query)
        for stock_price in historical_hourly_price:
            self.historical_hourly_ohlc_list.append(
                [stock_price[0], stock_price[1], stock_price[2], stock_price[3], stock_price[4], stock_price[5],
                 stock_price[6]])
        msg = str(datetime.now()) + " Loading historical hourly stock price, data length: " + str(
            len(self.historical_hourly_ohlc_list))
        print_save_log(self.log, msg)

    def initial_previous_hourly_stock_price(self, initial_timestamp):
        start_hourly_ohlc_trading_hour = datetime.strptime(str(self.market_calendar[self.market_calendar.index(
            datetime.strftime(initial_timestamp, "%Y-%m-%d")) + 1]) + " " + self.trading_hour_start, "%Y-%m-%d %H:%M:%S")
        start_trading_hour = datetime.strptime(
            datetime.strftime(initial_timestamp, "%Y-%m-%d " + self.trading_hour_start), "%Y-%m-%d %H:%M:%S")
        str_query = "select a.instrument_id,a.close from " + self.tb_market_data_daily_hk_stock + " as a,\
                    (select max(timestamp) as timestamp,instrument_id from " + self.tb_market_data_daily_hk_stock + " group by instrument_id) as b \
                    where a.timestamp = b.timestamp and a.instrument_id = b.instrument_id and from_days(to_days(a.timestamp)) < '" + str(
            start_trading_hour) + "';"
        stock_latest_close_price = self.da.query_command(str_query)
        for stock_price in stock_latest_close_price:
            self.hourly_ohlc_dict[stock_price[0]] = [start_hourly_ohlc_trading_hour, stock_price[0], stock_price[1],
                                                     stock_price[1], stock_price[1], stock_price[1], 0]
        self.pre_hourly_ohlc_dict = self.hourly_ohlc_dict.copy()
        msg = str(datetime.now()) + " Loading previous_hourly_stock_price, data length: " + str(
            len(self.pre_hourly_ohlc_dict))
        print_save_log(self.log, msg)

    def load_historical_daily_stock_price(self, start_timestamp):
        str_query = "select timestamp,instrument_id,open,high,low,close,volume from " + self.tb_market_data_daily_hk_stock + " where from_days(to_days(timestamp)) >= '" + str(
            start_timestamp) + "' order by timestamp asc;"
        historical_daily_price = self.da.query_command(str_query)
        for stock_price in historical_daily_price:
            self.historical_daily_ohlc_list.append(
                [stock_price[0].date(), stock_price[1], float(stock_price[2]), float(stock_price[3]),
                 float(stock_price[4]), float(stock_price[5]), float(stock_price[6])])
            if stock_price[1] not in self.pre_daily_ohlc_dict.keys():
                self.pre_daily_ohlc_dict[stock_price[1]] = [stock_price[0].date(), stock_price[1],
                                                            float(stock_price[2]), float(stock_price[3]),
                                                            float(stock_price[4]), float(stock_price[5]),
                                                            float(stock_price[6])]
        msg = str(datetime.now()) + " Loading historical daily stock price, data length: " + str(
            len(self.historical_daily_ohlc_list))
        print_save_log(self.log, msg)

    def get_daily_adjust_ratio(self, timestamp):
        daily_adjust_ratio_dict = {}
        str_adjust_ratio_query = "select instrument_id,timestamp,ratio from " + self.tb_daily_price_adjust_ratio + " where from_days(to_days(timestamp))='" + datetime.strftime(
            timestamp, "%Y-%m-%d") + "'"
        adjust_ratio = self.da.query_command(str_adjust_ratio_query)

        for ratio in adjust_ratio:
            product, timestamp, ratio_price = ratio[0], datetime.strftime(ratio[1], "%Y-%m-%d"), ratio[2]
            daily_adjust_ratio_dict[product] = ratio_price
        msg = str(datetime.now()) + " Loading adjust ratio, data length: " + str(len(adjust_ratio))
        print_save_log(self.log, msg)
        return daily_adjust_ratio_dict

    def convert_to_ohlc(self, market_data_ohlc):
        timestamp, product, is_new_hourly, is_new_daily = market_data_ohlc[0], market_data_ohlc[1], False, False
        if timestamp >= datetime.strptime(str(timestamp.date()) + " " + self.pre_open_allocation_session, "%Y-%m-%d %H:%M:%S") and market_data_ohlc[6] > 0:
            is_new_hourly = self.convert_to_hourly_ohlc(market_data_ohlc)
            self.convert_to_daily_ohlc(market_data_ohlc)
        return is_new_hourly, is_new_daily

    def convert_to_hourly_ohlc(self, market_data_ohlc):
        timestamp, product, open, high, low, close, volume, is_new_hourly = market_data_ohlc[0], \
            market_data_ohlc[1], market_data_ohlc[2], market_data_ohlc[3], \
            market_data_ohlc[4], market_data_ohlc[5], market_data_ohlc[6], False

        if product in self.hourly_ohlc_dict.keys() and product in self.pre_hourly_ohlc_dict.keys():
            if timestamp <= datetime.strptime(str(timestamp.date()) + " " + str(self.hourly_ohlc_dict[product][0].time()), "%Y-%m-%d %H:%M:%S") and self.is_adj_open_hourly_ohlc:
                # timestamp,instrument_id,open,high,low,close,volume
                self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product, open, high, low, close, volume]
                self.is_adj_open_hourly_ohlc = False
            else:
                # print str(timestamp.date()) + ", " + str(self.hourly_ohlc_dict[product][0].date())
                if timestamp.date() != self.hourly_ohlc_dict[product][0].date():
                    self.hourly_ohlc_dict[product][0] = datetime.strptime(
                        str(timestamp.date()) + " " + str(self.hourly_ohlc_dict[product][0].time()), "%Y-%m-%d %H:%M:%S")

                count_hours = (timestamp - self.hourly_ohlc_dict[product][0]).total_seconds() / 3600
                if count_hours <= 0:
                    hourly_high = high if high > self.hourly_ohlc_dict[product][3] else self.hourly_ohlc_dict[product][3]
                    hourly_low = low if low < self.hourly_ohlc_dict[product][4] else self.hourly_ohlc_dict[product][4]
                    hourly_total_volume = float(volume) + float(self.hourly_ohlc_dict[product][6])
                    self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product,
                                                      self.hourly_ohlc_dict[product][2], hourly_high, hourly_low, close,
                                                      hourly_total_volume]
                elif count_hours >= 1:
                    is_new_hourly = True
                    range_index = int(count_hours)
                    for i in range(0, range_index):
                        self.pre_hourly_ohlc_dict[product] = self.hourly_ohlc_dict[product]
                        if (self.trading_hour_start_int <= self.hourly_ohlc_dict[product][0].hour <= self.trading_hour_lunch_start_int) or (
                                self.trading_hour_lunch_end_int < self.hourly_ohlc_dict[product][0].hour <= self.trading_hour_end_int):
                            self.hourly_ohlc_list_for_db.append(
                                [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                                 self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                                 self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                            self.historical_hourly_ohlc_list.append(
                                [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                                 self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                                 self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                        if self.hourly_ohlc_dict[product][0].hour == self.trading_hour_lunch_start_int:
                            self.hourly_ohlc_dict[product][0] = self.hourly_ohlc_dict[product][0] + timedelta(hours=2)
                        else:
                            self.hourly_ohlc_dict[product][0] = self.hourly_ohlc_dict[product][0] + timedelta(hours=1)
                        if count_hours % 1 == 0 and i == range_index - 1:
                            self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product, open, high, low, close, market_data_ohlc[6]]
                        else:
                            self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product,
                                                              self.hourly_ohlc_dict[product][5],
                                                              self.hourly_ohlc_dict[product][5],
                                                              self.hourly_ohlc_dict[product][5],
                                                              self.hourly_ohlc_dict[product][5], 0]

                    if count_hours % 1 > 0:
                        if self.hourly_ohlc_dict[product][0].hour == self.trading_hour_lunch_start_int:
                            self.hourly_ohlc_list_for_db.append(
                                [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                                 self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                                 self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                            self.historical_hourly_ohlc_list.append(
                                [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                                 self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                                 self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                            self.hourly_ohlc_dict[product][0] = self.hourly_ohlc_dict[product][0] + timedelta(hours=2)

                        self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product, open, high, low,
                                                          close, market_data_ohlc[6]]
                        self.pre_hourly_ohlc_dict[product] = self.hourly_ohlc_dict[product]
                else:
                    is_new_hourly = True
                    if (self.trading_hour_start_int <= self.hourly_ohlc_dict[product][0].hour <= self.trading_hour_lunch_start_int) or \
                            (self.trading_hour_lunch_end_int < self.hourly_ohlc_dict[product][0].hour <= self.trading_hour_end_int):
                        self.hourly_ohlc_list_for_db.append(
                            [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                             self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                             self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                        self.historical_hourly_ohlc_list.append(
                            [self.hourly_ohlc_dict[product][0], product, self.hourly_ohlc_dict[product][2],
                             self.hourly_ohlc_dict[product][3], self.hourly_ohlc_dict[product][4],
                             self.hourly_ohlc_dict[product][5], self.hourly_ohlc_dict[product][6]])
                    if self.hourly_ohlc_dict[product][0].hour == self.trading_hour_lunch_start_int:
                        self.hourly_ohlc_dict[product][0] = self.hourly_ohlc_dict[product][0] + timedelta(hours=2)
                    else:
                        self.hourly_ohlc_dict[product][0] = self.hourly_ohlc_dict[product][0] + timedelta(hours=1)
                    self.hourly_ohlc_dict[product] = [self.hourly_ohlc_dict[product][0], product, open, high, low, close,
                                                      market_data_ohlc[6]]
        return is_new_hourly

    def convert_to_daily_ohlc(self, market_data_ohlc):
        timestamp, product, open, high, low, close, volume, is_new_day, is_new_hourly, product_diff_days = market_data_ohlc[0], market_data_ohlc[1], \
            market_data_ohlc[2], market_data_ohlc[3], market_data_ohlc[4], market_data_ohlc[5], market_data_ohlc[6], False, False, 0

        # [0-timestamp,1-instrument_id,2-open,3-high,4-low,5-close,6-volume]
        if product in self.daily_ohlc_dict.keys():
            daily_high = high if high > self.daily_ohlc_dict[product][3] else self.daily_ohlc_dict[product][3]
            daily_low = low if low < self.daily_ohlc_dict[product][4] else self.daily_ohlc_dict[product][4]
            daily_total_volume = volume + self.daily_ohlc_dict[product][6]
            if self.is_adj_open_daily_ohlc:
                self.daily_ohlc_dict[product] = [timestamp, product, open, daily_high, daily_low, close, daily_total_volume]
                self.is_adj_open_daily_ohlc = False
            else:
                self.daily_ohlc_dict[product] = [timestamp, product, self.daily_ohlc_dict[product][2], daily_high, daily_low, close, daily_total_volume]
        else:
            self.daily_ohlc_dict[product] = [market_data_ohlc[0], market_data_ohlc[1], open, high, low, close, market_data_ohlc[6]]

    def save_daily_ohlc_to_db(self):
        str_insert = "insert into " + self.tb_market_data_daily_hk_stock + " (timestamp,instrument_id,open,high,low,close,volume) values \
                (%s,%s,%s,%s,%s,%s,'%s')"
        self.da.insert_many_command(self.daily_ohlc_list_for_db, str_insert)
        msg = str(datetime.now()) + " Total " + str(
            len(self.daily_ohlc_list_for_db)) + " insert daily ohlc into db " + datetime.strftime(
            self.daily_ohlc_list_for_db[0][0], "%Y-%m-%d")
        print_save_log(self.log, msg)

    def save_hourly_ohlc_to_db(self):
        str_insert = "insert into " + self.tb_market_data_hourly_hk_stock + " (timestamp,instrument_id,open,high,low,close,volume) values \
                    (%s,%s,%s,%s,%s,%s,%s)"
        self.da.insert_many_command(self.hourly_ohlc_list_for_db, str_insert)
        msg = str(datetime.now()) + " Total " + str(
            len(self.hourly_ohlc_list_for_db)) + " insert hourly ohlc into db " + datetime.strftime(
            self.hourly_ohlc_list_for_db[0][0], "%Y-%m-%d")
        print_save_log(self.log, msg)
        self.hourly_ohlc_list_for_db = []

    def save_signal_to_db(self, timestamp, status, product, buy_sell, signal_price, expect_trade_volume, signal_comment):
        str_signal_query = "insert into " + self.tb_signals + " (timestamp,status,instrument_id,buy_sell,price,volume,comment) values ('%s',%s,'%s',%s,%s,%s,'%s')" % (
            str(timestamp), status, product, buy_sell, signal_price, expect_trade_volume, signal_comment)
        self.da.execute_command(str_signal_query)

    def save_order_to_db(self, str_order_query):
        self.da.execute_command(str_order_query)

    def update_signal(self, signal_id, status, product, trade_price, trade_volume):
        str_signal_update = "update " + self.tb_signals + " set status = 2 where signal_id= %s" % signal_id
        self.da.execute_command(str_signal_update)
        msg = str(datetime.now()) + " Updating signals status: " + str(status) + ", signal_id: " + str(
            signal_id) + ", instrument_id:" + str(product) + ", signal price: " + str(
            trade_price) + ", signal volume: " + str(trade_volume)
        print_save_log(self.log, msg)


class DataAccess:
    print "****************Start initialize data DataAccess class****************"

    def __init__(self, log):
        self.log = log
        config = ConfigParser.ConfigParser()
        config.read("../conf/sample1_strategy_param.ini")
        self.md_db_host = config.get("Database", "db_host")
        self.md_db_name = config.get("Database", "db_name")
        self.md_db_user = config.get("Database", "db_user")
        self.md_db_pwd = config.get("Database", "db_pwd")

    def query_command(self, sql_query):
        db = MySQLdb.connect(self.md_db_host, self.md_db_user, self.md_db_pwd, self.md_db_name)
        cursor = db.cursor()
        cursor.execute(sql_query)
        data_object = cursor.fetchall()
        cursor.close()
        db.close()
        return data_object

    def execute_command(self, sql_command):
        db = MySQLdb.connect(self.md_db_host, self.md_db_user, self.md_db_pwd, self.md_db_name)
        cursor = db.cursor()
        try:
            cursor.execute(sql_command)
            db.commit()
        except Exception as e:
            print_save_log(self.log, e)
            db.rollback()
        cursor.close()
        db.close()

    def execute_command_with_return(self, sql_command):
        db = MySQLdb.connect(self.md_db_host, self.md_db_user, self.md_db_pwd, self.md_db_name)
        cursor = db.cursor()
        try:
            cursor.execute(sql_command)
            db.commit()
            cursor.execute("SELECT LAST_INSERT_ID()")
            record_id = cursor.fetchall()[0][0]
        except Exception as e:
            print_save_log(self.log, e)
            db.rollback()
        cursor.close()
        db.close()
        return record_id

    def insert_many_command(self, insert_data_list, sql_query_str):
        db = MySQLdb.connect(self.md_db_host, self.md_db_user, self.md_db_pwd, self.md_db_name)
        cursor = db.cursor()
        try:
            cursor.executemany(sql_query_str, insert_data_list)
            db.commit()
        except Exception as e:
            print_save_log(self.log, e)
            db.rollback()
        cursor.close()
        db.close()

    def truncate_table(self, table_name):
        db = MySQLdb.connect(self.md_db_host, self.md_db_user, self.md_db_pwd, self.md_db_name)
        cursor = db.cursor()
        sql_truncate_table = "truncate table " + table_name
        try:
            cursor.execute(sql_truncate_table)
            db.commit()
        except Exception as e:
            print_save_log(self.log, e)
            db.rollback()
        cursor.close()
        db.close()


class ManekiStrategy:
    historical_daily_price_start_date, load_historic_hourly_price_start_date = None, None

    def __init__(self, log, dp):
        self.log, self.dp = log, dp
        config = ConfigParser.ConfigParser()
        config.read("../conf/sample1_strategy_param.ini")
        self.expect_loss = float(config.get("Money_Management", "expect_loss"))
        self.product_position_limit = config.get("Money_Management", "product_position_limit")
        self.entry_condition_close_chv_period = int(config.get("Buy_Logic", "entry_condition_close_chv_period"))
        self.entry_condition_close_ma_change_period = int(config.get("Buy_Logic", "entry_condition_close_ma_change_period"))
        self.exit_condition_close_clv_period = int(config.get("Buy_Logic", "exit_condition_close_clv_period"))
        self.entry_condition_volume_period = int(config.get("Buy_Logic", "entry_condition_volume_period"))
        self.entry_condition_volume_std_cv = int(config.get("Buy_Logic", "entry_condition_volume_std_cv"))
        self.entry_condition_atr_small_period = int(config.get("Buy_Logic", "entry_condition_atr_small_period"))
        self.entry_condition_atr_large_period = int(config.get("Buy_Logic", "entry_condition_atr_large_period"))
        self.entry_condition_atr_cv = float(config.get("Buy_Logic", "entry_condition_atr_cv"))
        self.entry_condition_minimum_turnover = int(config.get("Buy_Logic", "entry_condition_minimum_turnover"))
        self.exit_condition_close_price_period = int(config.get("Buy_Logic", "exit_condition_close_price_period"))
        self.CONST_BUY, self.CONST_SELL = 1, 2
        self.stop_loss_price = 0
        self.signal_risk_market_value = 0
        self.daily_realized_pnl_dict = {}
        self.stock_dict = self.dp.available_stock_dict.copy()
        self.cash, self.available_cash, self.holding_cash = self.dp.get_asset_info()
        self.orders_np = self.dp.load_orders()
        self.portfolio_dict = self.dp.load_portfolios()
        self.pre_portfolio_dict = self.portfolio_dict.copy()
        self.market_calendar = self.dp.market_calendar

    def trigger_buy_signal(self, daily_price_ohlc_np, product, timestamp, close_price, volume):
        signal_feed, buy_sell, is_trigger_bool = [], 1, False
        if len(daily_price_ohlc_np) == 0:
            print "condition: 1"
            return is_trigger_bool, signal_feed

        # stock filtering and > period 100 days
        daily_individual_stock_price_ohlc_np = daily_price_ohlc_np[np.where(daily_price_ohlc_np[:, 1] == product)]
        daily_individual_stock_price_ohlc_count = len(daily_individual_stock_price_ohlc_np)
        if close_price < 1 or len(daily_individual_stock_price_ohlc_np) <= self.entry_condition_close_ma_change_period:
            print "condition: 2"
            return is_trigger_bool, signal_feed

        # 100 days mean turnover > 1000000
        entry_condition_avg_volume_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - self.entry_condition_close_ma_change_period: daily_individual_stock_price_ohlc_count, 6]
        entry_condition_close_price_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - self.entry_condition_close_ma_change_period: daily_individual_stock_price_ohlc_count, 5]
        entry_condition_avg_volume_np = np.array(entry_condition_avg_volume_np.tolist(), dtype=float)
        entry_condition_avg_turnover = np.mean(entry_condition_avg_volume_np * entry_condition_close_price_np)
        if entry_condition_avg_turnover <= self.entry_condition_minimum_turnover:
            print "condition: 3"
            return is_trigger_bool, signal_feed

        # close price  > pre day 49 days maximum close price
        entry_condition_close_chv_range_mean_np = daily_individual_stock_price_ohlc_np[(daily_individual_stock_price_ohlc_count - 1) - self.entry_condition_close_chv_period:(
            daily_individual_stock_price_ohlc_count - 1), 5]
        entry_condition_close_chv_range_max_price = np.amax(entry_condition_close_chv_range_mean_np)
        if close_price <= entry_condition_close_chv_range_max_price:
            print "condition: 4, close_price: " + str(close_price) + ", past day 49 days maximum close price: " + str(entry_condition_close_chv_range_max_price)
            return is_trigger_bool, signal_feed

        # today 100 days MA close price > previous 100 days MA close price
        today_entry_condition_close_ma_change_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - self.entry_condition_close_ma_change_period: daily_individual_stock_price_ohlc_count, 5]
        today_entry_condition_close_ma_change_np = np.array(today_entry_condition_close_ma_change_np.tolist(),
                                                            dtype=float)

        pre_entry_condition_close_ma_change_np = daily_individual_stock_price_ohlc_np[(daily_individual_stock_price_ohlc_count - 1) - self.entry_condition_close_ma_change_period:(
            daily_individual_stock_price_ohlc_count - 1), 5]
        pre_entry_condition_close_ma_change_np = np.array(pre_entry_condition_close_ma_change_np.tolist(), dtype=float)

        today_entry_condition_close_ma_change_init = \
            talib.SMA(today_entry_condition_close_ma_change_np, self.entry_condition_close_ma_change_period)[
                self.entry_condition_close_ma_change_period - 1]
        pre_entry_condition_close_ma_change_init = \
            talib.SMA(pre_entry_condition_close_ma_change_np, self.entry_condition_close_ma_change_period)[
                self.entry_condition_close_ma_change_period - 1]
        if today_entry_condition_close_ma_change_init <= pre_entry_condition_close_ma_change_init:
            print "condition: 5, today 100 MA close price: " + str(today_entry_condition_close_ma_change_init) + ", pre 100 MA close price: " + str(pre_entry_condition_close_ma_change_init)
            return is_trigger_bool, signal_feed

        # condition: volume>25 days avg volume + 2 *sd(25 days volume)
        entry_condition_volume_mean_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - self.entry_condition_volume_period:daily_individual_stock_price_ohlc_count, 6]
        entry_condition_volume_mean_np = np.array(entry_condition_volume_mean_np.tolist(), dtype=float)
        entry_condition_volume_mean = np.mean(entry_condition_volume_mean_np)
        entry_condition_volume_std = np.std(entry_condition_volume_mean_np)
        if volume <= (entry_condition_volume_mean + self.entry_condition_volume_std_cv * entry_condition_volume_std):
            print "condition: 6"
            return is_trigger_bool, signal_feed

        ''' remove atr filtering
        # the entry_condition_atr_small_period -1 because the atr calculated from second index, the first point as start point don't join calculation.
        entry_condition_small_atr_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - entry_condition_atr_small_period - 1:daily_individual_stock_price_ohlc_count, 3:6]
        entry_condition_larger_atr_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - entry_condition_atr_large_period - 1:daily_individual_stock_price_ohlc_count, 3:6]

        entry_condition_small_atr_np = np.array(entry_condition_small_atr_np.tolist(), dtype=float)
        entry_condition_larger_atr_np = np.array(entry_condition_larger_atr_np.tolist(), dtype=float)

        # print df_small_atr
        entry_condition_small_atr = talib.ATR(entry_condition_small_atr_np[:, 0], entry_condition_small_atr_np[:, 1], entry_condition_small_atr_np[:, 2], entry_condition_atr_small_period)[entry_condition_atr_small_period]

        # print df_large_atr
        entry_condition_large_atr = talib.ATR(entry_condition_larger_atr_np[:, 0], entry_condition_larger_atr_np[:, 1], entry_condition_larger_atr_np[:, 2], entry_condition_atr_large_period)[entry_condition_atr_large_period]

        # print small_atr,",",large_atr
        if entry_condition_small_atr >= entry_condition_large_atr * entry_condition_atr_cv:
            return False, signal_feed
        '''
        is_trigger_bool = True
        signal_feed = [timestamp, product, buy_sell, close_price]
        return is_trigger_bool, signal_feed

    def trigger_sell_signal(self, hourly_price_ohlc_np, product, timestamp, current_price):
        buy_sell, is_trigger_bool, status = 2, False, 0
        sell_signal_feed = []
        if len(self.orders_np) == 0:
            return is_trigger_bool, sell_signal_feed

        individual_product_order_list_np = self.orders_np[np.where(self.orders_np[:, 1] == product)]
        if len(individual_product_order_list_np) <= 0:
            return is_trigger_bool, sell_signal_feed

        individual_product_order_list = individual_product_order_list_np.tolist()

        hourly_individual_stock_price_ohlc_np = hourly_price_ohlc_np[np.where(hourly_price_ohlc_np[:, 1] == product)]
        hourly_individual_stock_price_ohlc_count = len(hourly_individual_stock_price_ohlc_np)
        exit_condition_cv_np = hourly_individual_stock_price_ohlc_np[
                               (hourly_individual_stock_price_ohlc_count - 1) - self.exit_condition_close_price_period:(
                                   hourly_individual_stock_price_ohlc_count - 1), 3:6]
        exit_condition_cv_price_min = np.amin(exit_condition_cv_np)

        # [order_id, product, float(order_price), float(avail_volume), float(order_volume)]
        for product_order in individual_product_order_list:
            buy_order_id = product_order[0]
            product = product_order[1]
            order_volume = float(product_order[2])
            order_price = float(product_order[3])
            self.stop_loss_price = float(product_order[4])
            if order_volume <= 0:
                continue

            max_exit_price = max(self.stop_loss_price, exit_condition_cv_price_min)

            if current_price < max_exit_price:
                signal_comment = "buy_order_id: " + str(buy_order_id) + ", stop_loss_price: " + str(
                    self.stop_loss_price) + " , exit_condition_cv_price_min: " + str(
                    exit_condition_cv_price_min) + ", before_buy_cash: " + str(self.available_cash)
                str_signal_query = "insert into " + self.dp.tb_signals + " (status,timestamp,instrument_id,buy_sell,price,volume,comment) values (%s,'%s','%s',%s,%s,%s,'%s')" % (
                    status, str(timestamp), product, buy_sell, current_price, order_volume, signal_comment)
                sell_signal_id = self.da.execute_command_with_return(str_signal_query)
                sell_signal_feed.append(
                    [sell_signal_id, status, timestamp, product, buy_sell, current_price, order_volume, buy_order_id])
                is_trigger_bool = True
                msg = str(datetime.now()) + " Trigger Sell Signal Successful: " + str(
                    timestamp) + ", product:" + product + ", buy order id:" + str(
                    buy_order_id) + ", current_price:" + str(current_price) + ", order_volume:" + str(
                    order_volume) + ", order_price:" + str(order_price) + ", exit_condition_cv_price_min:" + str(
                    exit_condition_cv_price_min) + ", stop_loss_price:" + str(self.stop_loss_price)
                print_save_log(self.log, msg)
            '''
            else:
                msg = str(datetime.now()) + " Trigger Sell Signal Failed: " + str(
                    timestamp) + ", product:" + product + ", order_price:" + str(
                    order_price) + ", current_price:" + str(current_price) + ", exit_condition_cv_price_min:" + str(
                    exit_condition_cv_price_min) + ", stop_loss_price:" + str(self.stop_loss_price)
                print_save_log(self.log, msg)
            '''
        return is_trigger_bool, sell_signal_feed

    def money_management(self, signal_feed, daily_price_ohlc_np):
        timestamp, product, buy_sell, signal_price, lot_size, is_portfolio, expect_trade_volume, expect_loss_money, position_limit_money, cash_allow_trade_volume, loss_stop_trade_volume, position_limit_volume, status = \
            signal_feed[0], signal_feed[1], signal_feed[2], signal_feed[3], 0, True, 0, 0, 0, 0, 0, 0, 0

        stop_loss_price, stop_loss_atr = self.calculate_stop_loss_price(daily_price_ohlc_np, product, signal_price)
        lot_size = float(self.stock_dict[product])

        safe_position_market_value = 0
        order_list = self.orders_np.tolist()
        # order_id, instrument_id, net_position, buy_price, stop_loss_price.
        for order in order_list:
            position = order[2]
            if order[1] == product:
                is_portfolio = False
                msg = str(datetime.now()) + " Already has position:" + product + ", volume:" + str(position)
                print_save_log(self.log, msg)
            individual_stop_loss_price = order[4]
            safe_position_market_value += float(position) * individual_stop_loss_price

        msg = str(datetime.now()) + " Position Market Value: " + str(safe_position_market_value) + ", Signal Market Value: " + str(self.signal_risk_market_value) + ", Total Market Value:" + str(safe_position_market_value + self.signal_risk_market_value)
        print_save_log(self.log, msg)
        if is_portfolio:
            expect_loss_money = (self.available_cash + safe_position_market_value + self.signal_risk_market_value) * self.expect_loss
            loss_stop_trade_volume = int(expect_loss_money / (stop_loss_atr * lot_size)) * lot_size

            position_limit_money = (self.available_cash + safe_position_market_value + self.signal_risk_market_value) * float(self.product_position_limit)
            position_limit_volume = int(position_limit_money / (signal_price * lot_size)) * lot_size

            cash_allow_trade_volume = int(self.available_cash / (signal_price * lot_size)) * lot_size
            expect_trade_volume = min(loss_stop_trade_volume, position_limit_volume, cash_allow_trade_volume)
            self.signal_risk_market_value += expect_trade_volume * (signal_price - stop_loss_atr)

        signal_comment = "stop_loss_price:" + str(signal_price - stop_loss_atr) + ", before_buy_cash: " + str(
            self.available_cash) + ", turnover: " + str(
            signal_price * expect_trade_volume) + " ,expect_loss_money:" + str(
            expect_loss_money) + " ,position_limit_money:" + str(
            position_limit_money) + " ,cash_allow_trade_volume:" + str(cash_allow_trade_volume)

        self.dp.save_signal_to_db(timestamp, status, product, buy_sell, signal_price, expect_trade_volume, signal_comment)

        msg = str(datetime.now()) + " Trigger Buy Signal: " + str(timestamp) + ", cash:" + str(
            self.available_cash) + ", product:" + product + ", signal_price:" + str(
            signal_price) + ", expect_trade_volume:" + str(expect_trade_volume) + ", turnover:" + str(
            expect_trade_volume * signal_price) + " ,exit_condition_atr: " + str(stop_loss_atr) + ", loss_stop_trade_volume:" + str(loss_stop_trade_volume) + ", position_limit_volume:" + str(position_limit_volume) + ", cash_allow_trade_volume:" + str(cash_allow_trade_volume)
        print_save_log(self.log, msg)

        if buy_sell == self.CONST_BUY:
            self.holding_cash += (signal_price * expect_trade_volume)
            self.available_cash = self.cash - self.holding_cash

        self.dp.asset_management(self.cash, self.available_cash, self.holding_cash)

        return [timestamp, product, buy_sell, signal_price, expect_trade_volume]

    def order_management(self, trade_feed, signal_feed, daily_price_ohlc_np):
        # [timestamp, product, buy_sell, trade_price, trade_volume, order_id]

        if len(trade_feed) > 0:
            # timestamp, product, buy_sell, trade_price, trade_volume, order_id
            trade_timestamp, product, buy_sell, order_price, avail_volume, order_id, stop_loss_price = trade_feed[0], \
                                                                                                       trade_feed[1], \
                                                                                                       trade_feed[2], \
                                                                                                       trade_feed[3], \
                                                                                                       trade_feed[4], \
                                                                                                       trade_feed[5], 0

            trade_price, trade_volume = signal_feed[5], signal_feed[6]

            if daily_price_ohlc_np is not None:
                stop_loss_price, stop_loss_atr = self.calculate_stop_loss_price(daily_price_ohlc_np, product, order_price)

            if buy_sell == self.CONST_BUY:
                str_order_query = "insert into " + self.tb_orders + " (instrument_id, net_position, stop_loss_price, buy_timestamp, buy_price, buy_volume, buy_target_price,buy_target_volume) values  \
                                                                ('%s', %s, %s, '%s', %s, %s, %s, %s)" % (
                    product, avail_volume, stop_loss_price, str(trade_timestamp), order_price, avail_volume, order_price, avail_volume)
                self.dp.save_order_to_db(str_order_query)
                self.available_cash += (trade_price * trade_volume)
                self.holding_cash -= (trade_price * trade_volume)
            else:
                order_id = trade_feed[5]
                net_position = avail_volume - avail_volume
                str_order_query = "update %s set net_position=%s, sell_timestamp='%s', sell_price=%s, sell_volume=%s, sell_target_price=%s, sell_target_volume=%s where order_id=%s" \
                                  % (self.tb_orders, net_position, str(trade_timestamp), order_price, avail_volume,
                                     order_price, avail_volume, order_id)
                self.dp.save_order_to_db(str_order_query)
            self.dp.asset_management(self.cash, self.available_cash, self.holding_cash)
            self.orders_np = self.dp.load_orders()

    def trade_management(self, signal_risk_feed):
        # signal_id,status,timestamp,instrument_id,buy_sell,price,volume
        trade_feed = []
        order_id = ""
        signal_id, status, timestamp, product, buy_sell, trade_price, trade_volume = \
            signal_risk_feed[0], \
            signal_risk_feed[1], \
            signal_risk_feed[2], \
            signal_risk_feed[3], \
            signal_risk_feed[4], \
            signal_risk_feed[5], \
            signal_risk_feed[6]

        self.dp.update_signal(signal_id, status, product, trade_price, trade_volume)

        if buy_sell == self.CONST_SELL:
            self.available_cash = self.available_cash + (trade_price * trade_volume)
            self.cash = self.cash + (trade_price * trade_volume)
            self.dp.asset_management(self.cash, self.available_cash, self.holding_cash)
            order_id = signal_risk_feed[7]
            pre_realized_pnl = 0
            if product in self.daily_realized_pnl_dict.keys():
                pre_realized_pnl = self.daily_realized_pnl_dict[product]

            individual_order = self.orders_np[np.where(self.orders_np[:, 0] == order_id)].tolist()
            if len(individual_order) > 0:
                order_price = float(individual_order[0][3])
                self.daily_realized_pnl_dict[product] = pre_realized_pnl + ((trade_price - order_price) * trade_volume)
        else:
            self.cash = self.cash - (trade_price * trade_volume)
            self.available_cash = self.available_cash - (trade_price * trade_volume)

        if trade_volume > 0:
            str_trade_query = "insert into " + self.tb_trades + " (timestamp,instrument_id,buy_sell,trade_price,trade_volume) values ('%s','%s',%s,%s,%s)" % (
                str(timestamp), product, buy_sell, trade_price, trade_volume)
            self.da.execute_command(str_trade_query)
            trade_feed = [timestamp, product, buy_sell, trade_price, trade_volume, order_id]
            self.portfolio_management(trade_feed)
        msg = str(datetime.now()) + " Trading management, product: " + product + ", price: " + str(
            trade_price) + ", volume: " + str(trade_volume) + ", buy_sell: " + str(buy_sell)
        print_save_log(self.log, msg)
        return trade_feed

    def portfolio_management(self, trade_feed):
        timestamp, product, buy_sell, trade_price, trade_volume, unrealized_pnl, realized_pnl, pre_position, pre_avg_price, pre_realized_pnl = \
            trade_feed[0], trade_feed[1], trade_feed[2], trade_feed[3], trade_feed[4], 0, 0, 0, 0, 0
        daily_portfolio_list = []

        if product in self.portfolio_dict.keys():
            pre_position = self.portfolio_dict[product][0]
            pre_avg_price = self.portfolio_dict[product][1]
            unrealized_pnl = self.portfolio_dict[product][2]
            realized_pnl = self.portfolio_dict[product][3]

        if buy_sell == self.CONST_BUY:
            position = pre_position + trade_volume
            avg_price = ((pre_avg_price * pre_position) + (trade_price * trade_volume)) / position
            self.portfolio_dict[product] = [position, avg_price, unrealized_pnl, realized_pnl]
        if buy_sell == self.CONST_SELL:
            if product in self.portfolio_dict.keys():
                remain_volume = pre_position - trade_volume
                pre_unrealized_pnl = self.portfolio_dict[product][2]
                pre_realized_pnl = self.portfolio_dict[product][3]
                realized_pnl = (trade_price - pre_avg_price) * trade_volume + pre_realized_pnl
                if remain_volume >= 0:
                    self.portfolio_dict[product] = [remain_volume, trade_price, pre_unrealized_pnl, realized_pnl]
                else:
                    msg = str(
                        datetime.now()) + " Alert: The sell volume out of range!!! Product, Trade_Volume, Remain_volume: " + product + ", " + str(
                        trade_volume) + ", " + str(remain_volume)
                    print_save_log(self.log, msg)

        for portfolio_product in self.portfolio_dict:
            if int(self.portfolio_dict[portfolio_product][0]) > 0:
                daily_portfolio_list.append([portfolio_product, self.portfolio_dict[portfolio_product][0],
                                             self.portfolio_dict[portfolio_product][1],
                                             datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S")])

        if len(daily_portfolio_list) > 0:
            self.da.truncate_table(self.tb_portfolios)
            str_insert = "insert into " + self.tb_portfolios + " (instrument_id, volume, avg_price, timestamp) values \
                        (%s, %s, %s, %s);"
            self.da.insert_many_command(daily_portfolio_list, str_insert)

    def pnl_management(self, timestamp, daily_close_price_dict):
        del_portfolio_list, daily_pnl_list_for_db, position, unrealized, realized, pre_position, pre_unrealized, pre_realized, close_price = [], [], 0, 0, 0, 0, 0, 0, 0
        for product in self.portfolio_dict:
            unrealized, daily_realized, position, pre_position, pre_unrealized, is_close_price = 0, 0, 0, 0, 0, False
            # position = float(self.portfolio_dict[product][0])
            if len(self.orders_np) > 0:
                individual_product_order = self.orders_np[np.where(self.orders_np[:, 1] == product)].tolist()
                # order_id, instrument_id, net_position, buy_price
                for order in individual_product_order:
                    order_product = order[1]
                    order_position = float(order[2])
                    order_price = float(order[3])

                    if product in daily_close_price_dict.keys():
                        close_price = daily_close_price_dict[product][5]
                        unrealized += (close_price - order_price) * order_position
                    else:
                        is_close_price = True
                        msg = str(datetime.now()) + " PnL management: " + product + " " + str(
                            timestamp) + " didn't has market price"
                        print_save_log(self.log, msg)

                    if product == order_product:
                        position += float(order_position)
            if product in self.daily_realized_pnl_dict.keys():
                daily_realized = self.daily_realized_pnl_dict[product]

            if product in self.pre_portfolio_dict.keys():
                pre_position = float(self.pre_portfolio_dict[product][0])
                pre_unrealized = float(self.pre_portfolio_dict[product][2])

            daily_position = position - pre_position
            if is_close_price:
                unrealized = pre_unrealized
            daily_unrealized = unrealized - pre_unrealized
            daily_pnl_list_for_db.append([datetime.strftime(timestamp, "%Y-%m-%d"), product, daily_realized, daily_unrealized, daily_position])
            msg = str(datetime.now()) + " PnL management: " + product + ", net_position: " + str(position) + ", datetime:" + datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S") + ", realized:" + str(daily_realized) + ", unrealized:" + str(daily_unrealized)
            print_save_log(self.log, msg)
            if position <= 0:
                del_portfolio_list.append(product)
            else:
                daily_realized = 0
                self.portfolio_dict[product] = [self.portfolio_dict[product][0], self.portfolio_dict[product][1], unrealized, daily_realized]

        if len(daily_pnl_list_for_db) > 0:
            str_insert = "insert into " + self.tb_daily_pnl + " (timestamp,instrument_id,realized_pnl,unrealized_pnl,position) values \
                    (%s,%s,%s,%s,%s)"
            self.da.insert_many_command(daily_pnl_list_for_db, str_insert)

        for product in del_portfolio_list:
            self.portfolio_dict.pop(product, None)

        self.pre_portfolio_dict = self.portfolio_dict.copy()

    def stop_loss_price_update(self, daily_price_ohlc_np):
        order_list = self.orders_np.tolist()
        for order in order_list:
            # order_id, instrument_id, net_position, buy_price, stop_loss_price
            order_id, product, net_position, order_price, pre_stop_loss_price = order[0], order[1], order[2], order[3], \
                                                                                order[4]
            cur_stop_loss_price, stop_loss_atr = self.calculate_stop_loss_price(daily_price_ohlc_np, product,
                                                                                order_price)
            if pre_stop_loss_price < cur_stop_loss_price:
                str_order_query = "update %s set stop_loss_price=%s where order_id=%s" \
                                  % (self.tb_orders, cur_stop_loss_price, order_id)
                self.da.execute_command(str_order_query)
        self.orders_np = self.dp.load_orders()

    def calculate_stop_loss_price(self, daily_price_ohlc_np, product, order_price):

        exit_condition_atr_period = 10
        exit_condition_atr_cv = 2

        daily_individual_stock_price_ohlc_np = daily_price_ohlc_np[np.where(daily_price_ohlc_np[:, 1] == product)]
        daily_individual_stock_price_ohlc_count = len(daily_individual_stock_price_ohlc_np)
        exit_condition_atr_np = daily_individual_stock_price_ohlc_np[daily_individual_stock_price_ohlc_count - exit_condition_atr_period - 1: daily_individual_stock_price_ohlc_count, 3:6]
        exit_condition_atr_np = np.array(exit_condition_atr_np.tolist(), dtype=float)
        exit_condition_atr = \
            talib.ATR(exit_condition_atr_np[:, 0], exit_condition_atr_np[:, 1], exit_condition_atr_np[:, 2],
                      exit_condition_atr_period)[exit_condition_atr_period]
        cur_stop_loss_price = order_price - (exit_condition_atr * exit_condition_atr_cv)
        msg = str(datetime.now()) + " Calculating stop loss price, product: " + product + ", stop_loss_price: " + str(
            cur_stop_loss_price) + " ,stop_loss_atr: " + str(exit_condition_atr)
        print_save_log(self.log, msg)
        return cur_stop_loss_price, exit_condition_atr


def print_save_log(log_file, msg):
    print msg
    log_file.write(msg + "\n")
    log_file.flush()
