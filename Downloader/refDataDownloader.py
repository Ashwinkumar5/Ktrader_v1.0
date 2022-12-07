# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 20:55:24 2022

@author: KTrader
"""

'''
This module is used to download reference Data from Yahoo finance
'''
#import sys
import pandas as pd
import yfinance as yf
import time 
import requests
import io
import pymongo;

class Equity_Indices(object):
    
    def __init__(self,mongo_handle):
        
        self.yahoo_main_indices={
                'NIFTY':'^NSEI',
                'BANKNIFTY':'^NSEBANK',
                'DOWJONES':'^DJI'                
            };
        
        self.yahoo_nse_sector_indices={
                        'IT':'^CNXIT',
                        'SMALLCAP':'^CNXSC',
                        'FINANCE':'^CNXFIN',
                        'AUTO':'^CNXAUTO',
                        'FMCG':'^CNXFMCG',
                        'PHARMA':'^CNXPHARMA',
                        'METAL':'^CNXMETAL',
                        'PSUBANK':'^CNXPSUBANK',
                        'MEDIA':'^CNXMEDIA',
                        'SERVICE':'^CNXSERVICE',
                        'INFRA':'^CNXINFRA',
                        'PSE':'^CNXPSE'
            };
        
        self.trading_view_nse_sector_indices={
                        'IT':'CNXIT',
                        'SMALLCAP':'CNXSMALLCAP',
                        'FINANCE':'CNXFINANCE',
                        'AUTO':'CNXAUTO',
                        'FMCG':'CNXFMCG',
                        'PHARMA':'CNXPHARMA',
                        'ENERY':'CNXENERGY',
                        'METAL':'CNXMETAL',
                        'PSUBANK':'CNXPSUBANK',
                        'MEDIA':'CNXMEDIA',
                        'SERVICE':'CNXSERVICE',
                        'INFRA':'CNXINFRA',
                        'PSE':'CNXPSE',
                        'CONSUMPTION':'CNXCONSUMPTION'
            };
        
        self.industry_sector_indices_map = {
            'Services':'CNXSERVICE',
            'Automobile and Auto Components' : 'CNXAUTO',
            'Power':'CNXENERGY',
            'Metals & Mining':'CNXMETAL',
            'Healthcare':'CNXPHARMA',
            'Chemicals':'CNXSMALLCAP',
            'Media Entertainment & Publication':'CNXMEDIA',
            'Consumer Services':'CNXCONSUMPTION',
        	'Capital Goods':'CNXAUTO',
        	'Fast Moving Consumer Goods': 'CNXFMCG',
        	'Construction':'CNXINFRA',
        	'Utilities':'CNXSERVICE',
        	'Realty':'CNXINFRA',
        	'Forest Materials':'CNXSERVICE',
        	'Telecommunication':'CNXSERVICE',
        	'Construction Materials':'CNXINFRA',
        	'Consumer Durables':'CNXFMCG',
        	'Diversified':'CNXSMALLCAP',
        	'Financial Services':'^NSEBANK',
        	'Textiles':'CNXSMALLCAP',
        	'Information Technology':'CNXIT',
        	'Oil Gas & Consumable Fuels':'CNXENERGY'
        };
        
        #reference url
        #https://www1.nseindia.com/products/content/equities/indices/sectoral_indices.htm
        
        self.nse_symbol_dict={
            'NIFTY_50':'https://www1.nseindia.com/content/indices/ind_nifty50list.csv',
            'NIFTY_NEXT50':'https://www1.nseindia.com/content/indices/ind_niftynext50list.csv',
            'NIFTY_100':'https://www1.nseindia.com/content/indices/ind_nifty100list.csv',
            'NIFTY_200':'https://www1.nseindia.com/content/indices/ind_nifty200list.csv',
            'NIFTY_500':'https://www1.nseindia.com/content/indices/ind_nifty500list.csv',
            'NIFTY_500_MULTICAP':'https://www1.nseindia.com/content/indices/ind_nifty500Multicap502525_list.csv',
            'NIFTY_MIDCAP_150':'https://www1.nseindia.com/content/indices/ind_niftymidcap150list.csv',
            'NIFTY_MIDCAP_50':'https://www1.nseindia.com/content/indices/ind_niftymidcap50list.csv',
            'NIFTY_MIDCAP_SELECT':'https://www1.nseindia.com/content/indices/ind_niftymidcapselect_list.csv',
            'NIFTY_MIDCAP_100':'https://www1.nseindia.com/content/indices/ind_niftymidcap100list.csv',
            'NIFTY_SMALLCAP_250':'https://www1.nseindia.com/content/indices/ind_niftysmallcap250list.csv',
            'NIFTY_SMALLCAP_50':'https://www1.nseindia.com/content/indices/ind_niftysmallcap50list.csv',
            'NIFTY_SMALLCAP_100':'https://www1.nseindia.com/content/indices/ind_niftysmallcap100list.csv',
            'NIFTY_LARGE_MIDCAP_250':'https://www1.nseindia.com/content/indices/ind_niftylargemidcap250list.csv',
            'NIFTY_MID_SMALL_CAP_400':'https://www1.nseindia.com/content/indices/ind_niftymidsmallcap400list.csv',
            'NIFTY_MICRO_CAP_250':'https://www1.nseindia.com/content/indices/ind_niftymicrocap250_list.csv'
        };


        
    
        # Database
        self.NSE_DB = 'NSE';
        #Collection
        self.NSE_DB_STOCK_LIST = 'STOCKS';
        
        self.NSE_SCRIPT_EXTN = '.NS';
        
        self.NSE_BROAD_INDICES_HIST_DATA = 'NSE_BROAD_INDICES_HIST_DATA';
        self.NSE_SECTOR_INDICES_HIST_DATA = 'NSE_SECTOR_INDICES_HIST_DATA';
        
        self.NSE_STOCK_HIST_DATA = 'NSE_STOCK_HIST_DATA';
        
        #database         
        self.Daily_BUY_SIDE_ANALYSIS_DB = 'BUY_SIDE';
                
        
        self.yearTradingDays = 251;
        
        #mongo db instance
        self.mongo_instance = mongo_handle;
        #reference Data from Mongo
        self.nse_symbol_df=pd.DataFrame();
        self.nse_index_list =pd.DataFrame();
        
        
        #def nse stock_historical data reference 
        self.nse_stock_historical_data_dict={};
       
        #mkt broad index historical data 
        self.broad_index_historical_data_dict={};
        
        #mkt sector index historical data        
        self.sector_Index_historical_data_dict={};
        
    #--------------------------------------------------------------------------

    def get_main_indices(self,index):            
            return self.main_indices[index];
        
    def get_sector_indices(self,industry):          
            return self.industry_sector_indices_map[industry];
        
    def get_period(self,period_year):
        return (self.yearTradingDays * period_year);
   
    def get_nse_listed_stock_list(self):
        return list(self.nse_symbol_df['Symbol']);
    
    def get_nse_index_list(self):
        
        return list(self.nse_index_list);
    
    
    def get_nse_listed_stock_df(self):
        return self.nse_symbol_df;
    
    def get_nse_listed_stock_historycal_data(self):
        return self.nse_stock_historical_data_dict;
    
    def get_broad_index_historical_data(self):        
        return self.broad_index_historical_data_dict;
    
    def get_sector_Index_historical_data(self):
        return self.sector_Index_historical_data_dict;
    
    #--------------------------------------------------------------------------

    def get_NSE_BROAD_INDICES_HIST_DATA_DB(self):
        return self.NSE_BROAD_INDICES_HIST_DATA;

    def get_NSE_SECTOR_INDICES_HIST_DATA_DB(self):
        return self.NSE_SECTOR_INDICES_HIST_DATA;
    
    def get_NSE_Symbol_DB(self):
        return self.NSE_DB;
    
    def get_NSE_STOCK_HIST_DATA_DB(self):
        return self.NSE_STOCK_HIST_DATA;
    
    def get_NSE_Symbol_Collection(self):
        return self.NSE_DB_STOCK_LIST;

    #--------------------------------------------------------------------------

    def load_symbol_list(self):
        # load stock list 
        self.nse_symbol_df = self.mongo_instance.getCollectionrecords(self.get_NSE_Symbol_DB(),self.get_NSE_Symbol_Collection());
        
        #load nse index list
    def load_nse_index_list(self):
        # here we need list of collections from mongo db 
        self.nse_index_list = self.mongo_instance.getCollectionListforDB(self.get_NSE_SECTOR_INDICES_HIST_DATA_DB());
       
        
        
        #load data
    def load_data(self):
        
        if( len(self.nse_symbol_df) <= 0):
            self.load_symbol_list();
            self.load_nse_index_list();
            
            
        # loading nse stocka historycal data        
        for symbol in self.get_nse_listed_stock_list():           
            self.nse_stock_historical_data_dict[symbol] = self.mongo_instance.getCollectionrecords(self.get_NSE_STOCK_HIST_DATA_DB(),symbol);
        
        #load broad index historical data
        for index in self.yahoo_main_indices.values():
            self.broad_index_historical_data_dict[index] = self.mongo_instance.getCollectionrecords(self.get_NSE_BROAD_INDICES_HIST_DATA_DB(),index);
            
        #load sector index historical data
        
        for index in self.trading_view_nse_sector_indices.values():
            self.sector_Index_historical_data_dict[index] = self.mongo_instance.getCollectionrecords(self.get_NSE_SECTOR_INDICES_HIST_DATA_DB(),index);
        
class Download_Nifty_Symbols(object):
    def __init__(self):
        pass;
    
    def downloadNiftyBroadIndices(self,url):
        try:
            resp = requests.get(url).content;                
            ActiveSymbolsdf = pd.read_csv(io.StringIO(resp.decode('utf-8')));
            return ActiveSymbolsdf;
        except Exception as exp:
            print('Exception caught while downloding Nifty Broad indices symbols ',exp);
            
          
    
    
class Download_Prices_YFinance(object):
    
    def __init__(self,symbollist):
            self.symlist = symbollist;
    
    def dumpHistoricalData(self,symlist,start,end,directoryPath):
        try:            
            for sym in symlist:                 
                data=yf.download(sym ,start,end,progress=False);                
                data.to_csv(directoryPath+"//"+sym +".csv");
                
        except Exception as err:
            print ("Exception in download historical data from yahoo ..",err.args);
    
    def downloadHistorialPeriodforsymbol(self ,symbol,period_,interval_):
        try:    
             data=yf.download(symbol,period=period_,progress=False ,interval = interval_,group_by = 'ticker', auto_adjust = True,prepost = False,
                                     threads = True,proxy = None);             
             return data;
        except Exception as err:
            print ("Exception in download historical data from yahoo ..",err.args);
    
    def downloadStockInfo(self,symbol):
        stock_info = yf.Ticker(symbol);
        return stock_info;
    
    def downloadoptionChain(self,symbol):
        pass;
    
    def writeCSV(self):
        pass;