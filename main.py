# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 07:33:54 2022

@author: Ashwin
"""
import os
import sys
import datetime
import pandas as pd;
import pymongo;
import numpy as np;
from nsepy import get_history;
from tvDatafeed import TvDatafeed, Interval


sys.path.append("Utility");
sys.path.append("Downloader");
sys.path.append("Config");
sys.path.append("Swing");
sys.path.append("DB");
sys.path.append("momenum");
sys.path.append("Volume");
sys.path.append("Graph");

import refDataDownloader;
import Bullish as swingBullish;
import Trend  as trend;
import mail;
import common;
import Mongo;
import trends_strategy;
import volumeIndicator;
import chartDrawing ;
import common;

#-------------------------------------------------------------------------------------

#---------------Instances-----------------

mongo_instance=None;
Equity_instance =None;
download_Nifty_instance = None;
yFinanceInstance = None;
volume_profile_instance = None;
relative_comparator = None
breakout_indicator = None
chart_draw_instance = None;

#-------------------------------------------------------------------------------------

#---------------Object instances -----------------

trading_view =None;
globalsymbol_df=pd.DataFrame();
globalsymbol_list =[];
symbol_hist_data_dict={};
broadIndex_historical_data_dict = {};
sectorIndex_historical_data_dict = {};
global_index_list = [];

#-----------------------------------------------------------------------------------------------
#getting reference data

def getMongoInstance():

    global mongo_instance;

    if(mongo_instance == None):
        mongo_instance = Mongo.Mongo_db();
    return mongo_instance;

def getReferenceData_Equity():

    global Equity_instance;

    if(Equity_instance == None):
        Equity_instance = refDataDownloader.Equity_Indices(getMongoInstance());
    return Equity_instance;

def getDownload_Nifty():

    global download_Nifty_instance;

    if(download_Nifty_instance == None):
        download_Nifty_instance = refDataDownloader.Download_Nifty_Symbols();
    return download_Nifty_instance;

def getrefercedataYFinanceInstance():

    global yFinanceInstance;

    if(yFinanceInstance == None ):
        yFinanceInstance = refDataDownloader.Download_Prices_YFinance(getsymbol_list());
        
    return yFinanceInstance;


def getTradingViewInstance():
    global trading_view;

    if(trading_view == None):
        trading_view = TvDatafeed();

    return trading_view;

def getVOlumeProfileInstance():
    global volume_profile_instance;
    
    if(volume_profile_instance == None):
        volume_profile_instance = volumeIndicator.VOLUME_PROFILE();
        
    return volume_profile_instance;


def getrelative_comparator():
    global relative_comparator ;
    if(relative_comparator == None):
        relative_comparator = trends_strategy.RELATIVE_COMPARATOR();
    return relative_comparator;

def getbreakout_indicator():
    global breakout_indicator;
    
    if( breakout_indicator == None):
        breakout_indicator = trends_strategy.TREND_SCREENER();
        
    return breakout_indicator;

def getchartDrawInstance():
    global chart_draw_instance;
    
    if(chart_draw_instance == None ):
        chart_draw_instance = chartDrawing.PLOT_CHART();
        
    return chart_draw_instance;
    

def getchart_directory_path():
    chart_dir = common.getChartDirPath() +"\\"+common.getCurrectDateString();
    return chart_dir;

#get stock historical data

def getsymbol_list():
    global globalsymbol_list;
    return globalsymbol_list;

def getsymbol_df():
    global globalsymbol_df;
    return globalsymbol_df;

def getIndex_list():
    global global_index_list;
    return global_index_list;


def getsymbol_historical_data(symbol):

    global  symbol_hist_data_dict;    

    if symbol  in symbol_hist_data_dict.keys():
       
        symbol_hst_df =symbol_hist_data_dict[symbol];
        
        
        if '_id' in symbol_hst_df.columns:
            symbol_hst_df.drop('_id', axis=1, inplace=True);
            df = symbol_hst_df;
            return df;
        else:
            return symbol_hst_df;

#get boad index historical data
def getbroadIndex_historical_data_dict(index):
    global broadIndex_historical_data_dict;     
    
    if index in broadIndex_historical_data_dict.keys():
        broadIndex_df = broadIndex_historical_data_dict[index];
       
        if '_id' in broadIndex_df.columns:
            broadIndex_df.drop('_id', axis=1, inplace=True);
            df = broadIndex_df
            return df;
        else:
            return broadIndex_df;
    
#get sector index historical data
def getsectorIndex_historical_data_dict(index):
    global sectorIndex_historical_data_dict;
    
    if index in sectorIndex_historical_data_dict.keys():
        sectorIndex_df = sectorIndex_historical_data_dict[index];
       
        if '_id' in sectorIndex_df.columns:
            sectorIndex_df.drop('_id', axis=1, inplace=True);
            df = sectorIndex_df;
            return df;
        else:
            return sectorIndex_df; 
        

    
#---------------------------------------------------------------------------------------------
#Setting up-data

#setting symbol list
def setsymbol_list(symbol_list_):
    global globalsymbol_list;
    globalsymbol_list = symbol_list_;
    

#setting symbol historiacal data;
def set_symbol_historical_data(symbol_hist_data_):
     global  symbol_hist_data_dict;
     symbol_hist_data_dict = symbol_hist_data_;
     
#setting broad index historical data
def set_broad_index_historical_data(broadIndex_historical_data_dict_):
    global broadIndex_historical_data_dict;
    broadIndex_historical_data_dict = broadIndex_historical_data_dict_;
  
#setting sector index historical data
def set_sector_Index_historical_data_dict(sectorIndex_historical_data_dict_):
    global sectorIndex_historical_data_dict;    
    sectorIndex_historical_data_dict = sectorIndex_historical_data_dict_;

#setting symbol list    
def set_symbol_list_df(symbol_df):
    global globalsymbol_df;
    
    if '_id' in symbol_df.columns:
        symbol_df.drop('_id', axis=1, inplace=True);
    
    globalsymbol_df = symbol_df;
    


#setting global index list
def set_global_index_list(index_list):
    global global_index_list;    
    global_index_list = index_list;    
    return global_index_list;

#-------------------------------------------------------------------------------------------------
def createBuySideDB():
    try:
        DB_NAME = getReferenceData_Equity().Daily_BUY_SIDE_ANALYSIS_DB;
        getMongoInstance().createDB(DB_NAME);
        getMongoInstance().isDBExist(DB_NAME);
         
    except Exception as exp:
        print(' Caught exception while creating DB ',DB_NAME);
        
        
        
def getBuysideCollectionName():
    
    Coll_name = 'buy_side_';
    now = datetime.datetime.now()
    date_string = now.strftime('%Y-%m-%d')   
    Coll_name  = Coll_name + date_string+ "_stats";
    return Coll_name;


def getbuy_side_stat_df():
    
    dtypes = np.dtype(
            [
                ("Symbol", str),
                ("Volume", int),
                ("Current_Price",int),
                ("Engullfin", str),
                ("Trustinline", str),
                ("TweezerBottom", str),
                ("RsiComparator",str),
                ("RsiComparatorTreand",str),
                ("ConsolNarrowRange",str),                
                ("BreakoutNarrowRange",str),
                ("BreakoutWideRange",str),                
                ("LifeTimeHighZone",str),
                ("VPConvergence",str),
                ("VPDivergence",str),
                ("SellingClimax",str),
                ("PickVolumeBreakout",str),
                ("ObvBreakout",str),
                ("ObvBreakDown",str),
                ("DepressObvBreakout",str)
                
            ]
        );
    
    df = pd.DataFrame(np.empty(0, dtype=dtypes));
    df.set_index('Symbol');
    
     
    df['Symbol']                 = '';
    df['Current_Price']          = 00;
    df['Volume']                 = 00;
    df['Engullfin']              = '';
    df['Trustinline']            = '';
    df['TweezerBottom']          = '';
    df['RsiComparator']          = '';    
    df['RsiComparatorTreand']    = '';    
    df['ConsolNarrowRange']      = '';
    df['BreakoutNarrowRange']    = '';    
    df['BreakoutWideRange']      = '';
    df['LifeTimeHighZone']       = '';
    df['VPConvergence']          = '';
    df['VPDivergence']           = '';
    df['SellingClimax']          = '';
    df['PickVolumeBreakout']     = '';
    df['ObvBreakout']            = '';
    df['ObvBreakDown']           = '';
    df['DepressObvBreakout']     = '';
    
    return df;
    
#--------------------------------------------------------------------------------------------------
#load reference Data 
#loading nse_symbol
#loading symbol historical data from last 10 years
#load broad index historical Data
#load sector index historical data

def load_reference_data():
    
    #delete existing char directory and create new one 
    
    common.checkDirectory(getchart_directory_path());
    ref_obj= getReferenceData_Equity();
    ref_obj.load_data();
        
    # loading nse_symbol
    setsymbol_list(ref_obj.get_nse_listed_stock_list());    
    
    set_symbol_list_df(ref_obj.get_nse_listed_stock_df());
    
    #loading nse sector index 
    set_global_index_list(ref_obj.get_nse_index_list());
    
    #load nse symbol historical data
    set_symbol_historical_data(ref_obj.get_nse_listed_stock_historycal_data());
  
    #load nse broad index histoical data     
    set_broad_index_historical_data(ref_obj.get_broad_index_historical_data());
    
    #load sectoral index historical data 
    set_sector_Index_historical_data_dict(ref_obj.get_sector_Index_historical_data());

#----------------------------------------------------------------------------------------------------------------
def sendEmail():
    print('start sending email ...');    
    common.zip(common.getChartDirPath(),common.getCurrectDateString());    
    email = mail.Email();    
    email.sendEmail();
    print('ends  sending email ...');    

#----------------------------------------------------------------------------------------------------------------------------------
def SwingStrategy(symbol,buy_stat_df,index):
    try:
        # Create a symbol list
        #print (' start :: ',sys._getframe().f_code.co_name);
        
        swing = swingBullish.SWING_STRATEGIES();

        df = getsymbol_historical_data(symbol,);      
        
        swing.bullish_swing(df,symbol,buy_stat_df,index);
        
        #print (' End :: ',sys._getframe().f_code.co_name);
        
    except Exception as exp:
        print('Caught exception SwingStrategy exp = ',exp );
    
    
#----------------------------------------------------------------------------------------------------------------------------------

# Get the current listed stock list in NSE for screener

def getscreendata(symbol,buy_stat_df,index):    
    try:
     
     breakout_stats=[];     
     
     broad_index = '^NSEI';
     #symbol historical dataframe
     
     symbol_hst_df = getsymbol_historical_data(symbol);
     
     period = getReferenceData_Equity().get_period(5);
     
     symbol_hst_df = symbol_hst_df.tail(period);
     
     
     #broadindex historical dataframe
     nse_broad_index_df = getbroadIndex_historical_data_dict(broad_index);
     
     #Sectorwise index dataframe
     stock_df = getsymbol_df();     
     sector_key = stock_df.loc[stock_df['Symbol'] == symbol, 'Industry'].iloc[0];     
     sector_index = getReferenceData_Equity().get_sector_indices(sector_key);
     
     if sector_index in getReferenceData_Equity().yahoo_main_indices.values():
         sector_index_df = getbroadIndex_historical_data_dict(sector_index);
     else:
         sector_index_df = getsectorIndex_historical_data_dict(sector_index);

     #    # Rsi comparator of stock with broad index
     # getrelative_comparator().isStockOutPerformRelativeIndex(symbol_hst_df,
     #                                                          nse_broad_index_df,
     #                                                          symbol,
     #                                                          broad_index,
     #                                                          buy_stat_df,
     #                                                          index
     #                                                          );
     
     
     # # # Rsi comparator of stock with sector index
     
     # # trends_strategy.RELATIVE_COMPARATOR().isStockOutPerformRelativeIndex(symbol_hst_df,
     # #                                                                        sector_index_df,
     # #                                                                        symbol,
     # #                                                                        sector_index,
     # #                                                                        relative_index_comparator_stats);
     
     # #------------------------------------------------------------------------------------------------------------
     
     
      # #   Check for breakout of stocks
     
     getbreakout_indicator().uptrendscreenBreakout(symbol, 
                                                     getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB(),
                                                     getMongoInstance(),
                                                     buy_stat_df,
                                                     index)
     
     
      #check narrow range consolidation
     getbreakout_indicator().isNarrowRageConsolidation(symbol_hst_df, buy_stat_df, index);
     
      #check narrow range breakout 
     getbreakout_indicator().isNarrowRageBreakout(symbol_hst_df, buy_stat_df, index);
     
     
      #check for Obv breakout
     
     
     getbreakout_indicator().obvUpTrendBreakout(symbol,
                                                 getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB(), 
                                                 getMongoInstance(), 
                                                 buy_stat_df, 
                                                 index);
     
     
     getbreakout_indicator().obvDepressBreakout(symbol,
                                                symbol_hst_df, 
                                                buy_stat_df, 
                                                index);
     # return breakout_comment;
    except Exception as exp:
        print('Caught exception at getscreendata [ ',exp, ' ]');
     
     
     
#---------------------------------------------------------------------------------------------------------------------------------------------------------
# volume profile screener 
#---------------------------------------------------------------------------------------------------------------------------------------------------------
def volumeProfile( symbol, buy_stat_df, index ):
    try:
        volume_profile_instance = getVOlumeProfileInstance();
       
        
        #for symbol in getsymbol_list():
        symbol_df = getsymbol_historical_data(symbol);
        volume_profile_instance.Bullish_volumeProfilling(symbol_df, symbol, buy_stat_df, index,getchartDrawInstance());
        
        
            
        # once whole symbolist traverse print the volume analysis
        buy_stat_df.loc[index,'Current_Price'] = int(symbol_df.iloc[-1]['Close']);
        buy_stat_df.loc[index,'Volume'] = int(symbol_df.iloc[-1]['Volume']);
      
    except Exception as exp:
            print( 'Exception caught ==> ',exp);


# currently putting start,end, config directory path here but will move to config (TO DO)
#---------------------------------------------------------------------------------------------------------------------------------------------------------

#1) Download nifty broad symbol list
def downloadReferenceData():
    
    print ('start :: ',sys._getframe().f_code.co_name);    
    try:
        
        size=0;
        
        df= pd.DataFrame();        
        for url in getReferenceData_Equity().nse_symbol_dict.items():
            
            if(len(df) <= 0):
                df=( getDownload_Nifty().downloadNiftyBroadIndices(url[1]));    
            else:            
                df_dummy=(getDownload_Nifty().downloadNiftyBroadIndices(url[1]));
                df = pd.concat([df,df_dummy]);        
        df.sort_values(["Symbol","ISIN Code"], inplace=True);
        
        df=df.drop_duplicates(subset=["Symbol","ISIN Code"],
                      keep='last', inplace=False,ignore_index=True);
        
        print ('end :: ',sys._getframe().f_code.co_name);   
        
        return df;
        
    except Exception as exp:
        print(' Caught Exception in downloadReferenceData = ',exp);


# First time loading the reference data

def init():
    try:
        schema_dict = {'symbol': 'Symbol', 'open': 'Open','close':'Close','high':'High','low':'Low','volume':'Volume','date':'Date'};
        
        #upload symbols to mongo DB 
        instance= getMongoInstance();
        instance.connect();
        
        symbol_df = downloadReferenceData();
        
        try:
            instance.dropDB(getReferenceData_Equity().get_NSE_Symbol_DB())
            instance.dropDB(getReferenceData_Equity().get_NSE_BROAD_INDICES_HIST_DATA_DB());
            instance.dropDB(getReferenceData_Equity().get_NSE_SECTOR_INDICES_HIST_DATA_DB());
            instance.dropDB(getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB());
            instance.dropDB(getReferenceData_Equity().Daily_BUY_SIDE_ANALYSIS_DB());
        except Exception as exp:
            pass;
        
       
        #Create Buy Side Stock Analysis 
        getMongoInstance().createDB(getReferenceData_Equity().Daily_BUY_SIDE_ANALYSIS_DB);
        
        # UPLOAD symbol list in NSE 
        # NSE (DB ) --> STOCK(Collection )
        instance.createCollection(getReferenceData_Equity().get_NSE_Symbol_DB(),
                                  getReferenceData_Equity().get_NSE_Symbol_Collection(),
                                  symbol_df);
        
        getReferenceData_Equity().load_symbol_list();
        
        setsymbol_list(getReferenceData_Equity().get_nse_listed_stock_list());

        
       
        #upload stock historial Data ,        
        symbol_list = getsymbol_list();
        
        #Create Stock historycal db
        instance.createDB(getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB());
                
        for symbol in symbol_list:
            sym_NS = symbol + getReferenceData_Equity().NSE_SCRIPT_EXTN;
                    
            history_data=getrefercedataYFinanceInstance().downloadHistorialPeriodforsymbol(sym_NS ,'15y','1d');
            
            history_data['Date']=pd.to_datetime(history_data.index);
            
            if 'close' in history_data.columns:
                history_data.rename(schema_dict, axis=1, inplace=True);

            history_data['Obv'] = (np.sign(history_data['Close'].diff()) * history_data['Volume']).fillna(0).cumsum()
                                    
            instance.createCollection(getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB(), 
                                      symbol, history_data);

        print('time historical data has been saved to Mongo for all broad nse stocks ...');
        
        #UPLOAD BROAD INDICES HISTORY 
        
        broad_indices_dict = getReferenceData_Equity().yahoo_main_indices;
        for item in broad_indices_dict.items():
            history_data=getrefercedataYFinanceInstance().downloadHistorialPeriodforsymbol(item[1] ,'15y','1d');
            history_data['Date']=pd.to_datetime(history_data.index);
            
            if 'close' in history_data.columns:
                history_data.rename(schema_dict, axis=1, inplace=True);
                
            instance.createCollection(getReferenceData_Equity().get_NSE_BROAD_INDICES_HIST_DATA_DB(), 
                                      item[1], 
                                      history_data);
            
        #upload sector-wise indices historycal data
        sector_indices_dict = getReferenceData_Equity().trading_view_nse_sector_indices;
        for item in sector_indices_dict.items():
           history_data= getTradingViewInstance().get_hist(symbol=item[1],exchange='NSE',interval=Interval.in_daily,n_bars=10000);
           history_data['Date']=pd.to_datetime(history_data.index);
           
           if 'close' in history_data.columns:
               history_data.rename(schema_dict, axis=1, inplace=True);
               
           instance.createCollection(getReferenceData_Equity().get_NSE_SECTOR_INDICES_HIST_DATA_DB(), 
                                     item[1], 
                                     history_data);
    except Exception as exp:
        print('Exception caught while uploading reference data to mongo exp  => ',exp )
        

    print('complete reference data uploaded .. HAPPY Trading with KTreaders ..');
    
    
# Daily update EOD stock ,broad and sector indices 

def daily_update():
    
    try:
        print (' start :: ',sys._getframe().f_code.co_name);
        
        schema_dict = {'symbol': 'Symbol', 'open': 'Open','close':'Close','high':'High','low':'Low','volume':'Volume','date':'Date'};
        
        getReferenceData_Equity().load_symbol_list();
        setsymbol_list(getReferenceData_Equity().get_nse_listed_stock_list());
        
        #update stocks historical data

        nse_stock_hist_db_handle = getReferenceData_Equity().get_NSE_STOCK_HIST_DATA_DB();   
                                 
        symbol_list= getsymbol_list();
               
        
        #Symbol list
        for symbol in symbol_list:
           sym_NS = symbol + getReferenceData_Equity().NSE_SCRIPT_EXTN;
           df =getrefercedataYFinanceInstance().downloadHistorialPeriodforsymbol(sym_NS, '1d', '1d');
           df['Date']=pd.to_datetime(df.index);
           
           if 'close' in df.columns:
               df.rename(schema_dict, axis=1, inplace=True);
               
           getMongoInstance().addRecordToCollection(nse_stock_hist_db_handle, symbol, df);
          
         
          
        #update Broad index 
        broad_indices_dict = getReferenceData_Equity().yahoo_main_indices;
        db = getReferenceData_Equity().get_NSE_BROAD_INDICES_HIST_DATA_DB();
       
        for item in broad_indices_dict.items():
            history_data=getrefercedataYFinanceInstance().downloadHistorialPeriodforsymbol(item[1] ,'1d','1d');
            history_data['Date']=pd.to_datetime(history_data.index);   
            
            if 'close' in history_data.columns:            
                history_data.rename(schema_dict, axis=1, inplace=True);
                
            getMongoInstance().addRecordToCollection(db, item[1], history_data);
      
      #upload nse sector indices 
      
        sector_indices_dict = getReferenceData_Equity().trading_view_nse_sector_indices;
        db = getReferenceData_Equity().get_NSE_SECTOR_INDICES_HIST_DATA_DB()
        
        for item in sector_indices_dict.items():
            history_data= getTradingViewInstance().get_hist(symbol=item[1],exchange='NSE',interval=Interval.in_daily,n_bars=1);
            history_data['Date']=pd.to_datetime(history_data.index);   
            
            if 'close' in history_data.columns:
                history_data.rename(schema_dict, axis=1, inplace=True);
                
            getMongoInstance().addRecordToCollection(db, item[1], history_data);
            
            
    except pymongo.errors.DuplicateKeyError:
            pass
    except Exception as exp:
        print('Caught exception in daily_update [ ',exp, ']');
    finally:
        print (' end :: ',sys._getframe().f_code.co_name);

#----------------------------------------------------------------------------------------------------------------------------------

#Perform the swing operation-------------------------------------------------------------------------------------------------------


    
#----------------------------------------------------------------------------------------------------------------------------------
'''
# Momentum trading :

Buy side:
---------
    1) Check trend is uptrend
    2) Check breakout 
    3) check relative index comparision with sector index and broad index 
    4) Check volume profile 

#----------------------------------------------------------------------------------------------------------------------------------

'''

def stock_screener(index,buy_stat_df):
    

    for symbol in getsymbol_list():
        print('Start Symbol ',symbol);
        
        buy_stat_df.loc[index,'Symbol']=symbol;
        
        print ('swing start')
        SwingStrategy(symbol,buy_stat_df,index);        
        print ('swing end  ')
        print('start screendata')
        getscreendata(symbol,buy_stat_df,index);
        print('end screendata')
        print('start volume profile ')
        volumeProfile(symbol,buy_stat_df,index);
        print('end volume profile ')
        
        index = index + 1;
        
    return index ;

def Main():
    instance= getMongoInstance();
    instance.connect();
    index = 0;
    
 
    print (' start :: ',sys._getframe().f_code.co_name);
    
    # reference data uploaded to mongo db
    #init();    
    
    #daily_update();

    buy_stat_df = getbuy_side_stat_df();
    load_reference_data();
    
    
    stock_screener(index,buy_stat_df);
    
    getMongoInstance().createCollection(getReferenceData_Equity().Daily_BUY_SIDE_ANALYSIS_DB, getBuysideCollectionName(), buy_stat_df);
    stat_patth = getchart_directory_path();
    
    buy_stat_df.to_csv(stat_patth + '\\' + getBuysideCollectionName() + '.csv' );
    
    #sendEmail();
    
    instance.disconnect();
    
    print (' end :: ',sys._getframe().f_code.co_name);


if __name__== "__main__":
    Main();