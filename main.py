# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 07:33:54 2022

@author: Ashwin
"""
import os
import sys
import datetime
import pandas as pd;
import pandas_ta as pd_ta;
import talib as ta;

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
sys.path.append("Screener");

import refDataDownloader;
import Bullish as swingBullish;
import Trend  as trend;
import mail;
import common;
import Mongo;
import trends_strategy;
import volumeIndicator;
import chartDrawing ;
import screener;

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
Keltner_Channel     = None;
chande_osccilator   = None;
Parabolic_SAR       = None;
Adx                 = None;
screener_trend      = None;

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

def getKeltner_Channel():
    global Keltner_Channel ;
    if ( Keltner_Channel == None ):
        Keltner_Channel = trends_strategy.KENTLER_CHANNEL();
    return Keltner_Channel;

def get_chandeOscillator():
    global chande_osccilator ;
    if ( chande_osccilator == None ):
        chande_osccilator = trends_strategy.chande_oscilattor();
        
    return chande_osccilator;

def get_Parabolic_SAR():
    
    global Parabolic_SAR;
    
    if(Parabolic_SAR == None):
        Parabolic_SAR = trends_strategy.Parabolic_SAR();
        
    return Parabolic_SAR;

def get_Adx():
    
    global Adx;
    
    if( Adx == None):
        Adx = trends_strategy.Adx_Indicator();
        
    return Adx;

def get_Screener():
    
    global screener_trend;
    
    if(screener_trend == None):
        screener_trend  = screener.Screen_Trade();
    
    return screener_trend;


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

#----------------------------------------------------------------------------

def getbuy_side_stat_df():
    
    dtypes = np.dtype(
            [
                ("Symbol", str),
                ("Volume", int),
                ("Current_Price",int),
                ("Engullfin", str),
                ("Trustinline", str),
                ("TweezerBottom", str),
                ("Bullish_Refine_Swing", str),
                ("Berish_Refine_Swing", str),
                ("200SMATrend",str),
                ("50SMATrend",str),
                ("20SMATrend",str),
                ("RsiOutperformNifty",str),
                ("RsiComparatorTreand",str),
                ("ConsolNarrowRange",str),                
                ("BreakoutNarrowRange",str),
                ("BreakoutWideRange",str),                
                ("LifeTimeHighZone",str),
                ("VPConvergence",str),
                ("VPDivergence",str),
                ("SellingClimax",str),
                ("PickVolumeBreakout",str),
                ("ObvBreakout",int),
                ("ObvBreakDown",int),
                ("DepressObvBreakout",str),
                ("Keltner_Channel",str),
                ("Chande_MO",str),
                ("PSAR",str),
                ("ADX",float),
                ("obv_adx_converse",str),
                ("obv_Psar_converse",str)
                
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
    df['Bullish_Refine_Swing']   = '';
    df['Berish_Refine_Swing']    = '';
    df['200SMATrend']            = '';
    df['50SMATrend']             = '';
    df['20SMATrend']             = '';
    df['RsiOutperformNifty']     = '';
    df['RsiComparatorTreand']    = '';    
    df['ConsolNarrowRange']      = '';
    df['BreakoutNarrowRange']    = '';    
    df['BreakoutWideRange']      = '';
    df['LifeTimeHighZone']       = '';
    df['VPConvergence']          = '';
    df['VPDivergence']           = '';
    df['SellingClimax']          = '';
    df['PickVolumeBreakout']     = '';
    df['ObvBreakout']            = 00;
    df['ObvBreakDown']           = 00;
    df['DepressObvBreakout']     = '';
    df['Keltner_Channel']        = '';
    df['Chande_MO']              = '';
    df['PSAR']                   = '';
    df['ADX']                    = 0.0;
    df['obv_adx_converse']       = '';
    df['obv_Psar_converse']      = '';
    
    return df;

#----------------------------------------------------------------------------
def getSwing_stat():
    
    dtypes = np.dtype(
            [
                ("Symbol", str),
                ("Volume", int),
                ("Current_Price",int),
                ("Engullfin", str),
                ("Trustinline", str),
                ("TweezerBottom", str),
                ("Bullish_Refine_Swing", str),
                ("Berish_Refine_Swing", str),
                ("Keltner_Channel", str),
                ("PSAR",str),
                ("ADX",float),
                ("obv_adx_converse",str),
                ("obv_Psar_converse",str),
                ("Strategy",str)
                
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
    df['Bullish_Refine_Swing']   = '';
    df['Berish_Refine_Swing']    = '';
    df['Keltner_Channel']        = '';
    df['PSAR']                   = '';
    df['ADX']                    = 0.0;
    df['obv_adx_converse']       = '';
    df['obv_Psar_converse']      = '';
    df['Strategy']               = '';
    
    return df;

#----------------------------------------------------------------------------

def getAbsolute_Momentum_stat():
    
    dtypes = np.dtype(
            [
                ("Symbol", str),
                ("Volume", int),
                ("Current_Price",int),              
                ("200SMATrend",str),
                ("50SMATrend",str),
                ("20SMATrend",str),                
                ("RsiOutperformNifty",str),                
                ("ConsolNarrowRange",str),                
                ("BreakoutNarrowRange",str),
                ("BreakoutWideRange",str),    
                ("VPConvergence",str),
                ("VPDivergence",str),
                ("ObvBreakout",int),
                ("ObvBreakDown",int),
                ("DepressObvBreakout",str),
                ("PSAR",str),
                ("ADX",float),
                ("obv_adx_converse",str),
                ("obv_Psar_converse",str),
                ("Strategy",str)
                
                
            ]
        );
    
    df = pd.DataFrame(np.empty(0, dtype=dtypes));
    df.set_index('Symbol');
    
    df['Symbol']                 = '';
    df['Current_Price']          = 00;
    df['Volume']                 = 00;    
    df['200SMATrend']            = '';
    df['50SMATrend']             = '';
    df['20SMATrend']             = '';
    df['RsiOutperformNifty']     = '';
    df['ConsolNarrowRange']      = '';
    df['BreakoutNarrowRange']    = '';    
    df['BreakoutWideRange']      = '';
    df['VPConvergence']          = '';
    df['VPDivergence']           = '';    
    df['ObvBreakout']            = 00;
    df['ObvBreakDown']           = 00;
    df['DepressObvBreakout']     = '';
    df['PSAR']                   = '';
    df['ADX']                    = 0.0;
    df['obv_adx_converse']       = '';
    df['obv_Psar_converse']      = '';
    df['Strategy']               = '';
    
    return df;
#----------------------------------------------------------------------------

def getVolume_Price_Momentum_stat():
    
    dtypes = np.dtype(
            [
                ("Symbol", str),
                ("Volume", int),
                ("Current_Price",int),      
                ("SellingClimax",str),
                ("PickVolumeBreakout",str),
                ("ObvBreakout",int),
                ("ObvBreakDown",int),
                ("DepressObvBreakout",str),
                ("PSAR",str),
                ("ADX",float),
                ("obv_adx_converse",str),
                ("obv_Psar_converse",str),
                ("Strategy",str)
                
            ]
        );
    
    df = pd.DataFrame(np.empty(0, dtype=dtypes));
    df.set_index('Symbol');
    
    df['Symbol']                 = '';
    df['Current_Price']          = 00;
    df['Volume']                 = 00;    
    df['SellingClimax']          = '';
    df['PickVolumeBreakout']     = '';
    df['ObvBreakout']            = 00;
    df['ObvBreakDown']           = 00;
    df['DepressObvBreakout']     = '';
    df['PSAR']                   = '';
    df['ADX']                    = 0.0;
    df['obv_adx_converse']       = '';
    df['obv_Psar_converse']      = '';
    df['Strategy']               = '';
    
    return df;

#----------------------------------------------------------------------------
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

        df = getsymbol_historical_data(symbol);      
              
        swing.bullish_swing(df,symbol,buy_stat_df,index);
        
        swing.refined_Swing(common,df,symbol,buy_stat_df,index);
        
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
      
     #-------------------------------------------------------------------------------------------
     
     # ADX and OBV in upswing convergence
     if(  get_Adx().isADXinUpSwing(symbol_hst_df) and 
          getbreakout_indicator().isOBVinUpSwing(symbol_hst_df) ):
        buy_stat_df.loc[index,'obv_adx_converse'] = str('Yes') ;
     else:
        buy_stat_df.loc[index,'obv_adx_converse'] = str('No') ;
        
     # PSAR and OBV in upswing convergence
     if( getbreakout_indicator().isOBVinUpSwing(symbol_hst_df) and 
         get_Parabolic_SAR().isPSARinUpSwing(symbol_hst_df) ):
         buy_stat_df.loc[index,'obv_Psar_converse'] = str('Yes') ;
     else:
         buy_stat_df.loc[index,'obv_Psar_converse'] = str('No') ;
     
     
     get_Parabolic_SAR().psar_trend(symbol_hst_df,buy_stat_df,index);
     
     get_Adx().Adx_trend(symbol_hst_df,buy_stat_df,index);
 
     get_chandeOscillator().CMO_strategy(symbol,symbol_hst_df,10,buy_stat_df,index);
      
     getKeltner_Channel().kentler_channel_formation(getchartDrawInstance(),common,symbol,symbol_hst_df,buy_stat_df,index);

     getbreakout_indicator().is200_LongTermUpTrend(getchartDrawInstance(),common,symbol,symbol_hst_df,buy_stat_df,index);

     getbreakout_indicator().is50_MediumTermUpTrend(getchartDrawInstance(),common,symbol,symbol_hst_df,buy_stat_df,index);
    
     getbreakout_indicator().is20_ShortTermUpTrend(getchartDrawInstance(),common,symbol,symbol_hst_df,buy_stat_df,index);
     
     
     
     # Rsi comparator of stock with broad index
     getrelative_comparator().isStockOutPerformRelativeIndex(symbol_hst_df,
                                                               nse_broad_index_df,
                                                               symbol,
                                                               broad_index, 
                                                               buy_stat_df,
                                                               index
                                                               );
     
     
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
     
     
     # check is obv and ADX are in converge 
 
        
     
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

#------------------------------------------Perform the swing operation-------------------------------------------------------------

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
def init_stat_df(buy_stat_df,index):
    
    buy_stat_df.loc[index,'Symbol'] 				= "No";
    buy_stat_df.loc[index,'Volume'] 				= 0;
    buy_stat_df.loc[index,'Current_Price']			= 0;
    buy_stat_df.loc[index,'Engullfin'] 				= "No";
    buy_stat_df.loc[index,'Trustinline'] 			= "No";
    buy_stat_df.loc[index,'TweezerBottom'] 			= "No";
    buy_stat_df.loc[index,'200SMATrend'] 			= "No";
    buy_stat_df.loc[index,'50SMATrend']  			= "No";
    buy_stat_df.loc[index,'20SMATrend']  			= "No";
    buy_stat_df.loc[index,'RsiOutperformNifty']  	= "No";
    buy_stat_df.loc[index,'RsiComparatorTreand']  	= "No";
    buy_stat_df.loc[index,'ConsolNarrowRange']  	= "No";
    buy_stat_df.loc[index,'BreakoutNarrowRange']  	= "No";
    buy_stat_df.loc[index,'BreakoutWideRange']  	= "No";
    buy_stat_df.loc[index,'LifeTimeHighZone']  		= "No";
    buy_stat_df.loc[index,'VPConvergence']  		= "No";
    buy_stat_df.loc[index,'VPDivergence']  			= "No";
    buy_stat_df.loc[index,'SellingClimax']  		= "No";
    buy_stat_df.loc[index,'PickVolumeBreakout']  	= "No";
    buy_stat_df.loc[index,'ObvBreakout']  			= 0;
    buy_stat_df.loc[index,'ObvBreakDown']  			= 0;
    buy_stat_df.loc[index,'DepressObvBreakout']  	= "No";
    buy_stat_df.loc[index,'Keltner_Channel']  		= "No";
    buy_stat_df.loc[index,'Chande_MO']  			= "No";
    buy_stat_df.loc[index,'PSAR']  					= 0.0;
    buy_stat_df.loc[index,'obv_adx_converse']  		= "No";
    buy_stat_df.loc[index,'obv_Psar_converse']  	= "No";
    
    return buy_stat_df;

    
    

def stock_screener(index,buy_stat_df):
    
    volume_filter = 500 * 1000;
    for symbol in getsymbol_list():
        
        df = getsymbol_historical_data(symbol);
        
        sma_vol_14  = list(pd_ta.sma(df['Volume'],length=14,talib=False));
       
        if(sma_vol_14[-1] < volume_filter):
            #print (symbol,' skippinmg symbol volume [ ', sma_vol_14[-1], ' ]');
            #input();
            continue;
            
        
        buy_stat_df = init_stat_df(buy_stat_df,index);
        
        #----------------------------------------------------------------------
        print('Start Symbol ',symbol);        
        buy_stat_df.loc[index,'Symbol']=symbol;
        #----------------------------------------------------------------------
        print ('swing start');        
        SwingStrategy(symbol,buy_stat_df,index);        
        print ('swing end  ');
        #----------------------------------------------------------------------
        print('start screendata');
        getscreendata(symbol,buy_stat_df,index);
        print('end screendata');
        #----------------------------------------------------------------------
        print('start volume profile ');
        volumeProfile(symbol,buy_stat_df,index);
        print('end volume profile ');
        #----------------------------------------------------------------------
        index = index + 1;
        #----------------------------------------------------------------------
        
    return index ;


# buy_stat_df.loc[index_cnt,'200SMATrend'] = 'Yes';
def create_Reports(buy_stat_df):

    
    Swing_stat_df               = getSwing_stat();
    
    Absolute_Momentum_stat      = getAbsolute_Momentum_stat();
    
    Volume_Price_Momentum_stat  = getVolume_Price_Momentum_stat();
    
    len_df = len(buy_stat_df);
       
    
    for cnt in range(0,len_df-1):
        #--------------------------------------------------------------------------------------------
        
        Swing_stat_df.loc[cnt,'Symbol'] = buy_stat_df.iloc[cnt]['Symbol'];
        Swing_stat_df.loc[cnt,'Volume'] = buy_stat_df.iloc[cnt]['Volume'];
        Swing_stat_df.loc[cnt,'Current_Price'] = buy_stat_df.iloc[cnt]['Current_Price'];
        Swing_stat_df.loc[cnt,'Engullfin'] = buy_stat_df.iloc[cnt]['Engullfin'];
        Swing_stat_df.loc[cnt,'Trustinline'] = buy_stat_df.iloc[cnt]['Trustinline'];
        Swing_stat_df.loc[cnt,'TweezerBottom'] = buy_stat_df.iloc[cnt]['TweezerBottom'];
        Swing_stat_df.loc[cnt,'Bullish_Refine_Swing'] = buy_stat_df.iloc[cnt]['Bullish_Refine_Swing'];
        Swing_stat_df.loc[cnt,'Berish_Refine_Swing'] = buy_stat_df.iloc[cnt]['Berish_Refine_Swing'];
        Swing_stat_df.loc[cnt,'Keltner_Channel'] = buy_stat_df.iloc[cnt]['Keltner_Channel'];
        Swing_stat_df.loc[cnt,'PSAR'] = buy_stat_df.iloc[cnt]['PSAR'];
        Swing_stat_df.loc[cnt,'ADX'] = buy_stat_df.iloc[cnt]['ADX'];
        Swing_stat_df.loc[cnt,'obv_adx_converse'] = buy_stat_df.iloc[cnt]['obv_adx_converse'];
        Swing_stat_df.loc[cnt,'obv_Psar_converse'] = buy_stat_df.iloc[cnt]['obv_Psar_converse'];
        
        Absolute_Momentum_stat.loc[cnt,'Symbol'] = buy_stat_df.iloc[cnt]['Symbol'];
        Absolute_Momentum_stat.loc[cnt,'Volume'] = buy_stat_df.iloc[cnt]['Volume'];
        Absolute_Momentum_stat.loc[cnt,'Current_Price'] = buy_stat_df.iloc[cnt]['Current_Price'];
        Absolute_Momentum_stat.loc[cnt,'200SMATrend'] = buy_stat_df.iloc[cnt]['200SMATrend'];
        Absolute_Momentum_stat.loc[cnt,'50SMATrend'] = buy_stat_df.iloc[cnt]['50SMATrend'];
        Absolute_Momentum_stat.loc[cnt,'20SMATrend'] = buy_stat_df.iloc[cnt]['20SMATrend'];
        Absolute_Momentum_stat.loc[cnt,'RsiOutperformNifty'] = buy_stat_df.iloc[cnt]['RsiOutperformNifty'];
        Absolute_Momentum_stat.loc[cnt,'ConsolNarrowRange'] = buy_stat_df.iloc[cnt]['ConsolNarrowRange'];
        Absolute_Momentum_stat.loc[cnt,'BreakoutNarrowRange'] = buy_stat_df.iloc[cnt]['BreakoutNarrowRange'];
        Absolute_Momentum_stat.loc[cnt,'BreakoutWideRange'] = buy_stat_df.iloc[cnt]['BreakoutWideRange'];
        Absolute_Momentum_stat.loc[cnt,'VPConvergence'] = buy_stat_df.iloc[cnt]['VPConvergence'];
        Absolute_Momentum_stat.loc[cnt,'VPDivergence'] = buy_stat_df.iloc[cnt]['VPDivergence'];
        Absolute_Momentum_stat.loc[cnt,'ObvBreakout'] = buy_stat_df.iloc[cnt]['ObvBreakout'];
        Absolute_Momentum_stat.loc[cnt,'ObvBreakDown'] = buy_stat_df.iloc[cnt]['ObvBreakDown'];    
        Absolute_Momentum_stat.loc[cnt,'PSAR'] = buy_stat_df.iloc[cnt]['PSAR'];
        Absolute_Momentum_stat.loc[cnt,'ADX'] = buy_stat_df.iloc[cnt]['ADX'];
        Absolute_Momentum_stat.loc[cnt,'obv_adx_converse'] = buy_stat_df.iloc[cnt]['obv_adx_converse'];
        Absolute_Momentum_stat.loc[cnt,'obv_Psar_converse'] = buy_stat_df.iloc[cnt]['obv_Psar_converse'];
        
        Volume_Price_Momentum_stat.loc[cnt,'Symbol'] = buy_stat_df.iloc[cnt]['Symbol'];
        Volume_Price_Momentum_stat.loc[cnt,'Volume'] = buy_stat_df.iloc[cnt]['Volume'];
        Volume_Price_Momentum_stat.loc[cnt,'Current_Price'] = buy_stat_df.iloc[cnt]['Current_Price'];
        Volume_Price_Momentum_stat.loc[cnt,'SellingClimax'] = buy_stat_df.iloc[cnt]['SellingClimax'];
        Volume_Price_Momentum_stat.loc[cnt,'PickVolumeBreakout'] = buy_stat_df.iloc[cnt]['PickVolumeBreakout'];
        Volume_Price_Momentum_stat.loc[cnt,'ObvBreakout'] = buy_stat_df.iloc[cnt]['ObvBreakout'];
        Volume_Price_Momentum_stat.loc[cnt,'ObvBreakDown'] = buy_stat_df.iloc[cnt]['ObvBreakDown'];
        Volume_Price_Momentum_stat.loc[cnt,'DepressObvBreakout'] = buy_stat_df.iloc[cnt]['DepressObvBreakout'];
        Volume_Price_Momentum_stat.loc[cnt,'PSAR'] = buy_stat_df.iloc[cnt]['PSAR'];
        Volume_Price_Momentum_stat.loc[cnt,'ADX'] = buy_stat_df.iloc[cnt]['ADX'];
        Volume_Price_Momentum_stat.loc[cnt,'obv_adx_converse']  = buy_stat_df.iloc[cnt]['obv_adx_converse'];
        Volume_Price_Momentum_stat.loc[cnt,'obv_Psar_converse'] = buy_stat_df.iloc[cnt]['obv_Psar_converse'];
        
        
        
        #--------------------------------------------------------------------------------------------  
        if( get_Screener().swing_screener_1(buy_stat_df.iloc[cnt]) ):            
            Swing_stat_df.loc[cnt,'Strategy'] = "swing_screener_1";
        
        if(get_Screener().swing_screener_2(buy_stat_df.iloc[cnt])):
            Swing_stat_df.loc[cnt,'Strategy'] = "swing_screener_2";
            
        #--------------------------------------------------------------------------------------------  
        if(get_Screener().momentum_screener_1(buy_stat_df.iloc[cnt]) ):        
            Absolute_Momentum_stat.loc[cnt,'Strategy'] = "momentum_screener_1";
        
        elif(get_Screener().momentum_screener_2(buy_stat_df.iloc[cnt]) ):        
            Absolute_Momentum_stat.loc[cnt,'Strategy'] = "momentum_screener_2";
            
        elif(get_Screener().momentum_screener_3(buy_stat_df.iloc[cnt]) ):            
            Absolute_Momentum_stat.loc[cnt,'Strategy'] = "momentum_screener_3";
        else:
            pass;
        #--------------------------------------------------------------------------------------------  
        
        if(get_Screener().Volume_Price_Momentum_screener_1(buy_stat_df.iloc[cnt]) ):
            Volume_Price_Momentum_stat.loc[cnt,'Strategy'] = "Volume_Price_Momentum_screener_1";
        
        if(get_Screener().Volume_Price_Momentum_screener_2(buy_stat_df.iloc[cnt]) ):
            Volume_Price_Momentum_stat.loc[cnt,'Strategy'] = "Volume_Price_Momentum_screener_2";
            
        #--------------------------------------------------------------------------------------------
        

        
    stat_patth = getchart_directory_path();
    
    Swing_stat_df.to_csv(stat_patth + '\\' + "Swing" + '.csv' );
    Absolute_Momentum_stat.to_csv(stat_patth + '\\' + "Momentum" + '.csv' );
    
    Volume_Price_Momentum_stat.to_csv(stat_patth + '\\' + "Volume_Price_Momentum" + '.csv' );
        

def Main():
    
    buy_stat_df  = getbuy_side_stat_df();
    instance= getMongoInstance();
    instance.connect();
    index = 0;
    
 
    print (' start :: ',sys._getframe().f_code.co_name);
    
    #reference data uploaded to mongo db
    
    #init();
    
    #daily_update();

    
    load_reference_data();
        
    stock_screener(index,buy_stat_df);

    getMongoInstance().createCollection(getReferenceData_Equity().Daily_BUY_SIDE_ANALYSIS_DB, getBuysideCollectionName(), buy_stat_df);
    
    
    stat_patth = getchart_directory_path();
    
    buy_stat_df.to_csv(stat_patth + '\\' + getBuysideCollectionName() + '.csv' );
    
    create_Reports(buy_stat_df);
    
    #sendEmail();
    
    instance.disconnect();
    
    print (' end :: ',sys._getframe().f_code.co_name);


if __name__== "__main__":
    Main();