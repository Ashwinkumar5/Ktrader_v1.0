# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 08:23:27 2022

@author: Ashwin
"""
import pandas as pd;
import numpy as ny;
import datetime;
import os
import sys
import pymongo

'''
 threshold = 1 - (percent/100);
 
 df= self.download.downloadHistorialPeriodforsymbol(sym, period, interval);
 max_close= df['Close'].max();
 min_close= df['Close'].min();        
 if( min_close >= (max_close * threshold ) ):
    print (min_close ," : ",max_close,": ",(max_close * threshold ));
    return True;
 else:
     return False;
 
'''

class TREND_SCREENER(object):
    
    def __init__(self):
        pass
    
    
    def isNarrowRageConsolidation(symbol,data,buy_stat_df,index_cnt):
        
        percent = 5
        
        threshold = 1 - (percent/100);
        
        narrow_period = 14;
        
        df = data.tail(narrow_period);
        max_close = df['Close'].max();
        min_close = df['Close'].min();
        
        narrow_bench_mark = max_close * threshold;
        
        narrow_bench_mark  = narrow_bench_mark if( narrow_bench_mark > min_close) else min_close;
        
        current_close = df.iloc[-1]['Close'];
        
        if(max_close > current_close and  min_close < narrow_bench_mark ):
            buy_stat_df.loc[index_cnt,'ConsolNarrowRange'] = 'Yes';
        
        buy_stat_df.loc[index_cnt,'Current_Price'] = int(current_close);
        
        
    
    def isNarrowRageBreakout(symbol,data,buy_stat_df,index_cnt):
        
        narrow_period = 14 + 1;
        
        df = data[-narrow_period:-1];
        
        max_close = df['Close'].max();
        min_close = df['Close'].min();
        current_close = data.iloc[-1]['Close'];
        
        prev_day_vol = df.iloc[-1]['Close'];
        curr_day_vol = data.iloc[-1]['Volume'];
        
        if(max_close < current_close and prev_day_vol < curr_day_vol ):
            buy_stat_df.loc[index_cnt,'BreakoutNarrowRange'] = 'Yes';
            
    

    
    '''

    '''
    def isBreakOutPeriod(self,symbol,data,period_days,df_trend,index_trend,buy_stat_df,index_cnt):
        
        df = data.tail(period_days);
        max_price = df['Close'].max();
        min_price = df['Close'].min();
        
        current_price = data.iloc[-1]['Close'];
                    
        if( (current_price > max_price) and (
                current_price > df.iloc[-2]['Close'] and 
                df.iloc[-3]['Close'] < df.iloc[-2]['Close']) ):
            df_trend = df_trend.iloc[0:0];
            
            buy_stat_df.loc[index_cnt,'BreakoutWideRange'] = str(period_days) ;
            print('--------------------------------------------------------------------------------------');
            
        elif((current_price == max_price) and (
                current_price > df.iloc[-2]['Close'] and 
                df.iloc[-3]['Close'] < df.iloc[-2]['Close']) ):  
            df_trend = df_trend.iloc[0:0];
            buy_stat_df.loc[index_cnt,'BreakoutWideRange'] = str(period_days) ;
            print('--------------------------------------------------------------------------------------');
            
        else:
            #Comment :  This block will take in consideration in stock movement analysis 
            if( ( current_price < max_price and 
                  current_price > min_price ) and (
                  current_price > df.iloc[-2]['Close'] and 
                  df.iloc[-3]['Close'] < df.iloc[-2]['Close'])):
                          
                  df_trend.loc[index_trend] = [max_price,min_price,current_price,period_days];
                  # print(symbol,' Stock is in uptrend moving towards resistance do check for ' );
                  # print('Resistance for period ',period_days,' resistance price = ',max_price, ' Support price = ',min_price,'  current_stock price ',current_price );          
                  # #tm.sleep(5);
                  return False;
        return True;
    
    
    
    '''
        Screen stocks for break out of life time high for period of 
        2510 ::-> 10 years
        2259 ::-> 9 years
        2008 ::-> 8 years
        1757 ::-> 7 years
        1506 ::-> 6 years
        1260 ::-> 5 years
        1008 ::-> 4 years
        756, ::-> 3 years
        504  ::-> 2 years
        252  ::-> 1 years
        189  ::-> 9 month;
        126  ::-> 6 month
        63   ::-> 3 month
        21   ::-> 1 month
        validate the current price greater than period life time high 
            1) breakout for period in descending order 
        for stock dont have any breakout which indicate stock 
            1) stock is near lifetime high or uncharted territory 
        these are all sign of uptrend
        
    '''    
   
    
    def uptrendscreenBreakout(self,symbol,stock_hist_db,MongoInstance,buy_stat_df,index_cnt):
      
       breakout=[2510,2259,2008,1757,1506,1260,1008,756,504,252,189,126,63,21];     
       
       
       end_date = datetime.datetime.today();
       
       df_trend = pd.DataFrame(data=ny.array([[0,0,0,0]]),index=[0],columns=['Resistance','Support','Current','Period']);
       df_local = pd.DataFrame(data=ny.array([[0,0,0,0]]),index=[0],columns=['Resistance','Support','Current','Period']);
       
       index= 0;
       
       for period in breakout:

          start_date = (datetime.datetime.today() - datetime.timedelta(days=period));
           
          stock_df = MongoInstance.queryBetweenDates(stock_hist_db,symbol,start_date,end_date,period);
          
          if( self.isBreakOutPeriod(symbol,stock_df,period,df_trend,index,buy_stat_df,index_cnt) ):
              break;
          index = index +1;
          
       if(len(df_trend) >= len(breakout) -1):

           df_local = df_trend.copy();
           df_local['Duplicate'] = df_local['Resistance'].duplicated();
          
           
           if( (df_local['Resistance'].is_unique) or 
               (df_local.groupby('Duplicate').size()[1] ) >=8 ):
               buy_stat_df.loc[index_cnt,'LifeTimeHighZone']='Yes';
              
           
           df_local=df_local.iloc[0:0];
           
       df_trend=df_trend.iloc[0:0];
       
      
#------------------------------------------------------------------------------------------------------------------------------------------


'''
   Relative strength index comparator 
   db_name,collection,period):
'''
class RELATIVE_COMPARATOR(object):
    def __init__(self):
        
        pass;
        
   
    def loadIndices(self,refDataDownloader,MongoInstance):
        
        df_indices = {};
        
        broad_indices_db  =   refDataDownloader.Download_Nifty_Symbols().getEnquityObject().NSE_BROAD_INDICES_HIST_DATA;
        sector_indices_db =   refDataDownloader.Download_Nifty_Symbols().getEnquityObject().NSE_SECTOR_INDICES_HIST_DATA;
        
        period   = refDataDownloader.Download_Nifty_Symbols().getEnquityObject().get_period(5);
        
        #loading braod indices Data
        
        for item in refDataDownloader.Download_Nifty_Symbols().getEnquityObject().yahoo_main_indices.items():            
           df = MongoInstance.getCollectionforPeriod(broad_indices_db,item[1],period);
           df_indices[item[1]] = df;
      
        #loading sector indices Data
        for item in refDataDownloader.Download_Nifty_Symbols().getEnquityObject().trading_view_nse_sector_indices.items():
           df = MongoInstance.getCollectionforPeriod(sector_indices_db,item[1],period);
           df_indices[item[1]] = df;
        
        return df_indices;
     

#-----------------------------------------------------------------------------------------------------------------------
    def getRelativeStrengthIndexComparatorWithMovingAvg(self,stock_history,index_history):
           
        
         try:
            
             #print (' start :: ',sys._getframe().f_code.co_name);
             
             # get current close price
             current_close = stock_history.iloc[-1]['Close'];
             #get total length of stock_history             
             
             length = len(stock_history);
             
             relative_comparator_list=[];
             relative_comparator_date_list=[];
              
             # compute stock historical data with relative to index historycal data 
             
             for cnt in range (0,length-1):
                  relative_comparator_list.append(float( stock_history.iloc[cnt]['Close']/ index_history.iloc[cnt]['Close']) );
                  relative_comparator_date_list.append(stock_history.iloc[cnt]['Date']);

             # convert to dataframe to get 100 days moving average
             relative_comparator_df=pd.DataFrame(relative_comparator_list, columns=['relative_comparator_list']);
             #calculate 100 days moving average
             relative_comparator_moving_average_rolling = relative_comparator_df.rolling(window=100).mean()
             # converting to list 
             relative_comparator_moving_average_list=list(relative_comparator_moving_average_rolling['relative_comparator_list']);
        
             stock_history.iloc[0:0];         
             index_history.iloc[0:0];
             
             #print (' end :: ',sys._getframe().f_code.co_name);
             
             return relative_comparator_date_list,relative_comparator_list,relative_comparator_moving_average_list,current_close
         
         except Exception as exp:
             print('Exception caught =>  :: ',exp,' stock_history len = ',len(stock_history),'index len ',len(index_history));

#--------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
    def isStockOutPerformRelativeIndex(self,stock_df,index_df,symbol,index,stat_buy_df,index_cnt):
       
        #print (' start :: ',sys._getframe().f_code.co_name);
        try:

            relative_index_com_date,relative_index_com_price,relative_index_com_price_100_moving_avg,closing_price=self.getRelativeStrengthIndexComparatorWithMovingAvg(stock_df,index_df);        
            
            cnt=1;
                              
            if(relative_index_com_price[-cnt] >  relative_index_com_price_100_moving_avg[-cnt]):
                while(relative_index_com_price[-cnt] > relative_index_com_price_100_moving_avg[-cnt]):
                    cnt = cnt + 1;
               # if relative index cross over the mobing average 
                if( cnt <= 14 and closing_price < 500):                 
                    str_ = symbol + 'Is outperforming   the  index = '+ index +' .... from '+ str(relative_index_com_date[-cnt])+ ' and total days ='+ str(cnt) +' current price = '+ str(closing_price);
                    stat_buy_df.loc[index_cnt,'RsiComparator'] = 'Yes';
                  
            
            #------------------------------------------------------------------------------
            # Check RSI Comparator Tread
            
            if( ( relative_index_com_price[-1] > relative_index_com_price[-2] ) and
                ( relative_index_com_price[-2] > relative_index_com_price[-3] ) and
                ( relative_index_com_price[-3] > relative_index_com_price[-4] ) and
                ( relative_index_com_price[-4] > relative_index_com_price[-5] ) ):
                
                    stat_buy_df.loc[index_cnt,'RsiComparatorTreand'] = 'UpTrend';
            else:
                    stat_buy_df.loc[index_cnt,'RsiComparatorTreand'] = 'DownTrend';
                
            
            
            relative_index_com_date.clear();
            relative_index_com_price.clear();
            relative_index_com_price_100_moving_avg.clear();
            closeing_price=0;
            
        except Exception as exp:
            print('Caught exception isStockOutPerformRelativeIndex -> [ ',exp,']');
        
        #print (' end :: ',sys._getframe().f_code.co_name);
