# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 08:23:27 2022

@author: Ashwin
"""
import pandas as pd;
import numpy as np;
import datetime;
import os
import sys
import pymongo
import talib as ta;
import pandas_ta as pd_ta;
import tradingview_ta as tv;

import math

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
#-------------------------------------------------------------------------------------        
    
    # 200 SMA is in upward slop
    def is200_LongTermUpTrend(self,chart,common,symbol,data,buy_stat_df,index_cnt):
        try:
        
            data.fillna(0);
            
            dfsma_200 = common.getSMA(data,200,'Close');
            
            dfsma_200.fillna(0);
            buy_stat_df.fillna(0);
            
            dx = ta.LINEARREG_SLOPE(dfsma_200['Close'], timeperiod=200);
            
            dx.fillna(0);
            
            
            if( not math.isnan(dx.iloc[-1]) and int(dx.iloc[-1]) > 0 ):
               buy_stat_df.loc[index_cnt,'200SMATrend'] = 'Yes';
               return True;
            else:            
               buy_stat_df.loc[index_cnt,'200SMATrend'] = 'No';
               return False;
           
        except Exception as exp:
             print(' Exception in is200_LongTermUpTrend [ ',exp,' ]');
        
    # 50 SMA is in upward slop
    def is50_MediumTermUpTrend(self,chart,common,symbol,data,buy_stat_df,index_cnt):
        
        data.fillna(0);
        dfsma_50 = common.getSMA(data,50,'Close');
        
        buy_stat_df.fillna(0);
        dfsma_50.fillna(0);
        
        dx = ta.LINEARREG_SLOPE(dfsma_50['Close'], timeperiod=50);
        dx.fillna(0);
        
        if( not math.isnan(dx.iloc[-1]) and int(dx.iloc[-1]) > 0 ):
           buy_stat_df.loc[index_cnt,'50SMATrend'] = 'Yes';
           return True;
        else:
           buy_stat_df.loc[index_cnt,'50SMATrend'] = 'No';
           return False;
        
        
    # 20 SMA is in upward slop
    def is20_ShortTermUpTrend(self,chart,common,symbol,data,buy_stat_df,index_cnt):
        data.fillna(0);
        dfsma_20 = common.getSMA(data,20,'Close');
        dfsma_20.fillna(0);
        buy_stat_df.fillna(0);
        
        dx = ta.LINEARREG_SLOPE(dfsma_20['Close'], timeperiod=20);
        dx.fillna(0);
        
        if( not math.isnan(dx.iloc[-1]) and int(dx.iloc[-1]) > 0 ):
           buy_stat_df.loc[index_cnt,'20SMATrend'] = 'Yes';
           return True;
        else:
           buy_stat_df.loc[index_cnt,'20SMATrend'] = 'No';
           return False;
       
        
#-------------------------------------------------------------------------------------        
    
    def isNarrowRageConsolidation(self,data,buy_stat_df,index_cnt):
        
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
        
        
        
        
    
    def isNarrowRageBreakout(self,data,buy_stat_df,index_cnt):
        
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
       
       df_trend = pd.DataFrame(data=np.array([[0,0,0,0]]),index=[0],columns=['Resistance','Support','Current','Period']);
       df_local = pd.DataFrame(data=np.array([[0,0,0,0]]),index=[0],columns=['Resistance','Support','Current','Period']);
       
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
       
       
      
#-------------------------------------------------------------------------------------------------

    def obvUpTrendBreakout(self,symbol,stock_hist_db,MongoInstance,buy_stat_df,index_cnt):
    
        try:
            breakout=[63,21];     
            end_date = datetime.datetime.today();
                    
            index= 0;
               
            for period in breakout:
              start_date = (datetime.datetime.today() - datetime.timedelta(days=period));
                   
              stock_df = MongoInstance.queryBetweenDates(stock_hist_db,symbol,start_date,end_date,period);
              
              df = stock_df[-period:-1];
              

              max_Obv = df['Obv'].max();
              min_Obv = df['Obv'].min();
              
              current_obv = stock_df.iloc[-1]['Obv'];

              
              
              if( current_obv >  max_Obv):
                  buy_stat_df.loc[index_cnt,'ObvBreakout'] = period ;
                  print ('Symbol = > ',symbol,current_obv,'>',max_Obv,' Period => ', period);                  
                  return;
              elif (current_obv < min_Obv):
                  buy_stat_df.loc[index_cnt,'ObvBreakDown'] = 1 ;
                  buy_stat_df.loc[index_cnt,'ObvBreakout']  =  0 ;
              else:
                  buy_stat_df.loc[index_cnt,'ObvBreakDown'] = 0 ;
                  buy_stat_df.loc[index_cnt,'ObvBreakout']  =  0 ;
                  
                  
        except Exception as exp:
                print('Caught Exception in obvUpTrendBreakout',exp);

#-------------------------------------------------------------------------------------------------

    def isObvDepress(self,stock_hist_db,current_obv,start_period):    
        
        end_period = start_period + 2;
       
        for cnt in range (start_period,end_period):
            if( not (current_obv < stock_hist_db.iloc[-cnt]['Obv'])):       
               return False;
    
        return True;
        

    def obvDepressBreakout(self,symbol,stock_hist_db,buy_stat_df,index_cnt):
    
        try:
       
            current_obv = stock_hist_db.iloc[-1]['Obv'];
            prev_day_ob = stock_hist_db.iloc[-2]['Obv'];
       
            if( (current_obv > 0 and prev_day_ob > 0)  and self.isObvDepress(stock_hist_db,prev_day_ob,3) ):
       
                if(current_obv > prev_day_ob):       
                    buy_stat_df.loc[index_cnt,'DepressObvBreakout'] = str('Yes') ;
       
        except Exception as exp:
            print('Caught exception in obvDepressBreakout => ',exp)
            
            
            
    def isOBVinUpSwing(self,stock_hist_db):
        
        try:
            obv_sma_5 = ta.SMA(stock_hist_db['Obv'],timeperiod=5);
            
            if(obv_sma_5.iloc[-2] < stock_hist_db.iloc[-1]['Obv'] and 
               (stock_hist_db.iloc[-1]['Obv'] > stock_hist_db.iloc[-2]['Obv']) and 
               (stock_hist_db.iloc[-1]['Obv']) > stock_hist_db.iloc[-3]['Obv']):
                return True;
            else:
                return False;
            
        except Exception as exp:
            print('Caught Exception [ ',exp,' ]');
            
      
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
                '''
                if( cnt <= 14 and closing_price < 500):                 
                    str_ = symbol + 'Is outperforming   the  index = '+ index +' .... from '+ str(relative_index_com_date[-cnt])+ ' and total days ='+ str(cnt) +' current price = '+ str(closing_price);
                    stat_buy_df.loc[index_cnt,'RsiOutperformNifty'] = 'Yes';
                else:
                    stat_buy_df.loc[index_cnt,'RsiOutperformNifty'] = 'No';
                 '''
                str_ = symbol + 'Is outperforming   the  index = '+ index +' .... from '+ str(relative_index_com_date[-cnt])+ ' and total days ='+ str(cnt) +' current price = '+ str(closing_price);
                stat_buy_df.loc[index_cnt,'RsiOutperformNifty'] = 'Yes';
                 
            else:
                stat_buy_df.loc[index_cnt,'RsiOutperformNifty'] = 'No';
            
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


#--------------------------------------------------------------------------------------------------------------------------------------------------------------------
'''
    UPPER BAND 20 = EMA 20 [ C.STOCK ] + MULTIPLIER * ATR 10
    LOWER BAND 20 = EMA 20 [ C.STOCK ] - MULTIPLIER * ATR 10
'''

class KENTLER_CHANNEL(object):

    def __init__(self):
        self.kentler_Upper  = [];
        self.kentler_Lower  = [];
        self.kentler_Middle = pd.DataFrame();
        
        self.ATR            = pd.DataFrame();
        
        self.trendScanner   = TREND_SCREENER();
        self.channel_period = 10;
        self.multiplier     = 2;
        self.ATR_period     = 10;
        

    def kentler_channel_formation(self,chart, common, symbol, data, buy_stat_df, index_cnt):
        
        #Check if stock is in uptrend for short and medium period
        
        try:
            df_len = len(data);
            
            if( ( self.trendScanner.is20_ShortTermUpTrend(chart, common, symbol, data, buy_stat_df, index_cnt) ) and
                ( self.trendScanner.is50_MediumTermUpTrend(chart, common, symbol, data, buy_stat_df, index_cnt) ) ):
                
                  self.kentler_Middle = common.getDMA(data,self.channel_period,'Close');
                                    
                  self.ATR = ta.ATR(data['High'], 
                                    data['Low'], 
                                    data['Close'],
                                    self.ATR_period);
                  
                  self.ATR.fillna(0);
                  
                  for cnt in range(0,df_len-1):

                      self.kentler_Upper.append( self.kentler_Middle.iloc[cnt]['Close'] + (self.multiplier * self.ATR.iloc[cnt]) );
                 
                      self.kentler_Lower.append( self.kentler_Middle.iloc[cnt]['Close'] - (self.multiplier * self.ATR.iloc[cnt]) );
                 
                  if( ( data.iloc[-1]['Close'] >= self.kentler_Middle.iloc[-1]['Close'] ) and 
                      ( data.iloc[-1]['Close'] < self.kentler_Upper[-1] ) and 
                      ( data.iloc[-1]['Close'] > self.kentler_Lower[-1] ) and 
                      ( data.iloc[-2]['Close'] < self.kentler_Middle.iloc[-2]['Close']) ):                      
                      buy_stat_df.loc[index_cnt,'Keltner_Channel'] = 'Yes \n Irresptive of candle (green/red) \n exit on crossing downward 10 EMA middle line \n';
                      return True;
                  
                  else:
                      buy_stat_df.loc[index_cnt,'Keltner_Channel'] = 'False';
                      return False;

        except Exception as exp:
            print('Caught Exception [  ',exp,'  ]')
              
#----------------------------------------------------------------------------------------------------------------------------------------------------------

class chande_oscilattor(object):
    
    def __init__(self):
        pass;

    #reference code https://github.com/twopirllc/pandas-ta/blob/main/pandas_ta/momentum/cmo.py
    
    def CMO_strategy(self,symbol,data,period,buy_stat_df, index_cnt):
        
        try:
            
            cmo_ob = 25;
            cmo_os =-25;
            cmo = pd_ta.cmo(data['Close'], length=10, talib=False);
           
            if ( cmo.iloc[-1] >= cmo_ob ):                
              
                if ( cmo.iloc[-1] >= cmo_ob  and ( cmo.iloc[-1] < (cmo_ob + 5) ) ):                   
                    print(symbol,' =>  ',' cmo[-1] => ',cmo.iloc[-1]);
                    buy_stat_df.loc[index_cnt,'Chande_MO'] = 'CMO > 25 and < 30 Strong Buy Signal => ' + str(cmo.iloc[-1]);
                else:
                    buy_stat_df.loc[index_cnt,'Chande_MO'] = 'CMO greater than 25 Buy Signal ';
                    
            elif ( cmo.iloc[-1] <= cmo_os ):
                buy_stat_df.loc[index_cnt,'Chande_MO'] = 'CMO less than -25 Sell Signal ';
            else:
                pass;
           
            
        except Exception as exp:
            print('Exception caught [ ',exp,' ]');
            
            
            
#----------------------------------------------------------------------------------------

class Parabolic_SAR(object):
    
    def __init__(self):
        pass;
    
    def psar_trend(self,df,buy_stat_df, index_cnt):
    
        try:
            
            psar= ta.SAR(df['High'], df['Low'], acceleration=0.02, maximum=0.2)
            
            current_psar = psar.iloc[-1];
            
            current_closing = df.iloc[-1]['Close'];
            
            if( current_closing > current_psar):
                buy_stat_df.loc[index_cnt,'PSAR'] = ' Bullish ';
            else:
                buy_stat_df.loc[index_cnt,'PSAR'] = ' Berish ';
                
            
        except Exception as exp:
            print('Caught Exception [ ',exp,']');
            
    def isPSARinUpSwing(self,df):
        try:
            
            psar= ta.SAR(df['High'], df['Low'], acceleration=0.02, maximum=0.2)
            # psar > in last 3 days + obv is upswing
            if( ( (psar.iloc[-1] < df.iloc[-1]['Close']) and  (psar.iloc[-1] > psar.iloc[-2]) ) and 
                ( (psar.iloc[-2] < df.iloc[-2]['Close']) and  (psar.iloc[-2] > psar.iloc[-3]) ) and 
                ( (psar.iloc[-3] < df.iloc[-3]['Close']) and  (psar.iloc[-3] > psar.iloc[-4]) )  
                ):
                return True;
            else:
                return False;


                
            
        except Exception as exp:
            print('Caught Exception [ ',exp,']');
#---------------------------------------------------------------------------------------------

class Adx_Indicator(object):
    
    def __init__(self):
        pass;
    
    
    def ema(self,arr, periods=14, weight=1, init=None):
        leading_na = np.where(~np.isnan(arr))[0][0]
        arr = arr[leading_na:]
        alpha = weight / (periods + (weight-1))
        alpha_rev = 1 - alpha
        n = arr.shape[0]
        pows = alpha_rev**(np.arange(n+1))
        out1 = np.array([])
        if 0 in pows:
            out1 = self.ema(arr[:int(len(arr)/2)], periods)
            arr = arr[int(len(arr)/2) - 1:]
            init = out1[-1]
            n = arr.shape[0]
            pows = alpha_rev**(np.arange(n+1))
        scale_arr = 1/pows[:-1]
        if init:
            offset = init * pows[1:]
        else:
            offset = arr[0]*pows[1:]
        pw0 = alpha*alpha_rev**(n-1)
        mult = arr*pw0*scale_arr
        cumsums = mult.cumsum()
        out = offset + cumsums*scale_arr[::-1]
        out = out[1:] if len(out1) > 0 else out
        out = np.concatenate([out1, out])
        out[:periods] = np.nan
        out = np.concatenate(([np.nan]*leading_na, out))
        return out
    
    
    def atr(self,highs, lows, closes, periods=14, ema_weight=1):
        hi = np.array(highs)
        lo = np.array(lows)
        c = np.array(closes)
        tr = np.vstack([np.abs(hi[1:]-c[:-1]),
                        np.abs(lo[1:]-c[:-1]),
                        (hi-lo)[1:]]).max(axis=0)
        atr = self.ema(tr, periods=periods, weight=ema_weight)
        atr = np.concatenate([[np.nan], atr])
        return atr
    
    
    def adx(self,highs, lows, closes, periods=14):
        highs = np.array(highs)
        lows = np.array(lows)
        closes = np.array(closes)
        up = highs[1:] - highs[:-1]
        down = lows[:-1] - lows[1:]
        up_idx = up > down
        down_idx = down > up
        updm = np.zeros(len(up))
        updm[up_idx] = up[up_idx]
        updm[updm < 0] = 0
        downdm = np.zeros(len(down))
        downdm[down_idx] = down[down_idx]
        downdm[downdm < 0] = 0
        _atr = self.atr(highs, lows, closes, periods)[1:]
        updi = 100 * self.ema(updm, periods) / _atr
        downdi = 100 * self.ema(downdm, periods) / _atr
        zeros = (updi + downdi == 0)
        downdi[zeros] = .0000001
        adx = 100 * np.abs(updi - downdi) / (updi + downdi)
        adx = self.ema(np.concatenate([[np.nan], adx]), periods)
        return adx
    
       

    def ADX_1(self,data: pd.DataFrame, period: int):
   
        
        df = data.copy()
        alpha = 1/period
    
        # TR
        df['H-L'] = df['High'] - df['Low']
        df['H-C'] = np.abs(df['High'] - df['Close'].shift(1))
        df['L-C'] = np.abs(df['Low'] - df['Close'].shift(1))
        df['TR'] = df[['H-L', 'H-C', 'L-C']].max(axis=1)
        del df['H-L'], df['H-C'], df['L-C']
    
        # ATR
        df['ATR'] = df['TR'].ewm(alpha=alpha, adjust=False).mean()
    
        # +-DX
        df['H-pH'] = df['High'] - df['High'].shift(1)
        df['pL-L'] = df['Low'].shift(1) - df['Low']
        df['+DX'] = np.where(
            (df['H-pH'] > df['pL-L']) & (df['H-pH']>0),
            df['H-pH'],
            0.0
        )
        df['-DX'] = np.where(
            (df['H-pH'] < df['pL-L']) & (df['pL-L']>0),
            df['pL-L'],
            0.0
        )
        del df['H-pH'], df['pL-L']
    
        # +- DMI
        df['S+DM'] = df['+DX'].ewm(alpha=alpha, adjust=False).mean()
        df['S-DM'] = df['-DX'].ewm(alpha=alpha, adjust=False).mean()
        df['+DMI'] = (df['S+DM']/df['ATR'])*100
        df['-DMI'] = (df['S-DM']/df['ATR'])*100
        del df['S+DM'], df['S-DM']
    
        # ADX
        df['DX'] = (np.abs(df['+DMI'] - df['-DMI'])/(df['+DMI'] + df['-DMI']))*100
        df['ADX'] = df['DX'].ewm(alpha=alpha, adjust=False).mean()
        del df['DX'], df['ATR'], df['TR'], df['-DX'], df['+DX'], df['+DMI'], df['-DMI']
    
        return df
        

    def isADXinUpSwing(self,df):
        try:
            res = self.ADX_1(df,14);
            
            
            adx_sma  = ta.SMA(res['ADX'],timeperiod = 5);
            
            if( adx_sma.iloc[-2] < res.iloc[-1]['ADX'] and 
               (res.iloc[-1]['ADX'] > res.iloc[-2]['ADX']) and 
               (res.iloc[-1]['ADX'] > res.iloc[-3]['ADX']) ):
                return True;
            else:
                return False;
            
        except Exception as exp:
            print('Caught Exception under [ ',exp,' ]');
            
            
    def Adx_trend(self,df,buy_stat_df, index_cnt):
    
        try:
            
            res = self.ADX_1(df,14);

            buy_stat_df.loc[index_cnt,'ADX'] = res.iloc[-1]['ADX'];
            
        except Exception as exp:
            
            print('Caught Exception [ ',exp,']');
            
            
        
    
    