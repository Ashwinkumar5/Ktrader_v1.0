# -*- coding: utf-8 -*-
"""
Created on Sun Nov 20 12:19:26 2022

@author: Ashwin
"""
import pandas as pd
import ta as tech
import io
import sys
import os
import math


#---------------------------------------------------------------------
class VOLUME_PROFILE(object):
    
    # constructor 
    def __init__(self):
        self.period_profilling = 14 + 1 

    
    def Bullish_volumeProfilling(self,data,symbol,buy_stat_df, index,draw_instance):
          
           period = self.period_profilling;# this value can get tune

           
           if( self.isVolumeConversionWithPrice(data,period) ):
                #buy_stat_df.loc[index,'VPConvergence']= symbol + '  volume and price moving in convergent ';
                buy_stat_df.loc[index,'VPConvergence']= 'Yes';
             
           elif(self.isVolumeDiversionWithPrice(data,period)):
                #buy_stat_df.loc[index,'VPDivergence'] = symbol + ' volume and price moving in divergent '; 
                buy_stat_df.loc[index,'VPDivergence'] = 'Yes'
           else:
                pass;
             
           if(self.isSellingClimax_multiple_candle(data, period)):
               #buy_stat_df.loc[index,'SellingClimax'] = symbol + ' Selling climax';
               buy_stat_df.loc[index,'SellingClimax'] = 'Yes';
           elif( self.sellling_Climax_with_singleBigCandle(data, period)):
               buy_stat_df.loc[index,'SellingClimax'] = 'Yes';
           else:
                 pass;
              
           resistance = self.isPickVolumeBreakout(data);           
           if(resistance > 0 ):
              buy_stat_df.loc[index,'PickVolumeBreakout'] ='Today wait for breakout of resistance => ' + str(resistance) ;
           else:
               resistance = self.isPickVolumeBreakoutinPeriod(data);
               if(resistance > 0 ):
                   buy_stat_df.loc[index,'PickVolumeBreakout'] ='Before wait for breakout of resistance => ' + str(resistance) ;
              
#---------------------------------------------------------------------
        

    # Validate is Volume and price moving in same direction 
    # In upward 
    # In downward direction 
#---------------------------------------------------------------------
    def isVolumeConversionWithPrice(self, data, period):
        try:
            
            # get copy ofdata except current day

            df = data[0:-1];

            current_volume = int(data.iloc[-1]['Volume']); 
            current_close  = int(data.iloc[-1]['Close']);
            
            sma_df_close= self.getSMAMovingAverage(df, period, 'Close');
            sma_df_volume= self.getSMAMovingAverage(df, period, 'Volume');
            
            sma_close = int(sma_df_close.iloc[-1]['Close']);
            sma_volume = int(sma_df_volume.iloc[-1]['Volume']);
            
            if ((sma_close < current_close) and (sma_volume < current_volume)):
                return True
            else:
                return False

        except Exception as exp:
            print('Exception caught in isVolumeConversionWithPrice', exp)
            return False
#---------------------------------------------------------------------

    def isVolumeDiversionWithPrice(self, data, period):
        try:
            # get copy ofdata except current day
            
            df = data[0:-1];
            
            current_volume = int(data.iloc[-1]['Volume']); 
            current_close  = int(data.iloc[-1]['Close']);
            
            sma_df_close= self.getSMAMovingAverage(df, period, 'Close');
            sma_df_volume= self.getSMAMovingAverage(df, period, 'Volume');
            
            sma_close = int(sma_df_close.iloc[-1]['Close']);
            sma_volume = int(sma_df_volume.iloc[-1]['Volume']);
            
            if ( ( (sma_close > current_close) and
                 (sma_volume < current_volume) ) or
                 ( (sma_close < current_close) and
                 (  sma_volume > current_volume ) ) ):
                return True
            else:
                return False
        except Exception as exp:
            print('Exception caught in isVolumeDiversionWithPrice', exp)
            return False
#---------------------------------------------------------------------

    def isSellingClimax_multiple_candle(self, data, period):

        #Capture current [ low ,high ,open ,close ]
        
        current_vol   = data.iloc[-1]['Volume']
        current_close = data.iloc[-1]['Close']
        current_open  = data.iloc[-1]['Open'];
        current_high  = data.iloc[-1]['High'];
        current_Low   = data.iloc[-1]['Low'];
        
        
        max_period_vol = data[-period:-1]['Volume'].max()
        max_period_close = data[-period:-1]['Close'].max()

        if( ((current_close < max_period_close) and
            (current_vol   > max_period_vol) and
            (current_open  > current_close ) )and (
            
            (data.iloc[-4]['High'] > data.iloc[-3]['High']) and
            (data.iloc[-4]['Low'] > data.iloc[-3]['Low']) and
            (data.iloc[-3]['High'] > data.iloc[-2]['High']) and
            (data.iloc[-3]['Low'] > data.iloc[-2]['Low']) and
            (data.iloc[-2]['High'] > data.iloc[-1]['High']) and
            (data.iloc[-2]['Low'] > data.iloc[-1]['Low']) 
                
            ) and  self.isVolumeDiversionWithPrice(data,period)):
            
            return True
        else:
            return False
        #msg=str("Stock is alert with Sellign climax indicator if selling climax confirm then buy on next candle close \n stop loss : low of selling climax candle");

    def sellling_Climax_with_singleBigCandle(self,data, period):
        
        try:
            period = 21 + 1;
            Multiply_Factor = 5;
            
            sma_df_volume =  self.getSMAMovingAverage(data, period, 'Volume');
            last_max_in_21_days_volume = sma_df_volume.iloc[-1]['Volume']
                        
            if( ( data.iloc[-1]['Close'] < data.iloc[-1]['Open'])  and
                (last_max_in_21_days_volume * Multiply_Factor <= data.iloc[-1]['Volume']) ):
                return True;
            else:
                return False;
                        
        except Exception as exp:
            print('Caught Exception  sellling_Climax_with_singleBigCandle .. [ ',exp,']');
            
                

#---------------------------------------------------------------------
    # if price is in uptrend
    # check the closing of PickVOlume candle Close as Ristance breakout everyday
    # Stop Loss RESISTANCE BREAKOUT CANDLE Low

    def isPickVolumeBreakout(self, data):

        period = 10 + 1 ;
        
        Multiply_Factor = 6 ;
        
            # price and volume are in up direction
        if( self.isPriceMovingUp(data,period) and  self.isVolumeMovingUp(data,period)):
            
            current_vol = data.iloc[-1]['Volume']
            current_close = data.iloc[-1]['Close']
            
            df = data[-(period):-1];
            
            # average volume of last 10 days             
            avg_vol = sum(list(df['Volume']))/len(df['Volume']);
            
            # current volume check is greater than 6 times average volume of last 10 days 
            if( (data.iloc[-1]['Open'] < data.iloc[-1]['Close']) and
                (current_vol >= Multiply_Factor * avg_vol) ):
                 return current_close;
            else:
                 return 0;
        else:
            return 0;
        
#-----------------------------------------------------------------------------------------------------------------------
        
    def isPickVolumeBreakoutinPeriod(self, data):
        try:
            period = 42;        
            Multiply_Factor = 6 ;        
            
            max_vol = data[-(period):-1]['Volume'].max();
            
            sma_vol_df = self.getSMAMovingAverage(data, period, 'Volume');
            
            sma_vol = sma_vol_df.iloc[-1]['Volume'];
            
            index =  data.Volume[data.Volume == max_vol].index;
            
            max_vol_open  = int(data.iloc[index]['Open']);
            max_vol_close = int(data.iloc[index]['Close']);
            
            current_close  = data.iloc[-1]['Close'];
            
            if ( (max_vol_open < max_vol_close ) and
                 (current_close < max_vol_close) and
                 (max_vol >= sma_vol * Multiply_Factor) ):            
                return max_vol_close;
            else:
                return 0;
            
        except Exception as exp:
            print ('Caught exception [ ',exp, ' ]');

#-----------------------------------------------------------------------------------------------------------------------


    def isPriceMovingUp(self, data, period):
        
        df= data[0:-1];
     
        dd = self.getSMAMovingAverage(df, period,'Close');  
        
        if(data.iloc[-1]['Close'] > dd.iloc[-1]['Close']):
            return True
        else:
            return False
#---------------------------------------------------------------------
    def isVolumeMovingUp(self, data, period):        

        df = data[0:-1];
        
        df = self.getSMAMovingAverage(df, period,'Volume');
        
        if(data.iloc[-1]['Volume'] > df.iloc[-1]['Volume']):
            return True
        else:
            return False
#---------------------------------------------------------------------
    def getSMAMovingAverage(self, data, period, col):
        df = pd.DataFrame().reset_index();
        df.columns= [col];
        
        df[col] = data[col].rolling(period).mean();        
        
        df.dropna(inplace=True)
        return df;
#---------------------------------------------------------------------
    def getEMAMovingAverage(self, data, period, col):
        df = pd.DataFrame().reset_index();
        df.columns= [col];
        
        df[col] = data[col].ewm(span=period).mean()
        
        df.dropna(inplace=True)
        return df;
#---------------------------------------------------------------------
    def getCMAMovingAverage(self, data, period, col):     
        df = pd.DataFrame().reset_index();
        df.columns= [col];
        
        df[col] =data[col].expanding(period).mean()
        
        df.dropna(inplace=True)
        return df
#---------------------------------------------------------------------
