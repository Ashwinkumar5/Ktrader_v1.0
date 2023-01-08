# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 12:02:32 2022

@author: Ashwin
"""

import pandas as pd;
import numpy as nm;
import sys
sys.path.append("E:\\Pyhton\\KTrader\\Graph")
sys.path.append("E:\\Pyhton\\KTrader\\DB")
import chartDrawing
import math;
import Mongo;
import pandas_ta as ta

PREV_DAY=2;
CURR_DAY=1;
SWING_START_POINT=PREV_DAY + CURR_DAY;
SWING_END_POINT= SWING_START_POINT + PREV_DAY + CURR_DAY;
OPEN='Open';
CLOSE='Close';
HIGH='High';
LOW='Low';
VOLUME='Volume';
GAP_UP_PERCNT=7;
Engulfin="ENGULFIN";
TrustinLine="TRUSTINLINE";
TWEEZER_BOTTOM="TWEEZERBOTTOM";
MAX_PREV_DAYS=6;

'''
  current close 
'''
TWEZZER_BOTTOM_DELTA_DCT={
        1:0.10,
        2:0.20,
        3:1.5,
        4:3.0    
    };

#-------------------------------------------------------------------------------------------------------------
def isHammer(data):

        spread     =  data[CLOSE] -data[OPEN]; 
        lower_weak =  data[OPEN] - data[LOW];
        high_weak  =  data[HIGH] - data[CLOSE];

        if(high_weak > 0 ):
            return False;
        else:
            if(spread * 2 <= lower_weak  ):
                return True;
            else:
                return False;
#------------------------------------------------------------------------------------------------------------

def isShootingStar(data):
        spread = data[CLOSE] -data[OPEN]; 
        lower_weak =  data[OPEN] - data[LOW];
        high_weak  =  data[HIGH] - data[CLOSE];
        
        if( high_weak >= spread * 2 ):
            return True;
        else:
            return False;
#-------------------------------------------------------------------------------------------------------------    
def isGreen(data):
        if(data[OPEN] > data[CLOSE]):
            return False;
        else:
            return True;
#-------------------------------------------------------------------------------------------------------------    
def isRed(data):
        if(data[OPEN] < data[CLOSE]):
            return False;
        else:
            return True;
#-------------------------------------------------------------------------------------------------------------        
def isOpenGapUp(curr_day,prev_day):        
        gapupfactor = (prev_day[OPEN] - prev_day[CLOSE] )*(GAP_UP_PERCNT/100);        
        
        if(curr_day[OPEN] > (gapupfactor + prev_day[CLOSE])):
            return True;
        else:
            return False;
#-------------------------------------------------------------------------------------------------------------        
def isDownSwing(data,Symbol):                       
        low_close=data.iloc[-PREV_DAY][CLOSE];       
        
       
        for days in range(SWING_START_POINT,SWING_END_POINT):
                if( (low_close <= data.iloc[-days] [CLOSE] ) and not (isGreen(data.iloc[-days])) ):                    
                    low_close = data.iloc[-days][CLOSE];
                    
                    continue;
                else:
                    return False;        
        #for days in range(PREV_DAY,SWING_END_POINT):               
         #  print(Symbol," :: ",data.iloc[-days]['Date']," :: ",data.iloc[-days] ['Close']);        
        return True;

#-------------------------------------------------------------------------------------------------------------    
def isUpSwing(data,Symbol):         
              
        high=data.iloc[-PREV_DAY]['Close']; 
        
        for days in range(SWING_START_POINT,SWING_END_POINT):
                if( (high >= data.iloc[-days] [CLOSE] ) and not (isRed(data.iloc[-days])) ):                    
                    high = data.iloc[-days] [CLOSE];
                    continue;
                else:
                    return False;        
        #for days in range(2,6):               
         #  print(Symbol[-1]," :: ",data.iloc[-days]['Date']," :: ",data.iloc[-days] ['Close']);        
        return True;
#-------------------------------------------------------------------------------------------------------------    
def getDecimalFloatToString(num,dec):
        res = round(num,dec);        
        return str(res);
#-------------------------------------------------------------------------------------------------------------






#-------------------------------------------------------------------------------------------------------------
class SWING_STRATEGIES(object):
    
#-------------------------------------------------------------------------------------------------------------
    def __init__(self):
              
        self.grah = chartDrawing.PLOT_CHART();       
        self.stop_loss = "stop_loss = "
        self.day_close = 'Consider day closing .'
        
        self.Symbol = ''
        self.data = pd.DataFrame();
        

# Entry function 

#-------------------------------------------------------------------------------------------------------------
    
    def bullish_swing(self,dataframe,symbol,buy_stat_df,index):
        
        try:
             self.start_process(dataframe,symbol,buy_stat_df,index);
            
        except Exception as exp:
            print ('Exception caught in bullish_swing exp ',exp);
            
#-------------------------------------------------------------------------------------------------------------
    
    def isBullishReversallastdayLeftAway(self,data,symbol,ema3df):
        
        try:
            
            high = data.iloc[-2]['High'];
            low = data.iloc[-2]['Low'];
            close = data.iloc[-2]['Close'];
            open_ = data.iloc[-2]['Open'];

            
            
            if( ( high  > ema3df.iloc[-2] ) and 
                ( low   > ema3df.iloc[-2]) and 
                ( open_ > ema3df.iloc[-2] ) and 
                ( close > ema3df.iloc[-2] ) ):
                #print(symbol,' Bullish reversal high ',high,' low ',low,' close ',close,' open_ ',open_, ' ema3df.iloc[-2] ',ema3df.iloc[-2] );
                
                return True;
            
            else:

                return False;
            
        except Exception as exp:
            print('Exception in isBullishReversallastdayLeftAway ',exp)
  
            
  #-------------------------------------------------------------------------------------------------------------
  
    def isBullishReversalConfirmation(self,data,symbol,ema3df):
        
        #candle open is greater  than close means Red candle
        
        if(data.iloc[-1]['Open'] >  data.iloc[-1]['Close'] ):
        
            if( ( data.iloc[-1]['Low']   <= ema3df.iloc[-1] ) or
                ( data.iloc[-1]['High']  <= ema3df.iloc[-1] ) or
                ( data.iloc[-1]['Open']  <= ema3df.iloc[-1] ) or 
                ( data.iloc[-1]['Close'] <= ema3df.iloc[-1] )  ):
             
                return True;
        
        return False;
    
#-------------------------------------------------------------------------------------------------------------

    def isBerishReversallastdayLeftAway(self,data,symbol,ema3df):
        
        try:
            
            high = data.iloc[-2]['High'];
            low = data.iloc[-2]['Low'];
            close = data.iloc[-2]['Close'];
            open_ = data.iloc[-2]['Open'];
            
            
            
            if( ( high  < ema3df.iloc[-2] ) and 
                ( low   < ema3df.iloc[-2] ) and 
                ( open_ < ema3df.iloc[-2] ) and 
                ( close < ema3df.iloc[-2] ) ):
                #print(symbol,'Berish reversal : ','high =',high,' low = ',low,' open = ',open_,' close = ',close, ' ema3 ',ema3df.iloc[-2] )
                
                return True;
            
            else:
                return False;
            
        except Exception as exp:
            print('Exception in isBerishReversallastdayLeftAway ',exp)
            
    def isBerishReversalConfirmation(self,data,symbol,ema3df):

        #candle opwn is less than close means green candle
        if(data.iloc[-1]['Open'] < data.iloc[-1]['Close'] ):

            if( ( data.iloc[-1]['Close'] >= ema3df.iloc[-1] ) or
                ( data.iloc[-1]['High']  >= ema3df.iloc[-1] ) ):
                return True;

        return False;

#-------------------------------------------------------------------------------------------------------------
    # refine swing withn EMA of 3 and 5 length     

    def refined_Swing(self,common_get,dataframe,symbol,buy_stat_df,index):
       try:

               ema3df = ta.ema(dataframe["Close"], length=3);
               
               ema5df = ta.ema(dataframe["Close"], length=5);
               
              
               
               #ema3df = common_get.getDMA(dataframe,3,'Close');
               #ema5df = common_get.getDMA(dataframe,5,'Close');

               
               if( isDownSwing(dataframe,symbol) ):
                   
                   if( self.isBerishReversallastdayLeftAway(dataframe,symbol,ema3df) ):                                              
                       if(self.isBerishReversalConfirmation(dataframe,symbol,ema3df) ):
                           buy_stat_df.loc[index,'Bullish_Refine_Swing']= 'True';
                           buy_stat_df.loc[index,'Berish_Refine_Swing']= 'False';
                           return;

               elif(isUpSwing(dataframe, symbol)):
                   if( self.isBullishReversallastdayLeftAway(dataframe,symbol,ema3df) ):                       
                       if(self.isBullishReversalConfirmation(dataframe,symbol,ema3df) ):
                           buy_stat_df.loc[index,'Berish_Refine_Swing']= 'True';                           
                           buy_stat_df.loc[index,'Bullish_Refine_Swing']= 'False';
                           return;
               else:
                   buy_stat_df.loc[index,'Bullish_Refine_Swing']= 'False';
                   buy_stat_df.loc[index,'Berish_Refine_Swing']= 'False';
                   
           
           
       except Exception as exp:
           print('Caught exception under refined_Swing ', exp);
           
           
#-------------------------------------------------------------------------------------------------------------

    

#-------------------------------------------------------------------------------------------------------------
        
    def start_process(self,stock_hst_df,symbol,buy_stat_df,index):    
           
           if( isDownSwing(stock_hst_df,symbol) ):
                print (1);
                self.bullishEngulfin(stock_hst_df,symbol,buy_stat_df,index);
                print (2);
                self.bullishTrutingLine(stock_hst_df,symbol,buy_stat_df,index);
                print (3);
                self.bullishTweezerBottom(stock_hst_df,symbol,buy_stat_df,index);
                print (4);

           stock_hst_df.iloc[0:0];
          
#-------------------------------------------------------------------------------------------------------------
          
                    
                        
    def bullishEngulfin(self,stock_hst_df,symbol,buy_stat_df,index):              
        stop_loss ='';
        
        if( isGreen(stock_hst_df.iloc[-CURR_DAY]) and ( not isShootingStar(stock_hst_df.iloc[-CURR_DAY]))):
            if( ( stock_hst_df.iloc[-CURR_DAY][CLOSE] >  stock_hst_df.iloc[-PREV_DAY][OPEN] ) and ( stock_hst_df.iloc[-CURR_DAY][OPEN] <  stock_hst_df.iloc[-PREV_DAY][CLOSE] ) ):
                pattern = 'Bullish bullishEngulfin pattern ... \n Entry on current Close and exit on next 5th candle close \n';
                stop_loss='stop_loss only on days close = ' + str(getDecimalFloatToString(stock_hst_df.iloc[-CURR_DAY][OPEN],2));
                
                if(isHammer ( stock_hst_df.iloc[-CURR_DAY] )):
                    stop_loss = stop_loss + "\n --- HAMMER Candle ---";
                
                self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,Engulfin, pattern + stop_loss );
                buy_stat_df.loc[index,'Engullfin'] =  str(pattern + stop_loss);
               
               
#-------------------------------------------------------------------------------------------------------------
    
    def bullishTrutingLine(self,stock_hst_df,symbol,buy_stat_df,index):
        stop_loss ='';
        if( (isGreen(stock_hst_df.iloc[-CURR_DAY]) ) 
            and ( not isShootingStar(stock_hst_df.iloc[-CURR_DAY]) )
            and (isOpenGapUp(stock_hst_df.iloc[-CURR_DAY],stock_hst_df.iloc[-PREV_DAY])) 
            and ( stock_hst_df.iloc[-CURR_DAY][CLOSE] >  stock_hst_df.iloc[-PREV_DAY][OPEN]) ):
            
            pattern = 'Bullish TrustingLine pattern ... \n Entry on current Close and exit on next 5th candle close \n';            
            stop_loss='stop_loss only on days close = ' + str(getDecimalFloatToString(stock_hst_df.iloc[-PREV_DAY][CLOSE],2));
            
            if(isHammer ( stock_hst_df.iloc[-CURR_DAY] )):
                stop_loss = stop_loss + "\n --- HAMMER Candle ---";
            
            self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,TrustinLine,pattern + stop_loss );
            
            buy_stat_df.loc[index,'Trustinline'] =  str(pattern + stop_loss);
#-------------------------------------------------------------------------------------------------------------

    
    def bullishTweezerBottom(self,stock_hst_df,symbol,buy_stat_df,index):
        pattern = 'Bullish Tweezer bottom pattern ... \n Entry on current Close and exit on next 5th candle close \n';
        stop_loss='stop_loss only on days close = ';    
        
        delta=0;
        
        if(not isShootingStar(stock_hst_df.iloc[-CURR_DAY]) ):
            
            if( ( stock_hst_df.iloc[-CURR_DAY][LOW] == stock_hst_df.iloc[-PREV_DAY][LOW] ) ): 
                print (symbol,' : current low ',stock_hst_df.iloc[-CURR_DAY][LOW]," : prev low ",stock_hst_df.iloc[-PREV_DAY][LOW]," :: Bullish tweezer Bottom pattern ...");                                         
                stop_loss = stop_loss + self.getDecimalFloatToString(stock_hst_df.iloc[-PREV_DAY][LOW],2);
                
                if(self.isHammer ( stock_hst_df.iloc[-CURR_DAY] )):
                    stop_loss = stop_loss + "\n --- HAMMER Candle ---";
                    
                self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,TWEEZER_BOTTOM,pattern + stop_loss);
                buy_stat_df.loc[index,'TweezerBottom'] =  str(pattern + stop_loss);
            else:
                
                if( stock_hst_df.iloc[-CURR_DAY][LOW] < 100 and (stock_hst_df.iloc[-CURR_DAY][LOW] > 50) ):
                     delta = float(TWEZZER_BOTTOM_DELTA_DCT[1]);
                elif( stock_hst_df.iloc[-CURR_DAY][LOW] < 1000 and (stock_hst_df.iloc[-CURR_DAY][LOW] > 100) ):
                    delta = float(TWEZZER_BOTTOM_DELTA_DCT[2]);
                elif( (stock_hst_df.iloc[-CURR_DAY][LOW] < 5000) and ( stock_hst_df.iloc[-CURR_DAY][LOW] > 1000 ) ):
                    delta = float(TWEZZER_BOTTOM_DELTA_DCT[3]);
                elif( (stock_hst_df.iloc[-CURR_DAY][LOW] < 10000) and ( stock_hst_df.iloc[-CURR_DAY][LOW] > 5000 ) ):
                    delta = float(TWEZZER_BOTTOM_DELTA_DCT[4]);
                else:
                    delta=0;
                        
                if(stock_hst_df.iloc[-CURR_DAY][LOW] > stock_hst_df.iloc[-PREV_DAY][LOW]):
                    if( (stock_hst_df.iloc[-CURR_DAY][LOW] - stock_hst_df.iloc[-PREV_DAY][LOW]) <= delta):                    
                        print (symbol,' : current low ',stock_hst_df.iloc[-CURR_DAY][LOW]," : prev low ",stock_hst_df.iloc[-PREV_DAY][LOW],": Delta ",delta, ":: Bullish tweezer Bottom pattern ...");                                                 
                        stop_loss = stop_loss +  self.getDecimalFloatToString(stock_hst_df.iloc[-PREV_DAY][LOW],2);
                        
                        if(self.isHammer ( stock_hst_df.iloc[-CURR_DAY] )):
                            stop_loss = stop_loss + "\n --- HAMMER Candle ---";
                            
                        self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,TWEEZER_BOTTOM,pattern + stop_loss );
                        buy_stat_df.loc[index,'TweezerBottom'] =  pattern + stop_loss;                           
                else:
                     if( (stock_hst_df.iloc[-PREV_DAY][LOW] - stock_hst_df.iloc[-CURR_DAY][LOW]) <= delta):
                        print (symbol,' : current low ',stock_hst_df.iloc[-CURR_DAY][LOW]," : prev low ",stock_hst_df.iloc[-PREV_DAY][LOW],": Delta ",delta," :: Bullish tweezer Bottom pattern ...");                             
                        stop_loss = stop_loss + self.getDecimalFloatToString(stock_hst_df.iloc[-PREV_DAY][LOW],2);
                        
                        if(isHammer ( stock_hst_df.iloc[-CURR_DAY] )):
                            stop_loss = stop_loss + "\n --- HAMMER Candle ---";
                            
                        self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,TWEEZER_BOTTOM,pattern + stop_loss); 
                        buy_stat_df.loc[index,'TweezerBottom'] =  str(pattern + stop_loss);                       
        else:
            pass;
            
#-------------------------------------------------------------------------------------------------------------