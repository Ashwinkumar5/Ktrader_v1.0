# -*- coding: utf-8 -*-
"""
Created on Sat Nov 12 09:18:15 2022

@author: Ashwin
"""

import pandas as pd;
import numpy as nm;
import sys
sys.path.append("E:\\Pyhton\\KTrader\\Graph")
sys.path.append("E:\\Pyhton\\KTrader\\Utility")
sys.path.append("E:\\Pyhton\\KTrader\\Downloader")

import refDataDownloader;
import chartDrawing;
import math;
import const;
import datetime;
import operator;


class TREND(object):
    def __init__(self,symbollist):
        
        self.higherBottom=[];
        self.lowerPicks=[];
        self.sideWays=[];
        self.draw = chartDrawing.PLOT_CHART();
        self.resistance_perc = 10;
        self.current_resistance_days_diff=14;  
        self.symbolist = symbollist;
        self.download = refDataDownloader.Download_Prices_YFinance(symbollist);


    # Is price shown momentum in a week for continuos 3 days 
    # Check for short movementum is price is moving up for three days continuosly
    def checkForShortMovementum(self,sym,interval,period='1wk'):
        #print('checkForShortMovementum ...');
        #print("4.6");
        data = self.download.downloadHistorialPeriodforsymbol(sym, period, interval);
        rev_data=data.iloc[::-1];
        length = len(rev_data);
        days=0;
        #print("4.7");
        for item in range(1,length-1):            
            if( rev_data.iloc[item - 1]['Close'] >  rev_data.iloc[item ]['Close']): 
                #print(sym,": => ",item,' --> ',rev_data.iloc[item - 1]['Close'],">",rev_data.iloc[item ]['Close'],' =::= ',rev_data.iloc[item - 1]['Volume'],">", rev_data.iloc[item ]['Volume'] );
                days = days + 1;
        
        if(days >= 3):
            return True;
        
        return False;

    '''
        resistance close and current close price diff near by 10% and 1 week of diff
    '''    
    def isPriceConsiderforBreakout(self,sym,curr_closing,resistace,days_diff):
        #print("4.5");
        #if ( ( (resistace - curr_closing ) >= (curr_closing *  self.resistance_perc )/100 )and days_diff >= self.current_resistance_days_diff ): 
         #   print('4.5.1');
        if( self.checkForShortMovementum(sym,'1d') ):
            #print("4.5.3");
            return True;
        else:
            return False;
        return False;
    
    # Check for sotck latest breakout .....
    
    def getStockLatestBreakout(self,sym,break_date,break_price,period='4wk',interval='1d'):        
        try:
            df_org = self.download.downloadHistorialPeriodforsymbol(sym, period, interval);
            df_rev = df_org.iloc[::-1];
            length = len(df_rev);            
            index  = df_rev.index;
            for item in range(0,length-1):
             
                if(break_price >= int(df_rev.iloc[item]['Close'] ) ):
                    print('BreakOut date ==> ',index[item].date(),' :: => ',break_date,' Breakout Duration => ', ( index[item].date() - break_date).days );                   
                    break;
                
        except Exception as err:
            print ('getStockLatestBreakout = > Exception caught ',err);
            
    def isConsolidation(self,sym,period='3wk',interval='1d',percent=3):

        threshold = 1 - (percent/100);
        
        df= self.download.downloadHistorialPeriodforsymbol(sym, period, interval);
        max_close= df['Close'].max();
        min_close= df['Close'].min();        
        if( min_close >= (max_close * threshold ) ):
           print (min_close ," : ",max_close,": ",(max_close * threshold ));
           return True;
        else:
            return False;
              
    
    def checkBreakout(self):
        try:
            
            print (sys._getframe().f_code.co_name);
            
            resistance={};          
            weekly_picks=[];
            weekly_dates=[];
            pick_price={};
            pre_resi_break_counter=0;
            #print (1);            
            
            for sym in self.symbolist:                  
                              
                datadict =  self.download.downloadHistorialPeriodforsymbol(sym,'5y','1wk');
                #print(2);
                
                #print (datadict);
               
                year_weekwise=datadict;
                index = year_weekwise.index;
                length = len(year_weekwise);
                
                current_close = int(year_weekwise.iloc[-1]['Close']);
                current_date  = (index[length-1]).date();
                
                #print(3,":",length);
                '''
                   Collection all Picks from last 10 years as resistance to compare with
                   current closing price
                '''
                pick_price[index[0].date()] = 0;
                for rec in range (1,length-1):
                    if(( year_weekwise.iloc[rec-1]['Close'] < year_weekwise.iloc[rec ]['Close'] )
                             and ( year_weekwise.iloc[rec]['Close'] > year_weekwise.iloc[rec + 1]['Close']) ):                    
                    
                        pick_price[index[rec].date()]= int(year_weekwise.iloc[rec]['Close']);
                
                # Arrange pick prices in descending order 
                resistance = sorted(pick_price.items(), key=operator.itemgetter(1), reverse=True);
                
                resistance_dictionary={};
                #print ('------------------------------------------------------');
                #print(4,":",length);
                for date,price in resistance:
                    if(current_close < price):
                        #print (date," resistance:::",price," current_closing :: ",current_close);
                        resistance_dictionary[date]=price;
                    else:
                        
                        #print(4.1,":",length);
                        if(len(resistance_dictionary) > 0):
                            resp= self.isPriceConsiderforBreakout(
                                                                sym,
                                                                current_close,
                                                                resistance_dictionary[list(resistance_dictionary)[-1]], 
                                                                (current_date - list(resistance_dictionary)[-1]).days
                                            );
                            #print(4.2,":",length);
                            if(resp):                                
                               print(sym ,"::Elligible for brakout watch ");
                               print (date," resistance:::",price," current_closing :: ",current_close);
                               self.getStockLatestBreakout(sym,date,price);
                               
                               #print (sym,'  Latest BREAKOUT for the stock current price =>',current_close,' :: BREAKOUT resistance ', price ,'Durarion of BREKOUT => between [ ',current_date,' -- ',date,' ] Days = > ',(current_date - date).days );
                               
                               #resistance_dictionary[current_date] = current_close;
                               #df=pd.DataFrame(list(resistance_dictionary.items()), columns=['Date', 'Price']);
                               #self.draw.drawLineChart(df, sym, "Breakout", "Bullish Breckout check ..");
                        else:
                             #print(sym,' Stock is in uncharted territory .. ');
                             pass
                        
                        #print (sym,'  Latest BREAKOUT for the stock current price =>',current_close,' :: BREAKOUT resistance ', price ,'Durarion of BREKOUT => ',(current_date - date).days );
                        
                        break;
                    
                    
                        
                        
                #print ('------------------------------------------------------');
                #input();
                #self.draw.drawLineChart(price_dict, symbol, folder, pattern)
                resistance.clear();
                resistance_dictionary.clear();
                pick_price.clear();
                
                #input();
                
        except Exception as err:
            print('Received exception ..',err);
        finally:
            pass;
