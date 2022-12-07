# -*- coding: utf-8 -*-
"""
Created on Wed Nov  9 22:38:06 2022

@author: Ashwin
"""


import pandas as pd;
import matplotlib.pyplot as plot;
import mplfinance as mpl;
import os;
import datetime;
import sys;
sys.path.append("E:\\Pyhton\\KTrader\\Utility");

import common as ut;

Chart_Dir_Path="E:\\Pyhton\\KTrader\\charts";

Chart_Ext=".png";

class PLOT_CHART(object):
    
    def __init__(self):
        self.chart_dir = ut.getChartDirPath() +"\\"+ut.getCurrectDateString();
        pass;
        
        
    def getCurrent_chart_name(self,folder,symbol):        
        return self.chart_dir +"\\" + folder +"\\"+symbol+Chart_Ext;
   # self.grah.drawCandleChart(stock_hst_df.tail(MAX_PREV_DAYS),symbol,Engulfin, pattern + stop_loss );
    def drawCandleChart(self,data,symbol,folder,pattern):
        
        ut.createDirectory(self.chart_dir,folder);
        
        customstyle = mpl.make_mpf_style(base_mpf_style='yahoo',
                                         y_on_right=True,
                                         facecolor='w');
                
        title_=symbol + ': \n'+pattern;                
        #data.iloc[:,0]=pd.to_datetime(data.iloc[:,0],format = '%Y-%m-%d');
        data=data.set_index(pd.DatetimeIndex(data['Date'])); 
        chart = self.getCurrent_chart_name(folder,symbol);
               
        
        mpl.plot(data,
                 style=customstyle,
                 type='candle',
                 title = title_,
                 figratio=(12.00, 5.75),
                 returnfig=True,
                 show_nontrading=False,
                 volume=False, 
                 tight_layout=True,                   
                 savefig=chart);
        
    def drawLineChart(self,data,symbol,folder,pattern):
    
          ut.createDirectory(self.chart_dir,folder);
          print (1);
          customstyle = mpl.make_mpf_style(base_mpf_style='yahoo',
                                            y_on_right=True,
                                            facecolor='w');
          print (2);         
          title_=symbol + ': \n'+pattern;                
          chart = self.getCurrent_chart_name(folder,symbol);
          
          # print (3);
          # data.iloc[:,0]=pd.to_datetime(data.iloc[:,0],format = '%Y-%m-%d');          
          # data=data.set_index(pd.DatetimeIndex(data['Date'])); 
          # print (4);
          # chart = self.getCurrent_chart_name(folder,symbol);
          # print (chart);  
          # mpl.plot(data,
          #           style=customstyle,
          #           type='Line',
          #           title = title_,
          #           figratio=(12.00, 5.75),
          #           returnfig=True,
          #           show_nontrading=False,
          #           volume=False, 
          #           tight_layout=True,                   
          #           savefig=chart);
          
          date=data['Date'];
          price=data['Price'];
          
          plot.xlabel("X-axis")  # add X-axis label
          plot.ylabel("Y-axis")  # add Y-axis label
          plot.title(title_)  # add title
          plot.plot(date,price);
          
          plot.show()
          plot.savefig(chart);