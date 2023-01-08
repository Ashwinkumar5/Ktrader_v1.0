# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 14:42:52 2023

@author: Ashwin
"""

import os;

class MONEY_MGMT(object):
    
    
    def __init__(self):
        pass;
        
    
    def getStockQntityandRpt(self,Risk_Capital,stock_open,stock_close):
        
        standard_devisoir =65;
        
        Delta_SL = (stock_open - stock_close) if (stock_close < stock_open) else (stock_close -  stock_open );
        
        Risk_per_trade =  Risk_Capital / standard_devisoir ;
        
        trade_qntity = int(Risk_per_trade / Delta_SL);
        print (' Some important points \n 1) MAX 2 trades are allowed at a time \n 2) MAX Trade can carry is 5 \n 3) with this max loss RPT * 5');
        return trade_qntity,Risk_per_trade;

