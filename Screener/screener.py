# -*- coding: utf-8 -*-
"""
Created on Sat Jan  7 21:04:13 2023

@author: Ashwin
"""
import os;
import pandas
import numpy as np

#To Do
#https://tradingstrategyguides.com/holy-grail-trading-strategy/

class Screen_Trade(object):
    
    
    def __init__(self):
        pass;
        
    # PSAR = Bullish + ADX > 23 Momentum 
    def swing_screener_1(self,swing_df):

        if( ( swing_df['PSAR'].strip()  == "Berish" ) or 
            ( float(swing_df['ADX']) < 23.00 ) ):
            return False;
        
        else:
            return True;
    
    
    # PSAR = Bullish
    # ADX  > 23
    # obv_adx_converse = Yes
    # obv_Psar_converse =  Yes
    
    def swing_screener_2(self,swing_df):

        if( ( swing_df['PSAR'].strip()  == "Bullish" ) and 
            ( float(swing_df['ADX']) < 23.00 ) and 
            ( swing_df['obv_adx_converse'].strip()  == "Yes") and
            ( swing_df['obv_Psar_converse'].strip()  == "Yes") ):
            return True;
        
        else:
            return False;
        
        
        
    '''
        Stock 
        1) RsiOutperformNifty  Yes 
        2) ConsolNarrowRange   No ( no narrow consolidation)
        3) VPConvergence       Yes
        4) VPDivergence        No
        5) ObvBreakDown        No
        6) obv_adx_converse    Yes
        7) PSAR                Bullish
        8) ADX              >  23
   '''
    def momentum_screener_1(self,momentum_df):
        
        
        if(
            (  momentum_df['RsiOutperformNifty'].strip()  == "Yes"  ) and
            (  momentum_df['ConsolNarrowRange'].strip()   != "Yes"  ) and 
            (  momentum_df['VPConvergence'].strip()       == "Yes"  ) and 
            (  momentum_df['VPDivergence'].strip()        != "Yes"  ) and                         
            (  momentum_df['obv_adx_converse'].strip()    == "Yes"  ) and 
            (
                (  momentum_df['PSAR'].strip()             == "Bullish" ) or
                (  float(momentum_df['ADX'])                  > 23.00      )
            )
               
         ):
            return True;
        
        else:
            
            return False;
        
    
    '''
        Stock 
        1) RsiOutperformNifty  Yes 
        2) ConsolNarrowRange   No ( no narrow consolidation)
        3) VPConvergence       Yes
        4) VPDivergence        No
        5) ObvBreakDown        No        
        6) PSAR                Bullish
        7) ADX              >  23
   '''
        
    def momentum_screener_2(self,momentum_df):
        
        if(
            (  momentum_df['RsiOutperformNifty'].strip()  == "Yes"  ) and
            (  momentum_df['ConsolNarrowRange'].strip()   != "Yes"  ) and 
            (  momentum_df['VPConvergence'].strip()       == "Yes"  ) and 
            (  momentum_df['VPDivergence'].strip()        != "Yes"  ) and             
            (
                (  momentum_df['PSAR'].strip()             == "Bullish" ) or
                (  float(momentum_df['ADX'])                  > 23.00      )
            )
               
         ):
            return True;
        
        else:
            
            return False;
    
    '''
        Stock 
        1) RsiOutperformNifty  Yes 
        2) ConsolNarrowRange   No ( no narrow consolidation)
        3) VPConvergence       Yes
        4) VPDivergence        No
        5) ObvBreakDown        No
        6) obv_Psar_converse    Yes
        
   '''
    def momentum_screener_3(self,momentum_df):
        
        if(
            ( (  momentum_df['BreakoutNarrowRange'].strip() == "Yes"  ) or
              (  momentum_df['BreakoutWideRange'].strip()   == "Yes"  ) ) and 
              (  momentum_df['VPConvergence'].strip()       == "Yes"  ) and 
              (  momentum_df['VPDivergence'].strip()        != "Yes"  ) and             
              (  momentum_df['obv_Psar_converse'].strip()    == "Yes" )              
               
         ):
            return True;
        else:
            return False;
        
        
        # PickVolume Breakout  = Today
        # PSAR  =  Bullish
        # OBV PSAR / OBV ADC Convergence = Y
        
    def Volume_Price_Momentum_screener_1(self,volume_df):
        if (    ("Today" in volume_df['PickVolumeBreakout'].strip())  and 
                 ( volume_df['PSAR'].strip()             == "Bullish" ) and
                 (
                     ( volume_df['obv_Psar_converse'].strip() == "Yes" ) or 
                     ( volume_df['obv_adx_converse'].strip()  == "Yes" )
                 )
            ):
            return True;
            
        else:
            return False;
        
        # OBV Breakout > 0
        # PSAR  =  Bullish
        # OBV PSAR / OBV ADC Convergence = Y
        
    def Volume_Price_Momentum_screener_2(self,volume_df):
       
        
        if (    (volume_df['ObvBreakout'] > 0.0 )  and 
                 ( volume_df['PSAR'].strip()             == "Bullish" ) and
                 (
                     ( volume_df['obv_Psar_converse'].strip() == "Yes" ) or 
                     ( volume_df['obv_adx_converse'].strip()  == "Yes" )
                 )
            ):
            return True;
            
        else:
            return False;
        
        
        
    
     
    
