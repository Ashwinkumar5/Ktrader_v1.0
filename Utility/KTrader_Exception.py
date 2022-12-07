# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 21:01:07 2022

@author: Ashwin
"""

import os;
import sys;

class KTraderException(Exception):
    def __init__(self):
        pass;
        

class KTrader_FileNotFound(KTraderException):
    def __init__(self,exception_msg):
        print (exception_msg);