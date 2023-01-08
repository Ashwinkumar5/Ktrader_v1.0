# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 20:58:57 2022

@author: Ashwin
"""

import os;
import KTrader_Exception;
import datetime;
import shutil;
import pandas as pd
chart_dir = "E:\\Pyhton\\KTrader\\charts";
zipfile='';


def isFile(filepath):
    try:
        if(filepath !=""):
            if(os.path.exists(filepath)):
                return True;
            else:
                raise KTrader_Exception.KTrader_FileNotFound("File not found ",filepath);
    except:
            return False;
    else:
        return False;
    
def isDir(dirPath):
    try:
        if(dirPath != ""):
            if(os.path.isdir(dirPath)):
                return True;
            else:
                raise KTrader_Exception.KTrader_FileNotFound("Dir not found ",dirPath);
    except:
        return False;
    

def getCurrectDateString():
   date = datetime.datetime.utcnow().strftime("%a_%b_%d_%Y")   
   return str(date);
 
def checkDirectory(dir_path):   
    try:     
        print('in check dirctory')
        if( not os.path.exists(dir_path) ):
            os.mkdir( dir_path);           
        else:           
           shutil.rmtree(dir_path,ignore_errors=True);
    except OSError as err:
        print (err);
        return False;
        
                
def createDirectory(dir_path,foldername):
    folder=dir_path + "\\" + foldername;
    
    if( not os.path.exists(dir_path) ):
        os.makedirs(dir_path);
    
    if( not os.path.exists(folder) ):
            os.makedirs(folder);
            
def getChartDirPath():
    return chart_dir;

def zip(path,folder):        
    
    try:
         if ( path == "" ):
             return False;
         
         chartfolder=path + "\\" + folder;
         zipfile=chartfolder;
         
         if(isFile(zipfile + ".zip")):
             os.remove(zipfile);
             
         if( isDir(chartfolder) ):    
             shutil.make_archive(zipfile,'zip',chartfolder);
             
    except Exception as exp:
        print("Excpetion while zip creation ",exp);
        return False;

def getZip():
    return getChartDirPath() +"\\" + getCurrectDateString() + '.zip';




#------------------------------------------------------------------------------
def getSMA(data,period,column):
    
    df = pd.DataFrame().reset_index();
    df.columns= [column];
    
    df[column] = data[column].rolling(period).mean();        
    
    df.dropna(inplace=True)
    
    return df;

#------------------------------------------------------------------------------
def getDMA(data,period,column):
    
    df = pd.DataFrame().reset_index();
    df.columns= [column];
    
    df[column] = data[column].ewm(span=period).mean();
    
    df.dropna(inplace=True)

    return df;
