# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 21:51:04 2022

@author: Ashwin
"""

import pandas as pd;
import pymongo;
import sys;
import datetime;
class Mongo_db(object):
    
    def __init__(self):
        self.local_uri = "mongodb://localhost:27017/";
        self.client=None;
        self.MAX_RETRY = 5;
        pass;

    def get_mongoclient(self):
        if(self.client!=None):
            return self.client;
        else:
            return None;
    
        
#------------------------------------------------------------------------------------------------    
        
    def connect(self):
        print (' start :: ',sys._getframe().f_code.co_name);
        retry=0;
        while(retry < self.MAX_RETRY):
            
            try:
                self.client = pymongo.MongoClient(self.local_uri);
                version = self.client.server_info()['version'];                
                print('mongo connection successfull version connected [ ',version,' ]');
                break;
            except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while connecting to Mongo DB ',exp, '  : retry count => ',retry);
            retry = retry + 1;
        
        print (' end :: ',sys._getframe().f_code.co_name);
        
#------------------------------------------------------------------------------------------------    
        

    def disconnect(self):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None):
                self.client.close();
                print('mongo Disconnect successfull ');
        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
                print (' end :: ',sys._getframe().f_code.co_name);
        
#------------------------------------------------------------------------------------------------    
        
                
    def isDBExist(self,db_name):
        print (' start :: ',sys._getframe().f_code.co_name);
        if(self.client != None and db_name != "" ):
            db_list = self.client.list_database_names();            
            if db_name in db_list:
                print(db_name, ' Exist ');
                return True;
            else:
                print(db_name, ' Not Exist ');
                return False;
        print (' end :: ',sys._getframe().f_code.co_name);
        
#------------------------------------------------------------------------------------------------    
        
            
    def isCollectionExist(self,db_name,collection):
       print (' start :: ',sys._getframe().f_code.co_name);
       if(self.client != None and db_name != "" and collection != ""):
           db_handle = self.client[db_name];
           coll_list = db_handle.list_collection_names();
           
           if collection in coll_list:
               print(collection, ' Exist ');
               return True;
           else:
               print(collection, ' Not Exist ');
               return False;
       print (' end :: ',sys._getframe().f_code.co_name); 
       
       
#------------------------------------------------------------------------------------------------    
       
       
    def createDB(self,db_name):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" ):
                my_db = self.client[db_name];
                if( my_db.client != None):
                    print('DB Created successfully  .. ',db_name);
                    my_db.dummy.insert_one({});
                else:
                    print('DB Creation failed   .. ',db_name);
                    
            else:
                print('Invalid Mongo connection or db_name..[',db_name,']');
        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
            print (' end :: ',sys._getframe().f_code.co_name);
        
    
#------------------------------------------------------------------------------------------------    
    
    
    def dropDB(self,db_name):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" ):
                if (self.isDBExist(db_name)):
                    my_db = self.client[db_name];                
                    if( my_db.client != None):
                        collection_list = my_db.list_collection_names();
                        for coll_name in collection_list:
                            my_db.drop_collection(coll_name);
                        self.client.drop_database(db_name);
                        
                        if (self.isDBExist(db_name)):
                            print('DB drop unsuccesfull ..', db_name);
                        else:
                            print('DB drop succesfull ..', db_name);
                else:
                    print('Mongo DB not exist to drop ...',db_name);
                    
            else:
                print('Invalid Mongo connection or db_name..[',db_name,']');
        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
                print (' end :: ',sys._getframe().f_code.co_name);
            
#------------------------------------------------------------------------------------------------    
            
            
            
    def createCollection(self,db_name,collection,data_dict):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if(self.isCollectionExist(db_name,collection) == False ):
                    my_db = self.client[db_name];
                    coll = my_db[collection];                    
                    coll.insert_many(data_dict.to_dict('records'));
                    
                    print(collection,'  Collection created ')

        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
                print (' end :: ',sys._getframe().f_code.co_name);
                
#------------------------------------------------------------------------------------------------    
                
    
    def createCollectionwithindex(self,db_name,collection,data_dict):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if(self.isCollectionExist(db_name,collection) == False ):
                    my_db = self.client[db_name];
                    coll = my_db[collection];                    
                    coll.insert_many(data_dict.to_dict('records'));
                    coll.create_index(
                                [("Date", pymongo.DESCENDING)],
                                 unique=True
                                 )
                    print(collection,'  Collection created ')

        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
            print (' end :: ',sys._getframe().f_code.co_name);
            
            
#------------------------------------------------------------------------------------------------    
            

    def dropCollection(self,db_name , collection):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db = self.client[db_name];                
                    if( my_db.client != None):
                        my_db.drop_collection(collection);
                                                
                        if (self.isCollectionExist(db_name,collection)):
                            print('Collection drop unsuccesfull ..db ',db_name,' collection ', collection);
                        else:
                            print('Collection drop succesfull ..db ',db_name,' collection ', collection);
                else:
                    print('Mongo collection not exist to drop ...db ',db_name,' collection ', collection);
                    
            else:
                print('Invalid Mongo connection or db_name..[',db_name,'] collection[',collection,']');
        except pymongo.errors.ServerSelectionTimeoutError as exp:
                print('Exception caught while Disconnect to Mongo',exp);
        finally:
            print (' end :: ',sys._getframe().f_code.co_name);
            
            
#-------------------------------------------------------------------------------------------------

    def getCollectionListforDB(self,db_name):
        try:
            if(self.client != None and db_name != "" ):
                my_db = self.client[db_name];
                lst =  my_db.list_collection_names();
                return lst;
        except Exception as exp:
            print('Caught exception ',exp);
            


#-------------------------------------------------------------------------------------------------
    
    def addRecordToCollection(self,db_name,collection,data):
        print (' start :: ',sys._getframe().f_code.co_name);
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db = self.client[db_name];
                    coll = my_db[collection];
                    coll.insert_many(data.to_dict('records'))
        
        except pymongo.errors.DuplicateKeyError:
                print('duplicate key error ....')
                pass
        
        finally:
                print (' end :: ',sys._getframe().f_code.co_name);
                
                
                
    def removeIDcolumn(self,dataframe_):
        if '_id' in dataframe_.columns:
            dataframe_ = dataframe_.drop('_id', axis=1);
            return dataframe_;
    def removeRecordFromCollection(self):
        
        pass
    
    def UpdateRecordToCollection(self):
        
        pass

    def copyCollection(self):
        
        pass
    
#------------------------------------------------------------------------------------------------    
    
    def queryBetweenDates(self,db_name,collection,start_date,end_date,period):
        print (' start :: ',sys._getframe().f_code.co_name);
       
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db_handler = self.client[db_name];       
                    coll_handler = my_db_handler[collection];
                    cursor = coll_handler.find();
                    data_list = list(cursor);
                    df = pd.DataFrame(data_list);
                    # df['Date'] = pd.to_datetime(df.index);
                    
                    return df[-period:];   
                    
                    
                
        except Exception as exp:
            print ('Caught exception  ',exp);
            
        print (' end :: ',sys._getframe().f_code.co_name);
#------------------------------------------------------------------------------------------------    

    def getCollectionforPeriod(self,db_name,collection,period):
         print (' start :: ',sys._getframe().f_code.co_name);
    
         try:
             if(self.client != None and db_name != "" and collection != "" ):
                 if (self.isCollectionExist(db_name,collection)):
                     my_db_handler = self.client[db_name];       
                     coll_handler = my_db_handler[collection];
                     cursor = coll_handler.find();
                     data_list = list(cursor);
                     df = pd.DataFrame(data_list);
                     #df['Date'] = pd.to_datetime(df.index);
                     df = self.removeIDcolumn(df);

                 return df[-period:];   
             
         except Exception as exp:
            print ('Caught exception  ',exp);
         
         print (' end :: ',sys._getframe().f_code.co_name);

#------------------------------------------------------------------------------------------------    
    
    def getCollectionrecords(self,db_name,collection):

        print (' start :: ',sys._getframe().f_code.co_name);
       
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db_handler = self.client[db_name];       
                    coll_handler = my_db_handler[collection];
                    cursor = coll_handler.find();
                    data_list = list(cursor);
                    df = pd.DataFrame(data_list);
                    # df['Date'] = pd.to_datetime(df.index);
                    
                    return df
                                    
        except Exception as exp:
            print ('Caught exception  ',exp);
            
        print (' end :: ',sys._getframe().f_code.co_name);
#------------------------------------------------------------------------------------------------    
        
        
    def getrecordforColumn(self,db_name,collection,column):
        
        print (' start :: ',sys._getframe().f_code.co_name);
       
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db_handler = self.client[db_name];       
                    coll_handler = my_db_handler[collection];
                    cursor = coll_handler.find();
                    data_list = list(cursor);
                    df = pd.DataFrame(data_list);
                    return df[column];
                                    
        except Exception as exp:
            print ('Caught exception  ',exp);
            
        print (' end :: ',sys._getframe().f_code.co_name);
#------------------------------------------------------------------------------------------------    
    def getrecordforfield(self,db_name,collection,column,value,field):
        
        print (' start :: ',sys._getframe().f_code.co_name);
       
        try:
            if(self.client != None and db_name != "" and collection != "" ):
                if (self.isCollectionExist(db_name,collection)):
                    my_db_handler = self.client[db_name];
                    coll_handler = my_db_handler[collection];
                    cursor = coll_handler.find({'Symbol':value});
                    
                    for item in cursor:
                        return item[field];
                                    
        except Exception as exp:
            print('Caught exception  ',exp);
            
        print (' end :: ',sys._getframe().f_code.co_name);
    
    # CURD Operation in to Mongo DB 
    

