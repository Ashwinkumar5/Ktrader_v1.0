# -*- coding: utf-8 -*-
"""
Created on Fri Nov 11 16:22:02 2022

@author: Ashwin
"""

import smtplib;
import common as ut;
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders



class Email(object):
    
    def __init__(self):
        self.receipient = ['ashwinkumar.yeole@gmail.com','aparnamusale18@gmail.com'];
        self.frommail="personaldocs007@gmail.com";
        self.pwd='idrucshtspqhhmga';

    def getBody(self):
        str = "Hi, \n Please find today's Swing analysis chart .\n Analysise and Invest on your won Risk \n follow rules only success ...\n";
        return str;
    
    def getSubject(self):
        str = "KTrader Swing strategy for " +ut.getCurrectDateString();
        return str;
        
    def getFrom(self):
        return self.frommail;
    
    def getTo(self):        
        return self.receipient;
            
        
    def sendEmail(self):
        
        try:
           
            #print(1); 
            msg = MIMEMultipart();
            #print(2); 
            #Mention plain text, to aoiv email to spam 
            Body = MIMEText(self.getBody(),'plain');
            #print(3); 
            # Add Headers
            msg['Subject']  = self.getSubject();
            #print(4); 
            msg['From']     = self.getFrom();
            #print(4.1); 
            msg['To']       = ",".join(self.getTo());
            #print(4.2); 
            msg['CC']       = ''
            #print(4.3); 
            msg['BCC']      = ''
            #print(4.4); 
            
            #Attaching body 
           
            msg.attach(Body);
            #print (ut.getZip());
                       
            part = MIMEBase("application", "octet-stream");            
            part.set_payload(open(ut.getZip(), "rb").read());
            encoders.encode_base64(part);
            part.add_header("Content-Disposition", "attachment; filename=\"%s.zip\"" % (ut.getZip().split("\\")[-1]));
            msg.attach(part)
           
            #print (5); 
            # creates SMTP session
            s = smtplib.SMTP('smtp.gmail.com', 587)
            #print (6); 
            # start TLS for security
            s.starttls()
            #print (7); 
            # Authentication
            s.login(self.getFrom(),self.pwd);
            #print (8); 
            # Converts the Multipart msg into a string
            text = msg.as_string()
            #print (9); 
            # sending the mail
            s.sendmail(self.getFrom(), self.getTo(), text)
            #print (10); 
            # terminating the session
            s.quit()            
            #print (11); 
        except Exception as exp:
            print (" Exception while sending Email ...",exp);
            return False;
        else:
            return True;