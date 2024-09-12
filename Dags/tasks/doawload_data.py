import json
import requests
import logging

class DoawloadDataTask():
    '''Cette task sert à importer notre base de donnée en l'utilisant l' api user random ui'''
    def __init__(self,url):
            self.url = url
            
    def get_data(self):
       logging.info(f"beginnig  of doawload data")
       res = requests.get(self.url)
       respond=res.json()
       result=respond['results'][0]
       logging.info(f"End  of doawload data")
       return result