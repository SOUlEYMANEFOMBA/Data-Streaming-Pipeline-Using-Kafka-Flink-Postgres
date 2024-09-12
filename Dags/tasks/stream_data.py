import json
import requests

class StreamingDataTask():
    '''Cette task sert à importer notre base de donnée en l'utilisant l' api user random ui'''
    def __init__(self,url):
            self.url = url
            
    def streaming_data(self):
       reponds = requests.get(self.url)
    #    reponds.json()
       print(reponds.json())