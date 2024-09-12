import json
import requests

class StreamingDataTask():
    '''Cette task sert à importer notre base de donnée en l'utilisant l' api user random ui'''
    def __init__(self,url):
            self.url = url
            
    def streaming_data(self):
       res = requests.get(self.url)
       respond=res.json()
       result=respond['results'][0]
       print(json.dumps(result,indent=5))