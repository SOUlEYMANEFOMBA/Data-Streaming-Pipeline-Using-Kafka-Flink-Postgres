import json
import requests
import logging

class DoawloadDataTask():
    """
    Cette classe est responsable de l'importation des données en utilisant l'API Random User Generator.
    
    Attributes:
        url (str): L'URL de l'API pour obtenir les données des utilisateurs.
    """
    def __init__(self,url):
            self.url = url
            
    def get_data(self):
       """
        Cette méthode récupère les données des utilisateurs depuis l'API et retourne le premier utilisateur.

        Returns:
            dict: Un dictionnaire contenant les informations du premier utilisateur retourné par l'API.

        Raises:
            requests.exceptions.RequestException: Si une erreur se produit lors de la requête HTTP.
        """
       logging.info(f"beginnig  of doawload data")
       res = requests.get(self.url)
       respond=res.json()
       result=respond['results'][0]
       logging.info(f"End  of doawload data")
       return result