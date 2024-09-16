import logging



class FormatDataTask():
    '''Cette task sert à mettre notre donné au bon format '''
            
    def format_data(self, data_json):
        logging.info(f"beginnig  of data format")
        formatData={}
        location= data_json['location']
        formatData['first_name']=data_json['name']['first']
        formatData['last_name']=data_json['name']['last']
        formatData['gender']=data_json['gender']
        formatData['address']= f"{str(location['street']['number'])}{location['street']['name']}," \
                       f"{str(location['city'])},{location['state']},{location['country']}"
        formatData['post_code']=location['postcode']
        formatData['email'] = data_json['email']
        formatData['username'] =data_json['login']['username']
        formatData['dob'] = data_json['dob']['date']
        formatData['registered_date'] = data_json['registered']['date']
        formatData['phone'] = data_json['phone']
        formatData['picture'] = data_json['picture']['medium']
        
        logging.info(f"End of data format")
        return formatData
                       
       