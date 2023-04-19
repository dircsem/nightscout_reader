import json
from urllib import request
import hashlib


def build_url(apiUrl, dataset, page_field, start, end):
    if apiUrl is None:
        raise 'Invalid URL'
    
    if not apiUrl.endswith("/"):
        apiUrl += "/"
    
    date_query = ''
    if start is not None:
        date_query = f'''find[{page_field}][$gt]={start}&'''
    if end is not None:
        date_query = f'''find[{page_field}][$lt]={end}&'''
        
    return f"""{apiUrl}api/v1/{dataset}.json?{date_query}count=1000"""

def load_heroku_addresses():
    return [#ns address list]

def fetch_data(dataset, page_field):
    addresses = load_heroku_addresses()
    start_date = None
    end_date = None
    resultados = list()
    for address in addresses:
        owner = hashlib.md5(bytes(address,'utf-8')).hexdigest()
        print(owner)
        print(address)
        address = f'''https://{address}.herokuapp.com/'''
        count = 1000
        index = 0
        df = None
        try:
          df  = spark.sql(f"select max({page_field}) from bronze.{dataset} where owner = '{owner}'")
        except:
          pass
        full_load = False
        if df is None or df.isEmpty():
          start_date = None
        else:
          start_date = df.first()[0]
        
        while count == 1000 and (index == 0 or (index > 0 and  end_date is not None)):
            value = build_url(address,dataset, page_field, start_date, end_date)
            print(value)
            req = request.Request(value)
            with request.urlopen(req) as response:
                answers_data = json.loads(response.read())
            for answer in answers_data:
                answer.update({"owner": owner})
            count = len(answers_data)
            if count>0:
              end_date = answers_data[count-1][page_field]
              print(end_date)

              index+=1
              print(index)
              resultados.extend(answers_data)
        start_date = None
        end_date = None
        
    return resultados



entries = fetch_data('entries', 'date')
treatments = fetch_data('treatments','created_at')
status = fetch_data('devicestatus','created_at')
profiles = fetch_data('profile','created_at')

