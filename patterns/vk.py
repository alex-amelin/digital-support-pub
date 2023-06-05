import requests
import os
import json
import arrow
from collections import defaultdict
from urllib.parse import urlparse, parse_qs, quote
from dotenv import load_dotenv
from pprint import pprint

storage_fn = 'vk_data.json'

def save_dict(d):
    raw = json.dumps(d)
    with open(storage_fn, 'w') as f:
        f.write(raw)
        
# Загрузка данных из файла, если таковой имеется. 
# Если файл отсутствует это нормально и ошибки не будет
def read_dict():
    try:
        with open(storage_fn, 'r') as f:
            raw = f.read()
            return json.loads(raw)
    except:
        pass
    return {}