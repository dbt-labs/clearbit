import requests
import clearbit
import time
import yaml


def format_data(company):
    return  {
        "client_id": config['rjm_cid'],
        "table_name": "companies",
        "sequence": int(round(time.time() * 1000)),
        "action": "upsert",
        "key_names": [
            "id"
            ],
        "data": company
        }

def send_to_rj(data):
    suffix = 'push' if config['mode'] == 'production' else 'validate'
    url = "%s/%s" % (config['rjm_base_url'], suffix)
    print(url)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer %s" % (config['rjm_access_key'])
        }
    r = requests.post(url, json = data, headers = headers)


def get_company(target_domain):
    company = clearbit.Company.find(domain=target_domain)
    if company != None and 'pending' not in company:
      return company


def process_list(domains):
    data = []
    for domain in domains:
        company = get_company(domain)
        if company: data.append(format_data(company))
    send_to_rj(data)

def get_config():
    config = {}
    with open('config.yml', 'r') as f:
        m = yaml.safe_load(f)
        config.update(m)
    return config


config = get_config()
clearbit.key = config['clearbit_access_token']
process_list(['stripe.com', 'rjmetrics.com'])
