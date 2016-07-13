import requests
import clearbit
import time
import yaml
import fetch_data


def format_request(company):
    company['response_timestamp'] = time.time()
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


def format_null_request(domain):
    return  {
        "client_id": config['rjm_cid'],
        "table_name": "null_responses",
        "sequence": int(round(time.time() * 1000)),
        "action": "upsert",
        "key_names": [
            "domain"
            ],
        "data": {
            "domain": domain,
            "response_timestamp": time.time()
            }
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

def fetch_and_process(query):
    fetch_config = config['fetch']
    batch_size = fetch_config['batch_size']

    request_list = []
    def handle_row(row):
        domain = row[0]
        company = get_company(domain)
        if company:
            data = format_request(company)
        else:
            data = format_null_request(domain)
        request_list.append(data)

        if len(request_list) >= batch_size:
            send_to_rj(request_list)
            del request_list[:]

    fetch_data.fetch(fetch_config, query, handle_row)

def process_list(domains):
    data = []
    for domain in domains:
        company = get_company(domain)
        data.append(format_request(company)) if company else data.append(format_null_request(domain))
    print(data)
    send_to_rj(data)

def get_config():
    config = {}
    with open('config.yml', 'r') as f:
        m = yaml.safe_load(f)
        config.update(m)
    return config


config = get_config()
clearbit.key = config['clearbit_access_token']
fetch_and_process("select domain from accounts limit 10")
