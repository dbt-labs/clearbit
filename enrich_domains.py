import requests
import clearbit
import time
import yaml
import sys
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
    counts = {
        'total': 0,
        'ok': 0,
        'error': 0,
    }
    def handle_row(row):
        counts['total'] += 1

        domain = row[0]

        try:
          company = get_company(domain)
        except requests.exceptions.RequestException as e:
          counts['error'] += 1
          print "ERROR: {}".format(e.message)
          return

        counts['ok'] += 1
        if company:
            data = format_request(company)
        else:
            data = format_null_request(domain)
        request_list.append(data)

        if len(request_list) >= batch_size:
            send_to_rj(request_list)
            del request_list[:]

        if counts['total'] % 2 == 0:
          print "TOTAL: {}\tOK: {}\tERROR: {}".format(counts['total'], counts['ok'], counts['error'])

        if counts['error'] > int(counts['total'] * 0.1) and counts['total'] >= 10:
          print "TOO MANY ERRORS (> 10%) -- Quitting!"
          sys.exit(1)

    fetch_data.fetch(fetch_config, query, handle_row)

    # clean up any leftovers
    if len(request_list) > 0:
      send_to_rj(request_list)

    print "DONE"
    print "TOTAL: {}\tOK: {}\tERROR: {}".format(counts['total'], counts['ok'], counts['error'])

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
