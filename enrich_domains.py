import requests
import clearbit
import time
import yaml
import sys
import os, sys
import datafetcher

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
    print("SENDING TO: {}".format(url))
    headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer %s" % (config['rjm_access_key'])
        }
    r = requests.post(url, json = data, headers = headers)
    if r.status_code in [200, 201]:
      print("  -> Success {}".format(r.status_code))
    else:
      print("  -> ERROR! {}".format(r.content))

def get_company(target_domain):
    company = clearbit.Company.find(domain=target_domain, stream=True)
    if company != None and 'pending' not in company:
      return company

def fix_encode(company, field_name):
    "some responses contain utf-8 or None 'domain' or 'company' fields"

    if field_name in company and company[field_name] is not None:
        return company[field_name].encode('ascii', 'ignore')
    else:
        return "unknown"

def get_response_for_found_domain(domain, company):
    # in case there's an alias which doesn't get picked up by clearbit
    company['requested_domain'] = domain
    response = format_request(company)
    company_domain = fix_encode(company, 'domain')
    company_name   = fix_encode(company, 'name')
    print("{} Found:\t{}\t{}\t{}".format(counts['total'], domain, company_domain, company_name))
    counts['ok'] += 1
    return response

def get_response_for_missing_domain(domain, error_reason):
    response = format_null_request(domain)
    print("{} Not found:\t{}\t{}".format(counts['total'], domain, error_reason))
    counts['error'] += 1
    return response

def get_response_for_domain(domain):
    counts['total'] += 1
    try:
        company = get_company(domain)
    except requests.exceptions.RequestException as e:
        error_type = e.response.json().get('error', {}).get('type', 'unknown')
        return get_response_for_missing_domain(domain, error_type)

    if company:
        response = get_response_for_found_domain(domain, company)
    else:
        response = get_response_for_missing_domain(domain, "missing")

    return response

def fetch_and_process(query):
    fetch_config = config['fetch']
    batch_size = fetch_config['batch_size']

    for batch_of_records in datafetcher.fetch(fetch_config, query, batch_size):
        responses = [get_response_for_domain(record['domain']) for record in batch_of_records]
        send_to_rj(responses)

        print("TOTAL: {}\tOK: {}\tERROR: {}".format(counts['total'], counts['ok'], counts['error']))

    print("DONE")

def get_config():
    config = {}
    with open('config.yml', 'r') as f:
        m = yaml.safe_load(f)
        config.update(m)
    return config

if len(sys.argv) != 2:
    print("usage: {} [query-file]".format(sys.argv[0]))
    sys.exit(1)

query_file = sys.argv[1]
if not os.path.exists(query_file):
    print("Error: {} does not exist".format(query_file))
    sys.exit(1)

counts = {'total': 0, 'ok': 0, 'error': 0}
query = open(query_file).read()
config = get_config()
clearbit.key = config['clearbit_access_token']

fetch_and_process(query)
