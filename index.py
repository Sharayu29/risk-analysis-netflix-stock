import json
import http.client
from flask import Flask, request
import time
import math
import random
import yfinance as yf
import pandas as pd
from datetime import date, timedelta, datetime
from pandas_datareader import data as pdr
import numpy as np
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from functools import partial

app = Flask(__name__)

# Set the maximum request timeout in seconds to 10 minutes (600 seconds)
app.config['APPLICATION_ROOT'] = 600

def create_data_manager():
    stored_data = {}

    def store_data(key, value):
        stored_data[key] = value

    def get_data(key):
        nonlocal stored_data
        return stored_data.get(key)

    return store_data, get_data

store_data, get_data = create_data_manager()

def handle_error(exception):
    return json.dumps({"error": str(exception)})

@app.route('/warmup', methods=['POST'])
def warmup():
    try:
        global start_time, end_time
        start_time = time.time()
        data = request.get_json()

        store_data("s", data["s"])
        store_data("r", data["r"])

        c = http.client.HTTPSConnection("1dxs5laeug.execute-api.us-east-1.amazonaws.com")

        json_data = {
            "s": data["s"],
            "r": data["r"]
        }
        json_string = json.dumps(json_data)

        c.request("POST", "/default/warmup", json_string)
        response = c.getresponse()
        data = response.read().decode('utf-8')
        data = json.loads(data)
        end_time = time.time()

        return data
    except Exception as e:
        return handle_error(e)

@app.route('/get_warmup_cost', methods=['GET'])
def get_warmup_cost():
    time_consumed = end_time - start_time
    stored_s = get_data("s")
    stored_r = int(get_data("r"))
    if stored_s == 'lambda':
        cost = stored_r * 0.125 * 0.0000000021 * time_consumed * 1000
        return {"billable_time" : time_consumed, "cost" : cost}
    if stored_s == 'ec2':
        cost = stored_r * 0.0116 * time_consumed/3600
        return {"billable_time" : time_consumed, "cost" : cost}

@app.route('/terminate', methods=['GET'])
def terminate():
    try:
        c = http.client.HTTPSConnection("bccovkax00.execute-api.us-east-1.amazonaws.com")

        c.request("GET", "/default/terminate")
        response = c.getresponse()
        data = response.read().decode('utf-8')
        return data
    except Exception as e:
        return handle_error(e)

@app.route('/resources_ready', methods=['GET'])
def resources_ready():
    try:
        c = http.client.HTTPSConnection("j0tka584l0.execute-api.us-east-1.amazonaws.com")

        stored_s = get_data("s")
        stored_r = get_data("r")

        json_data = {
            "s": stored_s
        }
        json_string = json.dumps(json_data)

        c.request("POST", "/default/resources_ready", json_string)
        response = c.getresponse()
        data = response.read().decode('utf-8')
        parsed_data = json.loads(data)
        if parsed_data['response'] == True:
            return {"warm":"true"}
        else:
            return {"warm":"false"}
    except Exception as e:
        return handle_error(e)

@app.route('/resources_terminated', methods=['GET'])
def resources_terminated():
    try:
        c = http.client.HTTPSConnection("op8zzxzt4l.execute-api.us-east-1.amazonaws.com")

        c.request("GET", "/default/resources_terminated")
        response = c.getresponse()
        data = response.read().decode('utf-8')
        parsed_data = json.loads(data)
        if parsed_data['response'] == True:
            return {"terminated": True}
        else:
            return {"terminated":False}
    except Exception as e:
        return handle_error(e)

@app.route('/analyse', methods=['POST'])
def analyse():
    try:
        global start_time_analysis, end_time_analysis
        s = get_data("s")
        data_req = request.get_json()
        store_data("h", data_req["h"])
        store_data("d", data_req["d"])
        store_data("t", data_req["t"])
        store_data("p", data_req["p"])
        if s == 'lambda':
            start_time_analysis = time.time()

            data_res_parallel = parallel_function()
            end_time_analysis = time.time()
            return {"result":"ok"}
        
        elif s == 'ec2':
            start_time_analysis = time.time()
            data_res_ec2 = fetch_data_EC2()
            end_time_analysis = time.time()
            return {"result":"ok"}
        
    except Exception as e:
        return handle_error(e)

@app.route('/get_sig_vars9599', methods=['GET'])
def get_sig_vars9599():
    try:
        data_res_parallel = get_data("parallel_data")

        if len(data_res_parallel) != 0:
                
            sorted_data = sorted(data_res_parallel, key=lambda x: x["Date"], reverse=True)

            result = []

            for element in sorted_data:
                sorted_element = {
                    "var95": sorted(element["var95"], reverse=True)[:20],
                    "var99": sorted(element["var99"], reverse=True)[:20]
                }
                result.append(sorted_element)
            data_dict = {str(index): item for index, item in enumerate(result)}
            print(data_dict)
            data_dict = json.dumps(data_dict)
            return data_dict

        else:
            return {}

    except Exception as e:
        return handle_error(e)

@app.route('/get_endpoints', methods=['GET'])
def get_endpoints():
    try:
        s = get_data("s")
        if s == 'lambda':
            resources_dict = {
                "resource": "https://w8c3a8efb2.execute-api.us-east-1.amazonaws.com"
            }
            return resources_dict
        elif s=='ec2':
            c = http.client.HTTPSConnection("q7hnzbeotd.execute-api.us-east-1.amazonaws.com")    
            c.request("GET", "/default/get_endpoints", '')
            response = c.getresponse()
            data_res = response.read().decode('utf-8')
            global endpoints_data
            endpoints_data = json.loads(data_res)
            return endpoints_data
        else:
            return {}

    except Exception as e:
        return handle_error(e)

def parallel_function():
    s = get_data("s")
    r = int(get_data("r"))
    h = get_data("h")
    d = get_data("d")
    t = get_data("t")
    p = get_data("p")
    parallel = r
    runs=[value for value in range(parallel)]
    with ThreadPoolExecutor() as executor:
        results=list(executor.map(fetch_data_Lambda, runs))
    store_data("parallel_data", results)
    
    return results

@app.route('/get_avg_vars9599', methods=['GET'])
def get_avg_vars9599():
    try:
        parallel_data = get_data("parallel_data")
        if len(parallel_data) != 0:
            var95_sum = sum([sum(entry['var95']) for entry in parallel_data])
            num_elements = len(parallel_data) * len(parallel_data[0]['var95'])

            average_var95 = var95_sum / num_elements

            result = {
                "var95": average_var95,
                "var99": sum(parallel_data[0]['var99']) / len(parallel_data[0]['var99'])  # Using values from the first element
            }

            return result
        else:
            return {}
    except Exception as e:
        return handle_error(e)

@app.route('/get_sig_profit_loss', methods=['GET'])
def get_sig_profit_loss():
    try:
        profit_data = []
        parallel_data = get_data("parallel_data")

        if len(parallel_data) != 0:
            for i in range(int(len(parallel_data))):
                profit_data.append(get_profit_loss(parallel_data[i])['profit_loss_last_20'])
            profit_data = {str(index): item for index, item in enumerate(profit_data)}
            profit_data = json.dumps(profit_data)
            return profit_data
        else:
            return {}

    except Exception as e:
        return handle_error(e)

@app.route('/get_tot_profit_loss', methods=['GET'])
def get_tot_profit_loss():
    try:
        profit_data = []
        parallel_data = get_data("parallel_data")
        if len(parallel_data) != 0:
            for i in range(int(len(parallel_data))):
                profit_data.append(sum(get_profit_loss(parallel_data[i])['profit_loss_all']))
            return {"total_profit":profit_data[0]}
        else:
            return {}

    except Exception as e:
        return handle_error(e)

@app.route('/get_time_cost', methods=['GET'])
def get_time_cost():
    try:
        time_consumed = end_time_analysis - start_time_analysis
        stored_s = get_data("s")
        stored_r = int(get_data("r"))
        if stored_s == 'lambda':
            cost = stored_r * 0.125 * 0.0000000021 * time_consumed * 1000
            return {"billable_time" : time_consumed, "cost" : cost}
        if stored_s == 'ec2':
            cost = stored_r * 0.0116 * time_consumed/3600
            return {"billable_time" : time_consumed, "cost" : cost}
    except Exception as e:
        return handle_error(e)

@app.route('/get_audit', methods=['GET'])
def get_audit():
    try:
        s = get_data("s")
        r = get_data("r")
        h = get_data("h")
        d = get_data("d")
        t = get_data("t")
        p = get_data("p")

        profit_loss_res = get_tot_profit_loss()
        avg_res = get_avg_vars9599()
        av95 = avg_res["var95"]
        av99 = avg_res["var99"]
        time_cost_res = get_time_cost()
        time_consumed = time_cost_res["billable_time"]
        cost = time_cost_res["cost"]

        audit_res = {"s": s, "r":r, "h": h, "d":d, "t": t, "p":p, "profit_loss": profit_loss_res['total_profit'], "av95": av95, "av99": av99, "time": time_consumed, "cost": cost}
        c = http.client.HTTPSConnection("rjsp0zqo1c.execute-api.us-east-1.amazonaws.com")
        json_string = json.dumps(audit_res)
        c.request("POST", "/default/audit", json_string)
        response = c.getresponse()
        data_res = response.read().decode('utf-8')
        data_res = json.loads(data_res)
        nested_dict = {str(index): item for index, item in enumerate(data_res)}
        result_json = json.dumps(nested_dict)
        return result_json
    except Exception as e:
        return handle_error(e)

@app.route('/reset', methods=['GET'])
def reset():
    try:
        store_data("h", 0)
        store_data("d", 0)
        store_data("t", '')
        store_data("p", 0)
        store_data("parallel_data", [])
        start_time = 0 
        end_time = 0 
        start_time_analysis = 0 
        end_time_analysis = 0

        return {"result": "ok"}
        
    except Exception as e:
        return handle_error(e)


def fetch_data_Lambda(id):
    data_req = {}
    stored_s = get_data("s")
    stored_r = get_data("r")
    stored_h = get_data("h")
    stored_d = get_data("d")
    stored_t = get_data("t")
    stored_p = get_data("p")

    data_req["s"] = stored_s
    data_req["r"] = stored_r
    data_req['h'] = stored_h
    data_req['d'] = stored_d
    data_req['t'] = stored_t
    data_req['p'] = stored_p

    yf.pdr_override()

    # Get stock data from Yahoo Finance – here, asking for about 3 years
    today = date.today()
    decadeAgo = today - timedelta(days=1095)

    # Get stock data from Yahoo Finance – here, Gamestop which had an interesting
    #time in 2021: https://en.wikipedia.org/wiki/GameStop_short_squeeze

    data = pdr.get_data_yahoo('NFLX', start=decadeAgo, end=today)
    
    data['Buy']=0
    data['Sell']=0

    for i in range(2, len(data)):

        body = 0.01

        # Three Soldiers
        if (data.Close[i] - data.Open[i]) >= body  \
    and data.Close[i] > data.Close[i-1]  \
    and (data.Close[i-1] - data.Open[i-1]) >= body  \
    and data.Close[i-1] > data.Close[i-2]  \
    and (data.Close[i-2] - data.Open[i-2]) >= body:
            data.at[data.index[i], 'Buy'] = 1
            #print("Buy at ", data.index[i])

        # Three Crows
        if (data.Open[i] - data.Close[i]) >= body  \
    and data.Close[i] < data.Close[i-1] \
    and (data.Open[i-1] - data.Close[i-1]) >= body  \
    and data.Close[i-1] < data.Close[i-2]  \
    and (data.Open[i-2] - data.Close[i-2]) >= body:
            data.at[data.index[i], 'Sell'] = 1
            #print("Sell at ", data.index[i])
    data = data.reset_index()
    data['Date'] = data['Date'].astype(str)
    data = data.to_dict('records')
    store_data("signalled_data", data)

    data_req["data"] = data

    json_string = json.dumps(data_req)

    c = http.client.HTTPSConnection("w8c3a8efb2.execute-api.us-east-1.amazonaws.com")
    
    c.request("POST", "/default/analyse", json_string)
    
    response = c.getresponse()
    data_res = response.read().decode('utf-8')
    data_res = json.loads(data_res)
    store_data("data_res", data_res)
    return data_res

def fetch_data_EC2():
    data_req = {}
    stored_s = get_data("s")
    stored_r = get_data("r")
    stored_h = get_data("h")
    stored_d = get_data("d")
    stored_t = get_data("t")
    stored_p = get_data("p")

    data_req["s"] = stored_s
    data_req["r"] = stored_r
    data_req['h'] = stored_h
    data_req['d'] = stored_d
    data_req['t'] = stored_t
    data_req['p'] = stored_p

    yf.pdr_override()

    # Get stock data from Yahoo Finance – here, asking for about 3 years
    today = date.today()
    decadeAgo = today - timedelta(days=1095)

    # Get stock data from Yahoo Finance – here, Gamestop which had an interesting
    #time in 2021: https://en.wikipedia.org/wiki/GameStop_short_squeeze

    data = pdr.get_data_yahoo('NFLX', start=decadeAgo, end=today)
    
    data['Buy']=0
    data['Sell']=0

    for i in range(2, len(data)):

        body = 0.01

        # Three Soldiers
        if (data.Close[i] - data.Open[i]) >= body  \
    and data.Close[i] > data.Close[i-1]  \
    and (data.Close[i-1] - data.Open[i-1]) >= body  \
    and data.Close[i-1] > data.Close[i-2]  \
    and (data.Close[i-2] - data.Open[i-2]) >= body:
            data.at[data.index[i], 'Buy'] = 1
            #print("Buy at ", data.index[i])

        # Three Crows
        if (data.Open[i] - data.Close[i]) >= body  \
    and data.Close[i] < data.Close[i-1] \
    and (data.Open[i-1] - data.Close[i-1]) >= body  \
    and data.Close[i-1] < data.Close[i-2]  \
    and (data.Open[i-2] - data.Close[i-2]) >= body:
            data.at[data.index[i], 'Sell'] = 1
            #print("Sell at ", data.index[i])
    data = data.reset_index()
    data['Date'] = data['Date'].astype(str)
    data = data.to_dict('records')
    store_data("signalled_data", data)

    data_req["data"] = data

    json_string = json.dumps(data_req)
    ec2_analyse_data = []
    for item in endpoints_data:
        resource_url = item["resource"]
        ip_address = resource_url.split("http://")[1] 
        ip_address = str(ip_address)
        c = http.client.HTTPConnection(ip_address+":8080")
        c.request("POST", "/analyse", json_string, headers={"Content-Type": "application/json"})
        response = c.getresponse()
        data_res = response.read().decode('utf-8')
        data_res = json.loads(data_res)
        ec2_analyse_data.append(data_res)
        # store_data("data_res", data_res)
    store_data("parallel_data", ec2_analyse_data)
    return ec2_analyse_data

def get_profit_loss(data_res):
    try:
        p = get_data("p")

        signalled_data = get_data("signalled_data")

        data_res = pd.DataFrame.from_dict(data_res)
        data_res['Date'] = pd.to_datetime(data_res['Date'])
        data_res = data_res.sort_values(by='Date', ascending=True)
        data_res['New_date'] = data_res['Date'] + timedelta(days=int(p))

        signalled_data = pd.DataFrame.from_dict(signalled_data)
        signalled_data['Date'] = pd.to_datetime(signalled_data['Date'])
        signalled_data = signalled_data.sort_values(by='Date', ascending=True)

        profit_loss_dict = {"profit_loss_all": [], "profit_loss_last_20": []}

        for index, row in data_res.iterrows():
            signalled_row = signalled_data[signalled_data['Date'] == row['New_date']]
            if not signalled_row.empty:
                profit_loss = row['Close'] - signalled_row['Close'].iloc[0]
                profit_loss_dict["profit_loss_all"].append(profit_loss)

        profit_loss_dict['profit_loss_last_20'] = profit_loss_dict['profit_loss_all'][:20]

        print(profit_loss_dict)

        return profit_loss_dict

    except Exception as e:
        return handle_error(e)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)


