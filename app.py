from flask import Flask, jsonify
import random
import datetime
import quandl as q
import numpy as np
import pandas as pd
import requests
from flask_cors import CORS
from helper_functions import *

#Key to connect to the Quandl API
q.ApiConfig.api_key = "xaFxr9SP6Wd5sKFHdEax"

app = Flask(__name__)
CORS(app)
@app.route('/', methods=['GET', 'POST'])

@app.route("/<stock>/<start_date>/<end_date>")
def return_data(stock, start_date, end_date):
	
	#Pull metadata from Quandl (ticker and name)
	request_string = 'https://www.quandl.com/api/v3/datasets/EOD/{0}/metadata.json?api_key=xaFxr9SP6Wd5sKFHdEax'.format(stock)
	json_response = requests.get(request_string).json()

	#Check for valid ticker
	if 'quandl_error' in json_response.keys():
		return jsonify({"is_valid": False,
						"is_asset_error": True})

	ticker = '('+json_response['dataset']['dataset_code']+')'
	name_to_parse = json_response['dataset']['name']
	oldest_available = json_response['dataset']['oldest_available_date']
	newest_available = json_response['dataset']['newest_available_date']
	asset_name = name_to_parse.split('(')[0].replace('Inc.',"").replace('  ',' ').strip()

	#Data checking to make sure data is valid
	oldest_datetime = pd.to_datetime(oldest_available,format="%Y-%m-%d")
	newest_datetime = pd.to_datetime(newest_available,format="%Y-%m-%d")
	start_datetime = pd.to_datetime(start_date,format="%Y-%m-%d")
	end_datetime = pd.to_datetime(end_date,format="%Y-%m-%d")

	date_diff = end_datetime - start_datetime

	#Date check, if not a valid range given the data return this response
	if(start_datetime < oldest_datetime or end_datetime > newest_datetime or start_datetime > end_datetime):
		return jsonify({"oldest_available": oldest_available,
				        "newest_available": newest_available,
				        "is_valid": False,
						"is_date_error": True})

	#Make sure a decently wide range of dates is selected (set to a month right now)
	if(date_diff.days <= 31):
		return jsonify({"is_valid": False,
						"is_date_diff_error": True})

	
    
	#Pull daily data from Quandl
	daily_data = q.get("EOD/{0}.11".format(stock), #Only pull closing price
				start_date="{0}".format(start_date), 
				end_date="{0}".format(end_date))

	#Pull weekly data from Quandl
	weekly_data = q.get("EOD/{0}.11".format(stock), #Only pull closing price
				collapse="weekly",
				start_date="{0}".format(start_date), 
				end_date="{0}".format(end_date))

	#Pull monthly data from Quandl
	monthly_data = q.get("EOD/{0}.11".format(stock), #Only pull closing price
				collapse="monthly",
				start_date="{0}".format(start_date), 
				end_date="{0}".format(end_date))

	#Calculate log returns for all frequencies
	daily_returns = np.log(1 + daily_data.pct_change().dropna())
	weekly_returns = np.log(1 + weekly_data.pct_change().dropna())
	monthly_returns = np.log(1 + monthly_data.pct_change().dropna())

	#Assign current date to return
	current_date = datetime.date.today().strftime('%m/%d/%Y')

	#Assign start date from query
	start_date_return = start_datetime.strftime('%m/%d/%Y')

	#Assign end date from query
	end_date_return = end_datetime.strftime('%m/%d/%Y')	

	#Assign cumulative returns
	cumulative_returns = total_return(daily_returns)
	weekly_cumulative_returns = total_return(weekly_returns)
	monthly_cumulative_returns = total_return(monthly_returns)

	#Assign average returns
	daily_average_return = avg_return(daily_returns)
	weekly_average_return = avg_return(weekly_returns)
	monthly_average_return = avg_return(monthly_returns)

	#Assign standard deviation
	daily_standard_deviation = calc_std(daily_returns)
	weekly_standard_deviation = calc_std(weekly_returns)
	monthly_standard_deviation = calc_std(monthly_returns)

	#Assign skewness of daily returns
	daily_skewness = calc_skew(daily_returns)
	weekly_skewness = calc_skew(weekly_returns)
	monthly_skewness = calc_skew(monthly_returns)

	#Assign kurtosis of returns
	daily_kurtosis = calc_kurt(daily_returns)
	weekly_kurtosis = calc_kurt(weekly_returns)
	monthly_kurtosis = calc_kurt(monthly_returns)

	#Assign minimum return
	minimum_ret = min_ret(daily_returns)
	weekly_minimum_ret = min_ret(weekly_returns)
	monthly_minimum_ret = min_ret(monthly_returns)

	#Assign maximum return
	maximum_ret = max_ret(daily_returns)
	weekly_maximum_ret = max_ret(weekly_returns)
	monthly_maximum_ret = max_ret(monthly_returns)

	#Assign quartile 5%
	quartile_05 = calculate_quartile(daily_returns,.05)
	weekly_quartile_05 = calculate_quartile(weekly_returns,.05)
	monthly_quartile_05 = calculate_quartile(monthly_returns,.05)

	#Assign quartile 25%
	quartile_25 = calculate_quartile(daily_returns,.25)
	weekly_quartile_25 = calculate_quartile(weekly_returns,.25)
	monthly_quartile_25 = calculate_quartile(monthly_returns,.25)

	#Assign quartile 50%
	quartile_50 = calculate_quartile(daily_returns,.50)
	weekly_quartile_50 = calculate_quartile(weekly_returns,.50)
	monthly_quartile_50 = calculate_quartile(monthly_returns,.50)

	#Assign quartile 75%
	quartile_75 = calculate_quartile(daily_returns,.75)
	weekly_quartile_75 = calculate_quartile(weekly_returns,.75)
	monthly_quartile_75 = calculate_quartile(monthly_returns,.75)

	#Assign quartile 95%
	quartile_95 = calculate_quartile(daily_returns,.95)
	weekly_quartile_95 = calculate_quartile(weekly_returns,.95)
	monthly_quartile_95 = calculate_quartile(monthly_returns,.95)

	#Create a data set to build a histogram of daily returns
	daily_histogram_data = make_histogram_daily(daily_returns)
	
	#Create a data set to build a histogram of weekly returns
	weekly_histogram_data = make_histogram_weekly(weekly_returns)

	#Create a data set to build a histogram of monthly returns
	monthly_histogram_data = make_histogram_monthly(monthly_returns)

	#Create a data set to build a chart of autocorrelation of daily returns
	daily_autocorrelation_data = calculate_autocorrelation(daily_returns)

	#Create a data set to build a chart of autocorrelation of weekly returns
	weekly_autocorrelation_data = calculate_autocorrelation(weekly_returns)

	#Create a data set to build a chart of autocorrelation of monthly returns
	monthly_autocorrelation_data = calculate_autocorrelation(monthly_returns)

	return jsonify({"cumulative_returns": cumulative_returns,
					"weekly_cumulative_returns": weekly_cumulative_returns,
					"monthly_cumulative_returns": monthly_cumulative_returns,
					"start_date": start_date_return,
					"end_date": end_date_return,
					"daily_average_return": daily_average_return,
					"weekly_average_return": weekly_average_return,
					"monthly_average_return": monthly_average_return,
					"daily_standard_deviation": daily_standard_deviation,
					"weekly_standard_deviation": weekly_standard_deviation,
					"monthly_standard_deviation": monthly_standard_deviation,
					"daily_skewness": daily_skewness,
					"weekly_skewness": weekly_skewness,
					"monthly_skewness": monthly_skewness,
					"daily_kurtosis": daily_kurtosis,
					"weekly_kurtosis": weekly_kurtosis,
					"monthly_kurtosis": monthly_kurtosis,
					"minimum_return": minimum_ret,
					"weekly_minimum_return": weekly_minimum_ret,
					"monthly_minimum_return": monthly_minimum_ret,
					"maximum_return": maximum_ret,
					"weekly_maximum_return": weekly_maximum_ret,
					"monthly_maximum_return": monthly_maximum_ret,
					"daily_histogram_data": daily_histogram_data,
					"weekly_histogram_data": weekly_histogram_data,
					"monthly_histogram_data": monthly_histogram_data,
					"daily_autocorrelation_data": daily_autocorrelation_data,
					"weekly_autocorrelation_data": weekly_autocorrelation_data,
					"monthly_autocorrelation_data": monthly_autocorrelation_data,
					"quartile_05": quartile_05,
					"weekly_quartile_05": weekly_quartile_05,
					"monthly_quartile_05": monthly_quartile_05,
					"quartile_25": quartile_25,
					"weekly_quartile_25": weekly_quartile_25,
					"monthly_quartile_25": monthly_quartile_25,
					"quartile_50": quartile_50,
					"weekly_quartile_50": weekly_quartile_50,
					"monthly_quartile_50": monthly_quartile_50,
					"quartile_75": quartile_75,
					"weekly_quartile_75": weekly_quartile_75,
					"monthly_quartile_75": monthly_quartile_75,
					"quartile_95": quartile_95,
					"weekly_quartile_95": weekly_quartile_95,
					"monthly_quartile_95": monthly_quartile_95,
					"asset_symbol": ticker,
					"asset_name": asset_name,
					"oldest_available": oldest_available,
					"newest_available": newest_available,
					"is_valid": True
})

if __name__ == "__main__":
	app.run(debug=True, host='0.0.0.0', port=8080)
