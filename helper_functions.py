import pandas as pd
import json
import numpy as np
import math

#This script contains helper functions that will be used in the Flask API

#TODO
#Continue building out functions to calculate stats for flask
#Potentially standardize the decimal point rounding

#Takes in a pandas dateframe of returns, outputs cumulative return as float
def total_return(daily_ret):
    #Type checking
    if isinstance(daily_ret, pd.DataFrame):
        #Calculate series of cumulative returns
        cum_returns = daily_ret.cumsum()
    
    #Select the last value of the cumulative returns, convert to float 
        value = float(cum_returns.iloc[-1][0])
        return '%.5f' % round(value, 5) #Round to 3 decimal places
    else:
        raise ValueError('Expected a dataframe for total return')

#Takes in a pandas dataframe of returns, outputs average daily return  as float
def avg_return(ret):
    if isinstance(ret, pd.DataFrame):
        avg = ret.mean()
        return '%.5f' % round(avg, 5) 
    else:
        raise ValueError('Expected a dataframe for daily average return')

#Takes in a pandas dataframe of returns, outputs daily standard deviation 
def calc_std(ret):
    if isinstance(ret, pd.DataFrame):
        std = ret.std()
        return '%.5f' % round(std, 5) 
    else:
        raise ValueError('Expected a dataframe for daily standard deviation')

#Takes in a pandas dataframe, returns skewness of daily returns
def calc_skew(ret):
    if isinstance(ret, pd.DataFrame):
        return '%.5f' % round(ret.skew(),5)
    else:
        raise ValueError('Expected a dataframe for daily skew')

#Takes in a pandas dataframe, returns the kurtosis of daily returns
def calc_kurt(ret):
    if isinstance(ret, pd.DataFrame):
        return '%.5f' % round(ret.kurtosis(),5)
    else:
        raise ValueError('Expected a dataframe for daily kurtosis')  

def min_ret(daily_ret):
    if isinstance(daily_ret, pd.DataFrame):
        return '%.5f' % round(daily_ret.min(),5)
    else:
        raise ValueError('Expected a dataframe for daily minimum')  

def max_ret(daily_ret):
    if isinstance(daily_ret, pd.DataFrame):
        return '%.5f' % round(daily_ret.max(),5)
    else:
        raise ValueError('Expected a dataframe for daily maximum')  

#This can handle monthly, weekly, and daily returns
def make_histogram_daily(ret):
    if isinstance(ret, pd.DataFrame):
        #Use Numpy histogram functionality
        count, division = np.histogram(ret, bins=np.linspace(-.05,.05,101),density=True)

        #Convert to density values of unity density (so it sums to 1)
        unity_count = count / count.sum()

        #Convert to python native datatypes
        native_count = [np.asscalar(c) for c in unity_count]
        str_division = [str(d) for d in division]

        #Form bin/count pairs 
        histogram_dict = dict(zip(str_division,native_count))

        return json.dumps(histogram_dict)
    else:
        raise ValueError('Expected a dataframe for histogram calculations')  
        
def make_histogram_weekly(ret):
    if isinstance(ret, pd.DataFrame):
        #Use Numpy histogram functionality
        count, division = np.histogram(ret, bins=np.linspace(-.15,.15,101),density=True)
        
        #Convert to density values of unity density (so it sums to 1)
        unity_count = count / count.sum()

        #Convert to python native datatypes
        native_count = [np.asscalar(c) for c in unity_count]
        str_division = [str(d) for d in division]

        #Form bin/count pairs 
        histogram_dict = dict(zip(str_division,native_count))

        return json.dumps(histogram_dict)
    else:
        raise ValueError('Expected a dataframe for histogram calculations')
        
def make_histogram_monthly(ret):
    if isinstance(ret, pd.DataFrame):
        #Use Numpy histogram functionality
        count, division = np.histogram(ret, bins=np.linspace(-.3,.3,101), density=True)

        #Convert to density values of unity density (so it sums to 1)
        unity_count = count / count.sum()

        #Convert to python native datatypes
        native_count = [np.asscalar(c) for c in unity_count]
        str_division = [str(d) for d in division]

        #Form bin/count pairs 
        histogram_dict = dict(zip(str_division,native_count))

        return json.dumps(histogram_dict)
    else:
        raise ValueError('Expected a dataframe for histogram calculations')  

def calculate_quartile(daily_ret,quartile):
    if isinstance(daily_ret, pd.DataFrame):
        return "%.5f" % round(daily_ret.quantile(quartile),5)
    else:
        raise ValueError('Expected a dataframe from 5% quartile calculations')  

def calculate_autocorrelation(ret):
    if isinstance(ret, pd.DataFrame):
        auto_corr_array = []
        count_array = []

        for i in range(0,22):
            auto_corr_array.append(ret.Adj_Close.autocorr(i))
            count_array.append(i)

        autocorrelation_dict = dict(zip(count_array,auto_corr_array))
        return_dict = {}
        
        for key in autocorrelation_dict:
            if not math.isnan(autocorrelation_dict[key]):
                return_dict[key] = autocorrelation_dict[key]

        return json.dumps(return_dict)
    else:
        raise ValueError('Expected a dataframe for autocorrelation calculations') 
