
import pandas as pd
import datetime

def get_last_ym():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    return lastMonth.strftime("%Y%m")

def get_this_ym():
    today = datetime.date.today()
    return today.strftime("%Y%m")

def get_today():
    today = datetime.date.today()
    return today.strftime("%Y-%m-%d")

def get_yesterday():
    today = datetime.date.today()
    yday = today - datetime.timedelta(days=1)
    return yday.strftime("%Y-%m-%d")

def get_firstdate_this_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    return first.strftime("%Y-%m-%d")

def get_lastdate_this_m():
    today = datetime.date.today()
    p = pd.Period(today.strftime("%Y-%m-%d"))
    lday = p.days_in_month
    last = today.replace(day=lday)
    return last.strftime("%Y-%m-%d")

def get_firstdate_last_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    first = lastMonth.replace(day=1)
    return first.strftime("%Y-%m-%d")

def get_lastdate_last_m():
    today = datetime.date.today()
    first = today.replace(day=1)
    lastMonth = first - datetime.timedelta(days=1)
    return lastMonth.strftime("%Y-%m-%d")

# get fisical year