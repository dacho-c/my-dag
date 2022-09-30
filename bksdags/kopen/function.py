import datetime
import pandas as pd

def get_last_ym1():
    return '202201'

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

def get_first_ym_fisical_year():
    today = datetime.date.today()
    m = today.month
    if (m >= 4):
        today = today.replace(month=4)
    else:
        y = today.year - 1
        today = today.replace(year=y)
        today = today.replace(month=4)
    return today.strftime("%Y%m")

def get_Last_fisical_year():
    today = datetime.date.today()
    m = today.month
    if (m >= 4):
        today = today.replace(month=4)
    else:
        y = today.year - 1
        today = today.replace(year=y)
        today = today.replace(month=4)
    return today.strftime("%Y%m")