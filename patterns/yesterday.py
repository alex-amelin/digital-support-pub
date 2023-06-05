import datetime as dt

def getYesterday(): 
    today=dt.date.today() 
    oneday=dt.timedelta(days=1) 
    yesterday=today-oneday  
    return yesterday