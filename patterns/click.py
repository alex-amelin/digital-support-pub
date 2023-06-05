import requests
import pandas as pd
import io
from datetime import datetime


def click_mt(start,end,uid,tkn):
    # список аккаунтов
    url_api = 'https://api.click.ru/V0/accounts'
    headers = {
        'X-Auth-Token': tkn,
        'accept': 'application/json',
        'X-Auth-UserId': uid
    }

    data = requests.get(url_api, headers=headers).json()
    accounts = pd.json_normalize(data)
    accounts = accounts.explode('response.accounts').reset_index(drop=True)
    accounts = pd.json_normalize(accounts['response.accounts'])
    accounts = accounts.rename(columns={'id':'accountId', 'name':'account_name'})
    accounts = accounts[['accountId','account_name','service']]

    accounts = accounts.loc[accounts['service'] == 'MYTARGET']

    acc_list = accounts['accountId'].tolist()
    # список кампаний
    camp_all = pd.DataFrame()

    for i in acc_list:
        url_api = 'https://api.click.ru/V0/campaigns?accountId=' + str(i) + '&showDeleted=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'application/json',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).json()
        campaigns = pd.json_normalize(data)
        campaigns = campaigns.explode('response.items').reset_index(drop=True)
        campaigns = pd.json_normalize(campaigns['response.items'])
        campaigns = campaigns.rename(columns={'serviceId':'campaignId'})
        if campaigns.empty:
            pass
        else:
            campaigns = campaigns[['state', 'campaignId', 'accountId', 'name', 'isDeleted']]
            campaigns = campaigns.groupby('campaignId')['accountId','name','state'].first().reset_index()
            camp_all = pd.concat([camp_all, campaigns], ignore_index=True)

    # stats
    stats = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,date,impressions,clicks,cost,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }
        data = requests.get(url_api, headers=headers).content
        stats_part = pd.read_csv(io.StringIO(data.decode('utf-8')))
        stats = pd.concat([stats, stats_part], ignore_index=True)

    # conversions
    conv_all = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,conversionName,date,conversions,service,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).content
        conv = pd.read_csv(io.StringIO(data.decode('utf-8')))
        conv = conv.pivot_table(values='conversions', index=['date','accountId','campaignId'], columns=['conversionName'], aggfunc=sum)
        conv = conv.reset_index()
        conv['new_index'] = conv[['date','accountId','campaignId']].apply(lambda x: '|'.join(x.astype(str)), axis=1)
        conv = conv.drop('new_index', axis=1)
        conv = conv.fillna(value=0)
        conv_all = pd.concat([conv_all, conv], ignore_index=True)

    if camp_all.empty:
        res_df = pd.merge(stats, accounts, on='accountId', how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)
    else:
        res_df = pd.merge(camp_all, accounts, on='accountId', how='left')
        res_df = pd.merge(stats, res_df, on=['accountId','campaignId'], how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)

    res_df = res_df.rename(columns={'campaignId':'CampaignId'})
    res_df = res_df.rename(columns={'name':'CampaignName'})
    res_df = res_df.rename(columns={'date':'Date'})
    res_df = res_df.rename(columns={'impressions':'Impressions'})
    res_df = res_df.rename(columns={'clicks':'Clicks'})
    res_df = res_df.rename(columns={'cost':'Cost'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df['TrafficType'] = 'Таргет'

    return(res_df)

def click_vk(start,end,uid,tkn):
    # список аккаунтов
    url_api = 'https://api.click.ru/V0/accounts'
    headers = {
        'X-Auth-Token': tkn,
        'accept': 'application/json',
        'X-Auth-UserId': uid
    }

    data = requests.get(url_api, headers=headers).json()
    accounts = pd.json_normalize(data)
    accounts = accounts.explode('response.accounts').reset_index(drop=True)
    accounts = pd.json_normalize(accounts['response.accounts'])
    accounts = accounts.rename(columns={'id':'accountId', 'name':'account_name'})
    accounts = accounts[['accountId','account_name','service']]

    accounts = accounts.loc[accounts['service'].isin(['VK_ADS','VK'])]

    acc_list = accounts['accountId'].tolist()
    # список кампаний
    camp_all = pd.DataFrame()

    for i in acc_list:
        url_api = 'https://api.click.ru/V0/campaigns?accountId=' + str(i) + '&showDeleted=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'application/json',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).json()
        campaigns = pd.json_normalize(data)
        campaigns = campaigns.explode('response.items').reset_index(drop=True)
        campaigns = pd.json_normalize(campaigns['response.items'])
        campaigns = campaigns.rename(columns={'serviceId':'campaignId'})
        if campaigns.empty:
            pass
        else:
            campaigns = campaigns[['state', 'campaignId', 'accountId', 'name', 'isDeleted']]
            campaigns = campaigns.groupby('campaignId')['accountId','name','state'].first().reset_index()
            camp_all = pd.concat([camp_all, campaigns], ignore_index=True)

    # stats
    stats = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,date,impressions,clicks,cost,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }
        data = requests.get(url_api, headers=headers).content
        stats_part = pd.read_csv(io.StringIO(data.decode('utf-8')))
        stats = pd.concat([stats, stats_part], ignore_index=True)

    # conversions
    conv_all = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,conversionName,date,conversions,service,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).content
        conv = pd.read_csv(io.StringIO(data.decode('utf-8')))
        conv = conv.pivot_table(values='conversions', index=['date','accountId','campaignId'], columns=['conversionName'], aggfunc=sum)
        conv = conv.reset_index()
        conv['new_index'] = conv[['date','accountId','campaignId']].apply(lambda x: '|'.join(x.astype(str)), axis=1)
        conv = conv.drop('new_index', axis=1)
        conv = conv.fillna(value=0)
        conv_all = pd.concat([conv_all, conv], ignore_index=True)

    if camp_all.empty:
        res_df = pd.merge(stats, accounts, on='accountId', how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)
    else:
        res_df = pd.merge(camp_all, accounts, on='accountId', how='left')
        res_df = pd.merge(stats, res_df, on=['accountId','campaignId'], how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)

    res_df = res_df.rename(columns={'campaignId':'CampaignId'})
    res_df = res_df.rename(columns={'name':'CampaignName'})
    res_df = res_df.rename(columns={'date':'Date'})
    res_df = res_df.rename(columns={'impressions':'Impressions'})
    res_df = res_df.rename(columns={'clicks':'Clicks'})
    res_df = res_df.rename(columns={'cost':'Cost'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df['TrafficType'] = 'Таргет'

    return(res_df)

def click_ga(start,end,uid,tkn):
    # список аккаунтов
    url_api = 'https://api.click.ru/V0/accounts'
    headers = {
        'X-Auth-Token': tkn,
        'accept': 'application/json',
        'X-Auth-UserId': uid
    }

    data = requests.get(url_api, headers=headers).json()
    accounts = pd.json_normalize(data)
    accounts = accounts.explode('response.accounts').reset_index(drop=True)
    accounts = pd.json_normalize(accounts['response.accounts'])
    accounts = accounts.rename(columns={'id':'accountId', 'name':'account_name'})
    accounts = accounts[['accountId','account_name','service']]

    accounts = accounts.loc[accounts['service'] == 'ADWORDS']

    acc_list = accounts['accountId'].tolist()
    # список кампаний
    camp_all = pd.DataFrame()

    for i in acc_list:
        url_api = 'https://api.click.ru/V0/campaigns?accountId=' + str(i) + '&showDeleted=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'application/json',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).json()
        campaigns = pd.json_normalize(data)
        campaigns = campaigns.explode('response.items').reset_index(drop=True)
        campaigns = pd.json_normalize(campaigns['response.items'])
        campaigns = campaigns.rename(columns={'serviceId':'campaignId'})
        if campaigns.empty:
            pass
        else:
            campaigns = campaigns[['state', 'campaignId', 'accountId', 'name', 'isDeleted']]
            campaigns = campaigns.groupby('campaignId')['accountId','name','state'].first().reset_index()
            camp_all = pd.concat([camp_all, campaigns], ignore_index=True)

    # stats
    stats = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,date,impressions,clicks,cost,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }
        data = requests.get(url_api, headers=headers).content
        stats_part = pd.read_csv(io.StringIO(data.decode('utf-8')))
        stats = pd.concat([stats, stats_part], ignore_index=True)

    # conversions
    conv_all = pd.DataFrame()
    for i in acc_list:
        url_api = 'https://api.click.ru/V0/stat/v2?fields=' + 'accountId,campaignId,conversionName,date,conversions,service,adUrl' + '&dateFrom=' + start + '&dateTo=' + end + '&accountIds=' + str(i) + '&includeHeaders=true'
        headers = {
            'X-Auth-Token': tkn,
            'accept': 'text/csv',
            'X-Auth-UserId': uid
        }

        data = requests.get(url_api, headers=headers).content
        conv = pd.read_csv(io.StringIO(data.decode('utf-8')))
        conv = conv.pivot_table(values='conversions', index=['date','accountId','campaignId'], columns=['conversionName'], aggfunc=sum)
        conv = conv.reset_index()
        conv['new_index'] = conv[['date','accountId','campaignId']].apply(lambda x: '|'.join(x.astype(str)), axis=1)
        conv = conv.drop('new_index', axis=1)
        conv = conv.fillna(value=0)
        conv_all = pd.concat([conv_all, conv], ignore_index=True)

    if camp_all.empty:
        res_df = pd.merge(stats, accounts, on='accountId', how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)
    else:
        res_df = pd.merge(camp_all, accounts, on='accountId', how='left')
        res_df = pd.merge(stats, res_df, on=['accountId','campaignId'], how='left')
        res_df = pd.merge(res_df, conv_all, on=['accountId','date','campaignId'], how='left')
        res_df = res_df.fillna(value=0)

    res_df = res_df.rename(columns={'campaignId':'CampaignId'})
    res_df = res_df.rename(columns={'name':'CampaignName'})
    res_df = res_df.rename(columns={'date':'Date'})
    res_df = res_df.rename(columns={'impressions':'Impressions'})
    res_df = res_df.rename(columns={'clicks':'Clicks'})
    res_df = res_df.rename(columns={'cost':'Cost'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df = res_df.rename(columns={'account_name':'account'})
    res_df['TrafficType'] = 'Контекст'

    return(res_df)

def usd_to_rub(start,end,frame):
    dateFromCurr = start.split("-")
    day = dateFromCurr[2]
    month = dateFromCurr[1]
    year = dateFromCurr[0]
    dateFromCurr = str(day) + "/" + str(month) + "/" + str(year)

    dateToCurr = end.split("-")
    day = dateToCurr[2]
    month = dateToCurr[1]
    year = dateToCurr[0]
    dateToCurr = str(day) + "/" + str(month) + "/" + str(year)

    url = 'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=' + dateFromCurr +'&date_req2=' + dateToCurr + '&VAL_NM_RQ=R01235'
    curr = pd.read_xml(url)

    curr['Date'] = curr['Date'].apply(lambda x: datetime.strptime(x, '%d.%m.%Y').strftime('%Y-%m-%d'))
    res_df = pd.merge(frame, curr, on='Date', how='left')

    res_df['Value'] = res_df['Value'].fillna(method='ffill')
    res_df['Value'] = res_df['Value'].str.replace(',', '.').astype(float)
    res_df['Value'] = res_df['Value'].astype(float)
    res_df['Cost'] = res_df['Value'] * res_df['Cost']

    res_df = res_df.drop('Value', axis=1)
    res_df = res_df.drop('Nominal', axis=1)
    res_df = res_df.drop('Id', axis=1)

    return(res_df)
