import pandas as pd
from tapi_yandex_metrika import YandexMetrikaStats
from tapi_yandex_metrika import YandexMetrikaLogsapi
import datetime as dt  # для получения сегодняшней/вчерашней даты
import math  # для округления к большему (используется для подсчета кол-ва итераций в функции получения статистики из ЯМ)
import numpy as np
from ast import literal_eval


def convert_goals_ad(goals):
    goals_ym = []
    for i in goals:
        name = "ym:ad:goal" + i + "reaches"
        goals_ym.append(name)
    goals_ym = ','.join(goals_ym)
    return(goals_ym)

def convert_goals_s(goals):
    goals_ym = []
    for i in goals:
        name = "ym:s:goal" + i + "reaches"
        goals_ym.append(name)
    goals_ym = ','.join(goals_ym)
    return(goals_ym)


def unpack(df, column, fillna=None):
    ret = None
    if fillna is None:
        ret = pd.concat([df, pd.DataFrame((d for idx, d in df[column].iteritems()))], axis=1)
        del ret[column]
    else:
        ret = pd.concat([df, pd.DataFrame((d for idx, d in df[column].iteritems())).fillna(fillna)], axis=1)
        del ret[column]
    return ret

def get_ym_stat(counter, clientLogin, token_ym, _date1, _date2, _metrics, _dimensions):
    client = YandexMetrikaStats(access_token=token_ym)
    limit=100000
    params = dict(
        ids=counter,
        date1=_date1,
        date2=_date2,  # dt.date.today()
        metrics=_metrics,
        dimensions=_dimensions,
        lang="ru",
        limit=100000,
        accuracy='full',
        direct_client_logins=clientLogin
        # другие параметры -> https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
        )
    res_df = client.stats().get(params=params)

    count_iter = math.ceil(res_df['total_rows'] / limit)

    print('Необходимое кол-во итераций:', count_iter)
    if count_iter == 1:
        res_df = pd.DataFrame(res_df().to_dicts())
        print('Данные полностью скачены.')
    else:
        k = 0
        rep = res_df
        res_df = pd.DataFrame()
    
        for i in range(1, math.ceil(rep['total_rows'] / limit)+1):
            params = dict(
                ids=counter,
                date1=_date1,
                date2=_date2,  # dt.date.today()
                metrics=_metrics,
                dimensions=_dimensions,
                lang="ru",
                limit=100000,
                accuracy='full',
                direct_client_logins=clientLogin,
                offset=1+k)
    
            print(f'....качаю с строки #{k}, итерация #{i}')
            k += limit
        
            report = client.stats().get(params=params)
    
            res_df = pd.concat([res_df, pd.DataFrame(report().to_dicts())])
    
    columns = res_df.columns.to_list()

    for i in columns:
        if 'reaches' in i:
            res_df = res_df.astype({i: int})
        if 'DirectID' in i:
            res_df[i] = res_df[i].map(lambda x: x.lstrip('N-'))
        if 'ad:clicks' in i:
            res_df = res_df.astype({i: int})
        if 'AdCost' in i:
            res_df = res_df.astype({i: float})

    res_df.rename(columns=lambda x: x.strip().replace(":", "_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("ym_ad_goal", "Conversions_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("reaches", "_LSC"), inplace=True)
    if 'ym_ad_clicks' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_clicks':'Clicks'})
    if 'ym_ad_RUBConvertedAdCost' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_RUBConvertedAdCost':'Cost'})
    if 'ym_ad_date' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_date':'Date'})
    if 'ym_ad_cross_device_last_significantDirectID' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectID':'CampaignId'})
    if 'ym_ad_cross_device_last_significantDirectOrder' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectOrder':'CampaignName'})
    if 'ym_ad_region' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_region':'LocationOfPresenceName'})
    if 'ym_ad_bounceRate' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_bounceRate':'BounceRate'})
    res_df['Login'] = str(clientLogin)
    return res_df

def get_merch_stat(counter, clientLogin, token_ym, _date1, _date2, _metrics, _dimensions):
    client = YandexMetrikaStats(access_token=token_ym)
    limit=100000
    params = dict(
        ids=counter,
        date1=_date1,
        date2=_date2,  # dt.date.today()
        metrics=_metrics,
        dimensions=_dimensions,
        lang="ru",
        limit=100000,
        accuracy='full',
        direct_client_logins=clientLogin
        # другие параметры -> https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
        )
    res_df = client.stats().get(params=params)

    count_iter = math.ceil(res_df['total_rows'] / limit)

    print('Необходимое кол-во итераций:', count_iter)
    if count_iter == 1:
        res_df = pd.DataFrame(res_df().to_dicts())
        print('Данные полностью скачены.')
    else:
        k = 0
        rep = res_df
        res_df = pd.DataFrame()
    
        for i in range(1, math.ceil(rep['total_rows'] / limit)+1):
            params = dict(
                ids=counter,
                date1=_date1,
                date2=_date2,  # dt.date.today()
                metrics=_metrics,
                dimensions=_dimensions,
                lang="ru",
                limit=100000,
                accuracy='full',
                direct_client_logins=clientLogin,
                offset=1+k)
    
            print(f'....качаю с строки #{k}, итерация #{i}')
            k += limit
        
            report = client.stats().get(params=params)
    
            res_df = pd.concat([res_df, pd.DataFrame(report().to_dicts())])
    
    columns = res_df.columns.to_list()

    for i in columns:
        if 'reaches' in i:
            res_df = res_df.astype({i: int})
        if 'DirectID' in i:
            res_df[i] = res_df[i].map(lambda x: x.lstrip('N-'))
        if 'ad:clicks' in i:
            res_df = res_df.astype({i: int})
        if 'AdCost' in i:
            res_df = res_df.astype({i: float})

    res_df.rename(columns=lambda x: x.strip().replace(":", "_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("ym_ad_goal", "Conversions_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("reaches", "_LSC"), inplace=True)

    if 'ym_ad_clicks' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_clicks':'Clicks'})
    if 'ym_ad_RUBConvertedAdCost' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_RUBConvertedAdCost':'Cost'})
    if 'ym_ad_date' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_date':'Date'})
    if 'ym_ad_cross_device_last_significantDirectID' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectID':'CampaignId'})
    if 'ym_ad_cross_device_last_significantDirectOrder' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectOrder':'CampaignName'})
    if 'ym_ad_region' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_region':'LocationOfPresenceName'})
    if 'ym_ad_bounceRate' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_bounceRate':'BounceRate'})
    
    res_df['Login'] = str(clientLogin)
    res_df = res_df[(res_df['CampaignName'].str.contains("орговая")) | (res_df['CampaignName'].str.contains("оварная"))]
    return res_df

def get_logs_stat(counter, token_ym, _fields, _date1, _date2):
    client = YandexMetrikaLogsapi(
    access_token=token_ym,
    default_url_params={'counterId': counter},
    wait_report=True
    )
    params = {
    "fields": _fields,
    "source": "visits",
    "date1": _date1,
    "date2": _date2
    }
    info = client.create().post(params=params)
    request_id = info["log_request"]["request_id"]
    report = client.download(requestId=request_id).get()
    res_df = pd.DataFrame(report().to_dicts())
    res_df.rename(columns=lambda x: x.strip().replace(":", "_"), inplace=True)
    return res_df


# для форматирования времени
def to_time_str(df):
    time_int = round(df['Время на сайте'])
    return str(dt.timedelta(seconds=time_int))

# первая строка = заголовок
def row_to_header(df):
    df.columns = df.iloc[0]
    df = df[1:]
    return df

def get_ym_stat_filter(counter, clientLogin, token_ym, _date1, _date2, _metrics, _dimensions, _filter):
    client = YandexMetrikaStats(access_token=token_ym)
    limit=100000
    params = dict(
        ids=counter,
        date1=_date1,
        date2=_date2,  # dt.date.today()
        metrics=_metrics,
        filters=_filter,
        dimensions=_dimensions,
        lang="ru",
        limit=100000,
        accuracy='full',
        direct_client_logins=clientLogin
        # другие параметры -> https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
        )
    res_df = client.stats().get(params=params)

    count_iter = math.ceil(res_df['total_rows'] / limit)

    print('Необходимое кол-во итераций:', count_iter)
    if count_iter == 1:
        res_df = pd.DataFrame(res_df().to_dicts())
        print('Данные полностью скачены.')
    else:
        k = 0
        rep = res_df
        res_df = pd.DataFrame()
    
        for i in range(1, math.ceil(rep['total_rows'] / limit)+1):
            params = dict(
                ids=counter,
                date1=_date1,
                date2=_date2,  # dt.date.today()
                metrics=_metrics,
                filters=_filter,
                dimensions=_dimensions,
                lang="ru",
                limit=100000,
                accuracy='full',
                direct_client_logins=clientLogin,
                offset=1+k)
    
            print(f'....качаю с строки #{k}, итерация #{i}')
            k += limit
        
            report = client.stats().get(params=params)
    
            res_df = pd.concat([res_df, pd.DataFrame(report().to_dicts())])
    
    columns = res_df.columns.to_list()

    for i in columns:
        if 'reaches' in i:
            res_df = res_df.astype({i: int})
        if 'DirectID' in i:
            res_df[i] = res_df[i].map(lambda x: x.lstrip('N-'))
        if 'ad:clicks' in i:
            res_df = res_df.astype({i: int})
        if 'AdCost' in i:
            res_df = res_df.astype({i: float})

    res_df.rename(columns=lambda x: x.strip().replace(":", "_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("ym_ad_goal", "Conversions_"), inplace=True)
    res_df.rename(columns=lambda x: x.strip().replace("reaches", "_LSC"), inplace=True)
    if 'ym_ad_clicks' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_clicks':'Clicks'})
    if 'ym_ad_RUBConvertedAdCost' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_RUBConvertedAdCost':'Cost'})
    if 'ym_ad_date' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_date':'Date'})
    if 'ym_ad_cross_device_last_significantDirectID' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectID':'CampaignId'})
    if 'ym_ad_cross_device_last_significantDirectOrder' in res_df.columns:
        res_df = res_df.rename(columns={'ym_ad_cross_device_last_significantDirectOrder':'CampaignName'})
    if 'ym_ad_region' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_region':'LocationOfPresenceName'})
    if 'ym_ad_bounceRate' in res_df.columns:
        res_df  = res_df.rename(columns={'ym_ad_bounceRate':'BounceRate'})
    res_df['Login'] = str(clientLogin)
    return res_df

def associate_conv_camp(_counter,_token,_date1,_date2,_goals):
    res_df_assists_raw = pd.DataFrame()

    # camp associate
    fields = "ym:s:date,ym:s:lastTrafficSource,ym:s:visitID,ym:s:clientID,ym:s:lastDirectClickOrder,ym:s:goalsID,ym:s:UTMCampaign,ym:s:UTMSource,ym:s:UTMMedium,ym:s:UTMContent"
    dataset = get_logs_stat(
        _counter,
        _token,
        fields,
        _date1,
        _date2
    )

    for i in _goals:
        dataset['Conversion_' + i] = np.where(dataset['ym_s_goalsID'].str.contains(i), 1, 0)
        print(i, ' - done')

    res_df_assists_raw = pd.concat([res_df_assists_raw, dataset], ignore_index=True)

    goal_id = res_df_assists_raw.columns.tolist()
    goal_id = list(filter(lambda x: 'Conversion_' in x, goal_id))

    ass_camp = pd.DataFrame()

    for i in goal_id:
        count_session = res_df_assists_raw.groupby('ym_s_clientID').agg(
            session = ('ym_s_clientID', 'count'),
            goal = (i, 'sum')
            ).reset_index()

        conversion_date = res_df_assists_raw[res_df_assists_raw[i] == 1].groupby('ym_s_clientID').agg(
            client_id=('ym_s_clientID', 'first'),
            conversion_date=('ym_s_date', 'max')
            ).reset_index(drop=True)

        merged = pd.merge(res_df_assists_raw, count_session, on='ym_s_clientID', how='left')
        merged = merged.rename(columns={'ym_s_clientID':'client_id'})
        merged = pd.merge(merged, conversion_date, on='client_id', how='left')
        merged['associated_conversion'] = merged[i].apply(lambda x: 0 if x == 1 else 1)
        merged['atr_window'] = (pd.to_datetime(merged['conversion_date']) - pd.to_datetime(merged['ym_s_date'])).dt.days

        filtered = merged.loc[(merged['goal'] > 0) & (merged[i] == 0) & 
                            (merged['ym_s_UTMMedium'] != '') & (merged['session'] > 1) & 
                            (merged['atr_window'] >= 0)]
        
        filtered['utm_campaign'] = filtered['ym_s_lastDirectClickOrder'].where(filtered['ym_s_lastDirectClickOrder'] != '', filtered['ym_s_UTMCampaign'])
    
        filtered = filtered.rename(columns={'ym_s_date':'date',
                                            'ym_s_UTMSource':'utm_source',
                                            'ym_s_UTMMedium':'utm_medium'})

        final = filtered
        result = final.groupby(['date', 'utm_source', 'utm_medium', 'utm_campaign']).agg({'associated_conversion': 'sum'}).reset_index()
        result = result.sort_values('date', ascending=False)

        if len(result) > 0:
            result[f'{i}_ASS'] = result['associated_conversion']
            ass_camp = pd.concat([ass_camp, result], ignore_index=True)

        if ass_camp.empty:
            ass_camp = pd.DataFrame(columns=['date','utm_medium','utm_source','utm_campaign','tag'])
        else:
            ass_camp = ass_camp.groupby(['date', 'utm_medium', 'utm_source', 'utm_campaign']).sum().reset_index()
            ass_camp.drop('associated_conversion', axis=1, inplace=True)
    return(ass_camp)

def associate_conv_sour(_counter,_token,_date1,_date2,_goals):
    res_df_assists_source = pd.DataFrame()

    # source associate

    fields = "ym:s:date,ym:s:lastTrafficSource,ym:s:lastAdvEngine,ym:s:lastReferalSource,ym:s:lastSearchEngineRoot,ym:s:lastSocialNetwork,ym:s:referer,ym:s:visitID,ym:s:clientID,ym:s:goalsID"
    dataset = get_logs_stat(
        _counter,
        _token,
        fields,
        _date1,
        _date2
    )

    for i in _goals:
        dataset['Conversion_' + i] = np.where(dataset['ym_s_goalsID'].str.contains(i), 1, 0)
        print(i, ' - done')

    res_df_assists_source = pd.concat([res_df_assists_source, dataset], ignore_index=True)

    goal_id = res_df_assists_source.columns.tolist()
    goal_id = list(filter(lambda x: 'Conversion_' in x, goal_id))

    ass_source = pd.DataFrame()
    ass_adv_source = pd.DataFrame()
    ad = pd.DataFrame()
    direct = pd.DataFrame()
    internal = pd.DataFrame()
    messenger = pd.DataFrame()
    organic = pd.DataFrame()
    recommend = pd.DataFrame()
    referral = pd.DataFrame()
    saved = pd.DataFrame()
    social = pd.DataFrame()

    # ad
    ad = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'ad')]
    ad = ad[['ym_s_date','ym_s_lastTrafficSource','ym_s_lastAdvEngine','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    ad = ad.rename(columns={'ym_s_lastAdvEngine':'ym_s_source2'})

    # direct
    direct = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'direct')]
    direct = direct[['ym_s_date','ym_s_lastTrafficSource','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]

    # internal
    internal = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'internal')]
    internal = internal[['ym_s_date','ym_s_lastTrafficSource','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]

    # messenger
    messenger = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'messenger')]
    messenger = messenger[['ym_s_date','ym_s_lastTrafficSource','ym_s_referer','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    messenger = messenger.rename(columns={'ym_s_referer':'ym_s_source2'})

    # organic
    organic = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'organic')]
    organic = organic[['ym_s_date','ym_s_lastTrafficSource','ym_s_lastSearchEngineRoot','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    organic = organic.rename(columns={'ym_s_lastSearchEngineRoot':'ym_s_source2'})

    # recommend
    recommend = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'recommend')]
    recommend = recommend[['ym_s_date','ym_s_lastTrafficSource','ym_s_lastReferalSource','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    recommend = recommend.rename(columns={'ym_s_lastReferalSource':'ym_s_source2'})

    # referral
    referral = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'referral')]
    referral = referral[['ym_s_date','ym_s_lastTrafficSource','ym_s_lastReferalSource','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    referral = referral.rename(columns={'ym_s_lastReferalSource':'ym_s_source2'})

    # social
    social = res_df_assists_source.loc[(res_df_assists_source['ym_s_lastTrafficSource'] == 'social')]
    social = social[['ym_s_date','ym_s_lastTrafficSource','ym_s_lastSocialNetwork','ym_s_visitID','ym_s_clientID','ym_s_goalsID']]
    social = social.rename(columns={'ym_s_lastSocialNetwork':'ym_s_source2'})

    ass_adv_source = pd.DataFrame()
    ass_adv_source = pd.concat([ass_adv_source, ad], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, direct], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, internal], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, messenger], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, organic], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, recommend], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, referral], ignore_index=True)
    ass_adv_source = pd.concat([ass_adv_source, social], ignore_index=True)

    for i in _goals:
        ass_adv_source['Conversion_' + i] = np.where(ass_adv_source['ym_s_goalsID'].str.contains(i), 1, 0)
        print(i, ' - done')

    for i in goal_id:
        count_session = ass_adv_source.groupby('ym_s_clientID').agg(
        session = ('ym_s_clientID', 'count'),
        goal = (i, 'sum')
        ).reset_index()

        conversion_date = ass_adv_source[ass_adv_source[i] == 1].groupby('ym_s_clientID').agg(
        client_id=('ym_s_clientID', 'first'),
        conversion_date=('ym_s_date', 'max')
        ).reset_index(drop=True)

        merged = pd.merge(ass_adv_source, count_session, on='ym_s_clientID', how='left')
        merged = merged.rename(columns={'ym_s_clientID':'client_id'})
        merged = pd.merge(merged, conversion_date, on='client_id', how='left')
        merged['associated_conversion'] = merged[i].apply(lambda x: 0 if x == 1 else 1)
        merged['atr_window'] = (pd.to_datetime(merged['conversion_date']) - pd.to_datetime(merged['ym_s_date'])).dt.days

        filtered = merged.loc[(merged['goal'] > 0) & (merged[i] == 0) & 
                            (merged['session'] > 1) & 
                            (merged['atr_window'] >= 0)]
        
        filtered = filtered.rename(columns={'ym_s_date':'date',
                                            'ym_s_lastTrafficSource':'lastTrafficSource',
                                            'ym_s_source2':'source2'})

        final = filtered
        result = final.groupby(['date', 'lastTrafficSource', 'source2']).agg({'associated_conversion': 'sum'}).reset_index()
        result = result.sort_values('date', ascending=False)

        if len(result) > 0:
            result[f'{i}_ASS'] = result['associated_conversion']
            ass_source = pd.concat([ass_source, result], ignore_index=True)

        if ass_source.empty:
            ass_source = pd.DataFrame(columns=['date','lastTrafficSource','source2','tag'])
        else:
            ass_source = ass_source.groupby(['date', 'lastTrafficSource', 'source2']).sum().reset_index()
            ass_source.drop('associated_conversion', axis=1, inplace=True)

    return(ass_source)