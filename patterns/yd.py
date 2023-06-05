from tapi_yandex_direct import YandexDirect
import pandas as pd
import random
import numpy as np
import itertools
from ast import literal_eval


from datetime import date

def input_data(token, clientLogin, goals, start_date, end_date, mode, user_fields):
    client = YandexDirect(
    # Required parameters:
    access_token=token,
    # If you are making inquiries from an agent account, you must be sure to specify the account login.
    login=clientLogin,
    # Optional parameters:
    # Enable sandbox.
    is_sandbox=False,
    # Repeat request when units run out
    retry_if_not_enough_units=False,
    # The language in which the data for directories and errors will be returned.
    language="ru",
    # Repeat the request if the limits on the number of reports or requests are exceeded.
    retry_if_exceeded_limit=True,
    # Number of retries when server errors occur.
    retries_if_server_error=5,
    # When requesting a report, it will wait until the report is prepared and download it.
    wait_report=True,
    # Monetary values in the report are returned in currency with an accuracy of two decimal places.
    return_money_in_micros=False,
    # Do not display a line with the report name and date range in the report.
    skip_report_header=True,
    # Do not display a line with field names in the report.
    skip_column_header=False,
    # Do not display a line with the number of statistics lines in the report.
    skip_report_summary=True,
    )
    report_name = str(random.random())

    dates = pd.date_range(
        min(start_date, end_date),
        max(start_date, end_date)
            ).strftime('%Y-%m-%d').tolist()
    
    conversions = goals
    fields = user_fields

    return(client,dates,conversions,fields)

def yd_stat_all(start_date, end_date, client, conversions, fields):
    count_iter = 0
    df = pd.DataFrame()
    if conversions == "":
        report_name = str(random.random())
        body = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                        "DateFrom": start_date,
                        "DateTo": end_date
                        },
            "ReportName": report_name,
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO",
            "FieldNames": fields
            },
        }
        report = client.reports().post(data=body)
        df = pd.concat([df, pd.DataFrame(report().to_dicts())])
    else: 
        if len(conversions) <= 10:
            report_name = str(random.random())
            body = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                            "DateFrom": start_date,
                            "DateTo": end_date
                            },
                "Goals": conversions,
                "AttributionModels": ["LSC"],
                "ReportName": report_name,
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO",
                "FieldNames": fields
                },
            }
            report = client.reports().post(data=body)
            df = pd.concat([df, pd.DataFrame(report().to_dicts())])
        else:
            list_goals = list(itertools.zip_longest(*[iter(conversions)]*10))
            for i in list_goals:
                goals = list(i) # берем первый набор целей из списка и превращаем его из кортежа в список
                count_iter += 1
                if count_iter == 1:
                    goals = [x for x in goals if x is not None]
                    print(goals)
                    report_name = str(random.random())
                    body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                                    "DateFrom": start_date,
                                    "DateTo": end_date
                                    },
                        "Goals": goals,
                        "AttributionModels": ["LSC"],
                        "ReportName": report_name,
                        "ReportType": "CUSTOM_REPORT",
                        "DateRangeType": "CUSTOM_DATE",
                        "Format": "TSV",
                        "IncludeVAT": "NO",
                        "IncludeDiscount": "NO",
                        "FieldNames": fields
                        },
                    }
                    report = client.reports().post(data=body)
                    df = pd.concat([pd.DataFrame(report().to_dicts()), df])
                else:
                    goals = [x for x in goals if x is not None]
                    print(goals)
                    report_name = str(random.random())
                    fields = [x for x in fields if x != "Impressions"]
                    fields = [x for x in fields if x != "Clicks"]
                    fields = [x for x in fields if x != "Cost"]
                    fields = [x for x in fields if x != "AvgImpressionPosition"]
                    fields = [x for x in fields if x != "BounceRate"]
                    body = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                                    "DateFrom": start_date,
                                    "DateTo": end_date
                                    },
                        "Goals": goals,
                        "AttributionModels": ["LSC"],
                        "ReportName": report_name,
                        "ReportType": "CUSTOM_REPORT",
                        "DateRangeType": "CUSTOM_DATE",
                        "Format": "TSV",
                        "IncludeVAT": "NO",
                        "IncludeDiscount": "NO",
                        "FieldNames": fields
                        },
                    }
                    report = client.reports().post(data=body)
                    df = pd.concat([pd.DataFrame(report().to_dicts()),df])
    return(df)

def get_data_yd(token, clientLogin, goals, start_date, end_date, mode, user_fields):
    (client,dates,conversions,fields) = input_data(token, clientLogin, goals, start_date, end_date, mode, user_fields)
    first = 0
    last = 0
    count = 0
    df_part = pd.DataFrame()
    res_df = pd.DataFrame()
    for i in dates:
        report_name = str(random.random()) #каждый раз делаем рандомное имя отчета чтобы директ не послал нас к хуям
        objects = mode #количество дней на один запрос
        count += 1
        count_days = len(dates) - 1
        if count == 1: #проверка, первая ли итерация (чтобы взять нулевой объект списка дат)
            first = 0
            last = objects - 1
        else:
            first += objects
            last += objects

        if first <= count_days:
            if first < count_days and last >= count_days:
                start = dates[first]
                end = dates[-1]
                period = str(start) + ' ' + str(end)
                print(period)
                df_part = yd_stat_all(start,end,client,conversions,fields)
                res_df = pd.concat([df_part, res_df], ignore_index=True)
                del df_part
            elif first == count_days and last > count_days:
                start = dates[first]
                end = start
                period = str(start) + ' ' + str(end)
                print(period)
                df_part = yd_stat_all(start,end,client,conversions,fields)
                res_df = pd.concat([df_part, res_df], ignore_index=True)
                del df_part
            else:
                start = dates[first]
                end = dates[last]
                period = str(start) + ' ' + str(end)
                print(period)
                df_part = yd_stat_all(start,end,client,conversions,fields)
                res_df = pd.concat([df_part, res_df], ignore_index=True)
                del df_part
        else:
            print("Запрошенных дат меньше, чем дней в 1 итерации или все данные получены")
            #res_df = res_df.sort_values('Date')
            res_df = res_df.reset_index(drop=True)
            break

    #res_df = res_df.replace('--','0')
    columns = res_df.columns.to_list()
    for i in columns:
        if 'AvgClickPosition' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgClickPosition': float})
        if 'AvgCpc' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgCpc': float})
        if 'AvgCpm' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgCpm': float})
        if 'AvgEffectiveBid' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgEffectiveBid': float})
        if 'AvgImpressionFrequency' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgImpressionFrequency': float})
        if 'AvgImpressionPosition' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgImpressionPosition': float})
        if 'AvgPageviews' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgPageviews': float})
        if 'AvgTrafficVolume' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({'AvgTrafficVolume': float})
        if 'Cost' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({'Cost': float})
        if 'Clicks' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({'Clicks': int})
        if 'Impressions' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({'Impressions': int})
        if 'Conversions' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({i: int})
        if 'Revenue' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({i: float})
        if 'Bounces' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', 0)
            res_df = res_df.astype({i: int})
        if 'BounceRate' in i:
            res_df[i] = res_df[i].fillna('--')
            res_df[i] = res_df[i].replace('--', np.nan)
            res_df = res_df.astype({i: float})

    #res_df['Cost'] = res_df['Cost'].replace('.',',')
    #res_df = res_df.astype({"CampaignId": int},{"AdGroupId": int})
    #print(res_df.dtypes)
    res_df['Login'] = str(clientLogin)

    return(res_df)

def get_campaigns(token, clientLogin):
    client = YandexDirect(
    # Required parameters:
    access_token=token,
    # If you are making inquiries from an agent account, you must be sure to specify the account login.
    login=clientLogin,
    is_sandbox=False,
    # Repeat request when units run out
    retry_if_not_enough_units=False,
    # The language in which the data for directories and errors will be returned.
    language="ru",
    # Repeat the request if the limits on the number of reports or requests are exceeded.
    retry_if_exceeded_limit=True,
    # Number of retries when server errors occur.
    retries_if_server_error=5,
    # Report generation mode: online, offline or auto.
    processing_mode="offline",
    # When requesting a report, it will wait until the report is prepared and download it.
    wait_report=True,
    # Monetary values in the report are returned in currency with an accuracy of two decimal places.
    return_money_in_micros=False,
    # Do not display a line with the report name and date range in the report.
    skip_report_header=True,
    # Do not display a line with field names in the report.
    skip_column_header=False,
    # Do not display a line with the number of statistics lines in the report.
    skip_report_summary=True,
    )
    body = {
    "method": "get",
    "params": {
        "SelectionCriteria": {},
        "FieldNames": ["Id","Name","State"],
        "TextCampaignFieldNames": ['PriorityGoals'],
        "DynamicTextCampaignFieldNames": ['PriorityGoals'],
        "SmartCampaignFieldNames": ['PriorityGoals']
        },
    }
    report = client.campaigns().post(data=body)
    data = report().extract()
    df = pd.DataFrame()
    df = pd.json_normalize(data)
    df['Login'] = str(clientLogin)
    print(clientLogin)
    return(df)

def get_ads(token, clientLogin):
    client = YandexDirect(
    # Required parameters:
    access_token=token,
    # If you are making inquiries from an agent account, you must be sure to specify the account login.
    login=clientLogin,
    is_sandbox=False,
    # Repeat request when units run out
    retry_if_not_enough_units=False,
    # The language in which the data for directories and errors will be returned.
    language="ru",
    # Repeat the request if the limits on the number of reports or requests are exceeded.
    retry_if_exceeded_limit=True,
    # Number of retries when server errors occur.
    retries_if_server_error=5,
    # Report generation mode: online, offline or auto.
    processing_mode="offline",
    # When requesting a report, it will wait until the report is prepared and download it.
    wait_report=True,
    # Monetary values in the report are returned in currency with an accuracy of two decimal places.
    return_money_in_micros=False,
    # Do not display a line with the report name and date range in the report.
    skip_report_header=True,
    # Do not display a line with field names in the report.
    skip_column_header=False,
    # Do not display a line with the number of statistics lines in the report.
    skip_report_summary=True,
    )

    #Получаем список рекламных кампаний
    body = {
    "method": "get",
    "params": {
        "SelectionCriteria": {},
        "FieldNames": ["Id"]
        },
    }

    list_campaign = []
    report = client.campaigns().post(data=body)
    data = report().extract()
    for campaign in data:
        list_campaign.append("{}".format((campaign['Id'])))

    def compare_list(a, b):
        return not set(a).isdisjoint(b)

    res_df = pd.DataFrame()
    for i in list_campaign:
        body = {
        "method": "get",
        "params": {
        "SelectionCriteria": {
            "CampaignIds": [i]
            },
        "FieldNames": ["CampaignId","AgeLabel","AdGroupId","Id","State","Status","StatusClarification","AgeLabel","Type","Subtype"],
        "TextAdFieldNames": ["AdImageHash","DisplayDomain","Href","SitelinkSetId","Text","Title","Title2","Mobile","VCardId","DisplayUrlPath","AdImageModeration","SitelinksModeration","VCardModeration","AdExtensions","DisplayUrlPathModeration","VideoExtension","TurboPageId","TurboPageModeration","BusinessId"],
        "TextAdPriceExtensionFieldNames": ["Price","OldPrice","PriceCurrency","PriceQualifier"],
        "MobileAppAdFieldNames": ["AdImageHash","AdImageModeration","Title","Text","Features","Action","TrackingUrl","VideoExtension"],
        "DynamicTextAdFieldNames": ["AdImageHash","SitelinkSetId","Text","VCardId","AdImageModeration","SitelinksModeration","VCardModeration","AdExtensions"],
        "MobileAppImageAdFieldNames": ["AdImageHash","TrackingUrl"],
        "MobileAppAdBuilderAdFieldNames": ["Creative","TrackingUrl"],
        "MobileAppCpcVideoAdBuilderAdFieldNames": ["Creative","TrackingUrl"],
        "SmartAdBuilderAdFieldNames": ["Creative"],
        "TextImageAdFieldNames": ["AdImageHash","Href","TurboPageId","TurboPageModeration"],
        "TextAdBuilderAdFieldNames": ["Creative","Href","TurboPageId","TurboPageModeration"],
        "CpcVideoAdBuilderAdFieldNames": ["Creative","Href","TurboPageId","TurboPageModeration"],
        "CpmBannerAdBuilderAdFieldNames": ["Creative","Href","TurboPageId","TurboPageModeration"],
        "CpmVideoAdBuilderAdFieldNames": ["Creative","Href","TurboPageId","TurboPageModeration"]
            },
        }

        report = client.ads().post(data=body)
        data = report().extract()
        df_check = pd.DataFrame(data)
        df = pd.DataFrame()
        df = pd.json_normalize(data)
        df_check = pd.json_normalize(data)
        columns = df.columns.to_list()

        # Обработка совпадающих параметров рекламных объявлений в разных типах рк
        # AdImageHash
        if compare_list(['TextAd.AdImageHash','DynamicTextAd.AdImageHash','MobileAppImageAd.AdImageHash','TextImageAd.AdImageHash'],columns)== True:
            df['AdImageHash'] = np.nan
            df['AdImageHash'] = df['AdImageHash'].fillna('')
        
        if 'TextAd.AdImageHash' in df.columns:
            df['TextAd.AdImageHash'] = df['TextAd.AdImageHash'].fillna('')
            df['AdImageHash'] = df['AdImageHash'].astype(str) + df['TextAd.AdImageHash'].astype(str)
            df = df.drop('TextAd.AdImageHash', axis=1)
        if 'DynamicTextAd.AdImageHash' in df.columns:
            df['DynamicTextAd.AdImageHash'] = df['DynamicTextAd.AdImageHash'].fillna('')
            df['AdImageHash'] = df['AdImageHash'].astype(str) + df['DynamicTextAd.AdImageHash'].astype(str)
            df = df.drop('DynamicTextAd.AdImageHash', axis=1)
        if 'MobileAppImageAd.AdImageHash' in df.columns:
            df['MobileAppImageAd.AdImageHash'] = df['MobileAppImageAd.AdImageHash'].fillna('')
            df['AdImageHash'] = df['AdImageHash'].astype(str) + df['MobileAppImageAd.AdImageHash'].astype(str)
            df = df.drop('MobileAppImageAd.AdImageHash', axis=1)
        if 'TextImageAd.AdImageHash' in df.columns:
            df['TextImageAd.AdImageHash'] = df['TextImageAd.AdImageHash'].fillna('')
            df['AdImageHash'] = df['AdImageHash'].astype(str) + df['TextImageAd.AdImageHash'].astype(str)
            df = df.drop('TextImageAd.AdImageHash', axis=1)

        # Title
        if compare_list(['TextAd.Title','MobileAppAd.Title'], columns) == True:
            df['Title'] = np.nan
            df['Title'] = df['Title'].fillna('')

        if 'TextAd.Title' in df.columns:
            df['TextAd.Title'] = df['TextAd.Title'].fillna('')
            df['Title'] = df['Title'].astype(str) + df['TextAd.Title'].astype(str)
            df = df.drop('TextAd.Title', axis=1)
        if 'MobileAppAd.Title' in df.columns:
            df['MobileAppAd.Title'] = df['MobileAppAd.Title'].fillna('')
            df['Title'] = df['Title'].astype(str) + df['MobileAppAd.Title'].astype(str)
            df = df.drop('MobileAppAd.Title', axis=1)
        
        # AdExtensions
        if compare_list(['TextAd.AdExtensions','DynamicTextAd.AdExtensions'], columns) == True:
            df['AdExtensions'] = np.nan
            df['AdExtensions'] = df['AdExtensions'].fillna('')

        if 'TextAd.AdExtensions' in df.columns:
            df['TextAd.AdExtensions'] = df['TextAd.AdExtensions'].fillna('')
            df['AdExtensions'] = df['AdExtensions'].astype(str) + df['TextAd.AdExtensions'].astype(str)
            df = df.drop('TextAd.AdExtensions', axis=1)
        if 'DynamicTextAd.AdExtensions' in df.columns:
            df['DynamicTextAd.AdExtensions'] = df['DynamicTextAd.AdExtensions'].fillna('')
            df['AdExtensions'] = df['AdExtensions'].astype(str) + df['DynamicTextAd.AdExtensions'].astype(str)
            df = df.drop('DynamicTextAd.AdExtensions', axis=1)

        # Creative
        if compare_list(['MobileAppAdBuilderAd.Creative','MobileAppCpcVideoAd.Creative','SmartAdBuilderAd.Creative','TextAdBuilderAd.Creative','CpcVideoAdBuilderAd.Creative','CpmBannerAdBuilderAd.Creative','CpmVideoAdBuilderAd.Creative'], columns) == True:
            df['Creative'] = np.nan
            df['Creative'] = df['Creative'].fillna('')

        if 'MobileAppAdBuilderAd.Creative' in df.columns:
            df['MobileAppAdBuilderAd.Creative'] = df['MobileAppAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['MobileAppAdBuilderAd.Creative'].astype(str)
            df = df.drop('MobileAppAdBuilderAd.Creative', axis=1)
        if 'MobileAppCpcVideoAd.Creative' in df.columns:
            df['MobileAppCpcVideoAd.Creative'] = df['MobileAppCpcVideoAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['MobileAppCpcVideoAd.Creative'].astype(str)
            df = df.drop('MobileAppCpcVideoAd.Creative', axis=1)
        if 'SmartAdBuilderAd.Creative' in df.columns:
            df['SmartAdBuilderAd.Creative'] = df['SmartAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['SmartAdBuilderAd.Creative'].astype(str)
            df = df.drop('SmartAdBuilderAd.Creative', axis=1)
        if 'TextAdBuilderAd.Creative' in df.columns:
            df['TextAdBuilderAd.Creative'] = df['TextAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['TextAdBuilderAd.Creative'].astype(str)
            df = df.drop('TextAdBuilderAd.Creative', axis=1)
        if 'CpcVideoAdBuilderAd.Creative' in df.columns:
            df['CpcVideoAdBuilderAd.Creative'] = df['CpcVideoAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['CpcVideoAdBuilderAd.Creative'].astype(str)
            df = df.drop('CpcVideoAdBuilderAd.Creative', axis=1)
        if 'CpmBannerAdBuilderAd.Creative' in df.columns:
            df['CpmBannerAdBuilderAd.Creative'] = df['CpmBannerAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['CpmBannerAdBuilderAd.Creative'].astype(str)
            df = df.drop('CpmBannerAdBuilderAd.Creative', axis=1)
        if 'CpmVideoAdBuilderAd.Creative' in df.columns:
            df['CpmVideoAdBuilderAd.Creative'] = df['CpmVideoAdBuilderAd.Creative'].fillna('')
            df['Creative'] = df['Creative'].astype(str) + df['CpmVideoAdBuilderAd.Creative'].astype(str)
            df = df.drop('CpmVideoAdBuilderAd.Creative', axis=1)
        
        # Href
        if compare_list(['TextImageAd.Href','TextAdBuilderAd.Href','CpcVideoAdBuilderAd.Href','CpmBannerAdBuilderAd.Href','CpmVideoAdBuilderAd.Href','TextAd.Href'], columns) == True:
            df['Href'] = np.nan
            df['Href'] = df['Href'].fillna('')

        if 'TextImageAd.Href' in df.columns:
            df['TextImageAd.Href'] = df['TextImageAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['TextImageAd.Href'].astype(str)
            df = df.drop('TextImageAd.Href', axis=1)
        if 'TextAdBuilderAd.Href' in df.columns:
            df['TextAdBuilderAd.Href'] = df['TextAdBuilderAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['TextAdBuilderAd.Href'].astype(str)
            df = df.drop('TextAdBuilderAd.Href', axis=1)
        if 'CpcVideoAdBuilderAd.Href' in df.columns:
            df['CpcVideoAdBuilderAd.Href'] = df['CpcVideoAdBuilderAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['CpcVideoAdBuilderAd.Href'].astype(str)
            df = df.drop('CpcVideoAdBuilderAd.Href', axis=1)
        if 'CpmBannerAdBuilderAd.Href' in df.columns:
            df['CpmBannerAdBuilderAd.Href'] = df['CpmBannerAdBuilderAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['CpmBannerAdBuilderAd.Href'].astype(str)
            df = df.drop('CpmBannerAdBuilderAd.Href', axis=1)
        if 'CpmVideoAdBuilderAd.Href' in df.columns:
            df['CpmVideoAdBuilderAd.Href'] = df['CpmVideoAdBuilderAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['CpmVideoAdBuilderAd.Href'].astype(str)
            df = df.drop('CpmVideoAdBuilderAd.Href', axis=1)
        if 'TextAd.Href' in df.columns:
            df['TextAd.Href'] = df['TextAd.Href'].fillna('')
            df['Href'] = df['Href'].astype(str) + df['TextAd.Href'].astype(str)
            df = df.drop('TextAd.Href', axis=1)

        # AdImageModeration
        if compare_list(['TextAd.AdImageModeration','MobileAppAd.AdImageModeration','DynamicTextAd.AdImageModeration'], columns) == True:
            df['AdImageModeration'] = np.nan
            df['AdImageModeration'] = df['AdImageModeration'].fillna('')

        if 'TextAd.AdImageModeration' in df.columns:
            df['TextAd.AdImageModeration'] = df['TextAd.AdImageModeration'].fillna('')
            df['AdImageModeration'] = df['AdImageModeration'].astype(str) + df['TextAd.AdImageModeration'].astype(str)
            df = df.drop('TextAd.AdImageModeration', axis=1)
        if 'MobileAppAd.AdImageModeration' in df.columns:
            df['MobileAppAd.AdImageModeration'] = df['MobileAppAd.AdImageModeration'].fillna('')
            df['AdImageModeration'] = df['AdImageModeration'].astype(str) + df['MobileAppAd.AdImageModeration'].astype(str)
            df = df.drop('MobileAppAd.AdImageModeration', axis=1)
        if 'DynamicTextAd.AdImageModeration' in df.columns:
            df['DynamicTextAd.AdImageModeration'] = df['DynamicTextAd.AdImageModeration'].fillna('')
            df['AdImageModeration'] = df['AdImageModeration'].astype(str) + df['DynamicTextAd.AdImageModeration'].astype(str)
            df = df.drop('DynamicTextAd.AdImageModeration', axis=1)

        # Text
        if compare_list(['TextAd.Text','MobileAppAd.Text','DynamicTextAd.Text'], columns) == True:
            df['Text'] = np.nan
            df['Text'] = df['Text'].fillna('')

        if 'TextAd.Text' in df.columns:
            df['TextAd.Text'] = df['TextAd.Text'].fillna('')
            df['Text'] = df['Text'].astype(str) + df['TextAd.Text'].astype(str)
            df = df.drop('TextAd.Text', axis=1)
        if 'MobileAppAd.Text' in df.columns:
            df['MobileAppAd.Text'] = df['MobileAppAd.Text'].fillna('')
            df['Text'] = df['Text'].astype(str) + df['MobileAppAd.Text'].astype(str)
            df = df.drop('MobileAppAd.Text', axis=1)
        if 'DynamicTextAd.Text' in df.columns:
            df['DynamicTextAd.Text'] = df['DynamicTextAd.Text'].fillna('')
            df['Text'] = df['Text'].astype(str) + df['DynamicTextAd.Text'].astype(str)
            df = df.drop('DynamicTextAd.Text', axis=1)

        df_test = df
        df = df_test.replace(r'^\s*$', "empty_cell", regex=True)
        df = df.rename(columns=lambda x: x.split('.')[-1]) # избавляемся от префиксов в названиях столбцов
        df = df.reset_index(drop=True) # сбрасываем индекс
        df = df.loc[:,~df.columns.duplicated()] # избавляемся от столбцов дубликатов

        if 'AdExtensions' in df.columns:
            col_one_list = df['AdExtensions'].tolist()
            col_one_list = list(set(col_one_list))
            if col_one_list == ['empty_cell']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]','empty_cell']:
                col_one_list[0] = 'null'
                col_one_list[1] = 'null'
                col_one_list = list(set(col_one_list))
            if col_one_list == ['empty_cell','[]']:
                col_one_list[0] = 'null'
                col_one_list[1] = 'null'
                col_one_list = list(set(col_one_list))
            else:
                col_one_list = [x for x in col_one_list if x != 'empty_cell']
                col_one_list = [item for item in col_one_list if not(pd.isnull(item)) == True]
                col_one_list = [str(n) for n in col_one_list]

            if col_one_list != ['null']:
                ext_full = df[['AdExtensions','Id']].copy()
                ext_full2 = df[['AdExtensions','Id']].copy()
                ext_full = ext_full.loc[ext_full['AdExtensions'] != 'empty_cell']
                ext_full['AdExtensions'] = ext_full["AdExtensions"].apply(lambda x: literal_eval(x))
                ext_full = pd.DataFrame(ext_full)
                ext_full = ext_full.explode('AdExtensions').reset_index(drop=True)
                ext_full = pd.concat([ext_full, ext_full.pop("AdExtensions").apply(pd.Series)], axis=1)
                ext_full = ext_full[ext_full['Type'].notna()]
                ext_list = ext_full.drop_duplicates('AdExtensionId')
                ext_list = ext_list[ext_list['Type'].notna()]
                ext_list = ext_list[['AdExtensionId']].copy()
                ext_list = ext_list.astype({"AdExtensionId": int})
                ext_list = ext_list['AdExtensionId'].to_list()
                body = {"method": "get",  # Используемый метод.
                                    "params": {"SelectionCriteria": {"Ids":ext_list},  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                                            "FieldNames": ["Id","Type","Status","StatusClarification","Associated"],
                                            "CalloutFieldNames": ["CalloutText"],  # Имена параметров, которые требуется получить.
                                            }}
                
                report = client.adextensions().post(data=body)
                data = report().extract()
                df_extensions = pd.DataFrame()
                df_extensions = pd.json_normalize(data)
                df_extensions = df_extensions.loc[:,~df_extensions.columns.duplicated()] # избавляемся от столбцов дубликатов
                df_extensions = df_extensions.rename(columns={'Id':'AdExtensionId'})
                df_extensions = df_extensions.rename(columns={'Status':'Status_AdExtension'})
                df_extensions = df_extensions.rename(columns={'StatusClarification':'SClarif_AdExtension'})
                df_extensions = pd.merge(ext_full, df_extensions, on='AdExtensionId')
                df_extensions = df_extensions.groupby(['Id'], as_index=False).agg({'Callout.CalloutText':'||'.join, 'Status_AdExtension':'||'.join, 'SClarif_AdExtension':'||'.join})
                df_extensions = pd.merge(df, df_extensions, how='outer', on='Id')
                df_extensions = df_extensions.drop('AdExtensions', axis=1)
            else:
                df_extensions = df
        else:
            df_extensions = df

        if 'SitelinkSetId' in df.columns:
            col_one_list = df['SitelinkSetId'].tolist()
            col_one_list = list(set(col_one_list))
            if col_one_list == ['empty_cell']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]','empty_cell']:
                col_one_list[0] = 'null'
                col_one_list[1] = 'null'
                col_one_list = list(set(col_one_list))
            else:
                col_one_list = [x for x in col_one_list if x != 'empty_cell']
                col_one_list= [item for item in col_one_list if not(pd.isnull(item)) == True]
                col_one_list = [int(n) for n in col_one_list]

            if col_one_list != []:
                body = {"method": "get",  # Используемый метод.
                        "params": {"SelectionCriteria": {"Ids":col_one_list},  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                                "FieldNames": ["Id"],
                                "SitelinkFieldNames": ["Title","Href","Description","TurboPageId"],  # Имена параметров, которые требуется получить.
                                }}

                report = client.sitelinks().post(data=body)
                data = report().extract()
                df_sitelinks = pd.DataFrame()
                df_sitelinks = pd.json_normalize(data,meta=['Id'],record_path=['Sitelinks'])
                df_sitelinks = df_sitelinks.rename(columns=lambda x: x.split('.')[-1])
                df_sitelinks = df_sitelinks.reset_index(drop=True)
                df_sitelinks = df_sitelinks.loc[:,~df_sitelinks.columns.duplicated()] # избавляемся от столбцов дубликатов
                df_sitelinks = df_sitelinks.rename(columns={'Id':'SitelinkSetId'})
                df_sitelinks = df_sitelinks.astype({"Description": str})
                df_sitelinks = df_sitelinks.astype({"Href": str})
                df_sitelinks = df_sitelinks.astype({"Title": str})
                df_sitelinks = df_sitelinks.groupby(['SitelinkSetId'], as_index=False).agg({'Title':'||'.join, 'Href':'||'.join, 'Description':'||'.join})
                df_sitelinks['SitelinkSetId'] = df_sitelinks['SitelinkSetId'].apply(str)
                df_sitelinks = df_sitelinks.rename(columns={'Title':'Title_Sitelink'})
                df_sitelinks = df_sitelinks.rename(columns={'Href':'Href_Sitelink'})
                df_sitelinks = df_sitelinks.rename(columns={'Description':'Description_Sitelink'})
                df_sitelinks = df_sitelinks.astype({"SitelinkSetId": int})
                df_sitelinks = pd.merge(df_extensions, df_sitelinks, how='outer', on='SitelinkSetId')
            else:
                df_sitelinks = df_extensions
        else:
            df_sitelinks = df_extensions

        if 'AdImageHash' in df.columns:
            col_one_list = df['AdImageHash'].tolist()
            col_one_list = list(set(col_one_list))
            if col_one_list == ['empty_cell']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]']:
                col_one_list[0] = 'null'
            if col_one_list == ['[]','empty_cell']:
                col_one_list[0] = 'null'
                col_one_list[1] = 'null'
                col_one_list = list(set(col_one_list))
            else:
                col_one_list = [x for x in col_one_list if x != 'empty_cell']
                col_one_list = [item for item in col_one_list if not(pd.isnull(item)) == True]
                col_one_list = [str(n) for n in col_one_list]
            
            if col_one_list != ['null']:
                col_one_list = list(filter(lambda a: a != 'null', col_one_list))
                body = {"method": "get",  # Используемый метод.
                "params": {"SelectionCriteria": {"AdImageHashes": col_one_list},  # Критерий отбора кампаний. Для получения всех кампаний должен быть пустым
                        "FieldNames": ["AdImageHash","Name","Type","Subtype","OriginalUrl","Associated"]  # Имена параметров, которые требуется получить.
                        }}

                report = client.adimages().post(data=body)
                data = report().extract()
                df_images = pd.DataFrame()
                df_images = pd.json_normalize(data)
                df_images = df_images.rename(columns={'Type':'Image_Type'})
                df_images = df_images.rename(columns={'Subtype':'Image_Subtype'})
                df_images = pd.merge(df_sitelinks, df_images, how='outer', on='AdImageHash')
            else:
                df_images = df_sitelinks
        else:
            df_images = df_sitelinks

        res_df = pd.concat([res_df, df_images], ignore_index=True)

    res_df = res_df.rename(columns=lambda x: x.split('.')[-1])
    res_df.columns = res_df.columns.str.replace(' ', '_')

    if 'ThumbnailUrl' in res_df.columns:
        res_df['ThumbnailUrl'] = res_df['ThumbnailUrl'].fillna('')
        if 'OriginalUrl' in res_df.columns:
            res_df['OriginalUrl'] = res_df['OriginalUrl'].fillna('')
            res_df['OriginalUrl'] = res_df['OriginalUrl'].astype(str) + res_df['ThumbnailUrl'].astype(str)
            res_df = res_df.drop('ThumbnailUrl', axis=1)
        else:
            res_df = res_df.rename(columns={'ThumbnailUrl':'OriginalUrl'})

    res_df = res_df.replace('empty_cell',np.nan)
    res_df = res_df.replace('nan',np.nan)
    res_df['Login'] = str(clientLogin)
    return(res_df)

def get_keywords(token, clientLogin):
    client = YandexDirect(
    # Required parameters:
    access_token=token,
    # If you are making inquiries from an agent account, you must be sure to specify the account login.
    login=clientLogin,
    is_sandbox=False,
    # Repeat request when units run out
    retry_if_not_enough_units=False,
    # The language in which the data for directories and errors will be returned.
    language="ru",
    # Repeat the request if the limits on the number of reports or requests are exceeded.
    retry_if_exceeded_limit=True,
    # Number of retries when server errors occur.
    retries_if_server_error=5,
    # Report generation mode: online, offline or auto.
    processing_mode="offline",
    # When requesting a report, it will wait until the report is prepared and download it.
    wait_report=True,
    # Monetary values in the report are returned in currency with an accuracy of two decimal places.
    return_money_in_micros=False,
    # Do not display a line with the report name and date range in the report.
    skip_report_header=True,
    # Do not display a line with field names in the report.
    skip_column_header=False,
    # Do not display a line with the number of statistics lines in the report.
    skip_report_summary=True,
    )
    body = {
    "method": "get",
    "params": {
        "SelectionCriteria": {},
        "FieldNames": ["Id","Name","State"],
        "TextCampaignFieldNames": ['PriorityGoals'],
        "DynamicTextCampaignFieldNames": ['PriorityGoals'],
        "SmartCampaignFieldNames": ['PriorityGoals']
        },
    }
    report = client.campaigns().post(data=body)
    data = report().extract()
    df = pd.DataFrame()
    df = pd.json_normalize(data)
    df['Login'] = str(clientLogin)
    print(clientLogin)
    return(df)