from airflow import DAG
from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

import sys
import configparser
import pandas as pd
from sqlalchemy import create_engine

get_data = DAG(
    "mhid_control", # Название задачи в airflow. по умолчанию - идентификатор клиента_control
    start_date=days_ago(0, 0, 0, 0, 0) # когда запускать задачу
    )

def script():
    sys.path.append(r'/home/alex/airflow/dags/patterns')
    sys.path.append(r'/home/alex/airflow/dags/')
    config = configparser.ConfigParser()
    config.read(r'/home/alex/airflow/dags/__fields.ini')
    config_db = configparser.ConfigParser()
    config_db.read(r'/home/alex/airflow/dags/__settings.ini')

    # папка patterns должна лежать рядом с данным скриптом
    import patterns.yd as yd
    import patterns.ym as ym
    import patterns.click as click
    import patterns.yesterday as yesterday

    from sqlalchemy import create_engine

    ###########################################################################################
    ################################################################ укажите параметры скрипта
    dateTo = str(yesterday.getYesterday()) #конечная дата
    dataset = "control_oceanis_riverstart" # целевой датасет или база данных для записи

    mode_write = "replace" # перезапись данных
    #mode_write = "append" # добавление данных

    # PostgreSQL - коннект
    engine = create_engine("postgresql://" + config_db['postgresql']['username'] + ':' + config_db['postgresql']['password'] + '@' + config_db['postgresql']['host'] + ':' + config_db['postgresql']['port'] + "/" + dataset)

    ########################################################################################
    ################################################################ Итоговые датафреймы (в них собираются данные)
    control_metrika_stats = pd.DataFrame(columns=['date','lastsignTrafficSourceName','lastSignSourceEngineName','lastSignUTMSource','lastSignUTMMedium','lastSignUTMCampaign','startURL','sumVisits','bounceRate','avgPageViews','Login','tag','directID'])
    control_ydirect_stats = pd.DataFrame(columns=['Date','State','AvgPageviews','BounceRate','CampaignType','CampaignName','CampaignId','AdGroupName','AdGroupId','Criterion','CriterionId','Impressions','Clicks','Cost','Login','tag','custom_dimension'])
    control_vkads_stats = pd.DataFrame(columns=['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName','tag'])
    control_mytarget_stats = pd.DataFrame(columns=['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName','tag'])
    control_google_stats = pd.DataFrame(columns=['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName','tag'])
    control_assists_camp = pd.DataFrame(columns=['date','utm_medium','utm_source','utm_campaign','tag'])
    control_assists_sour = pd.DataFrame(columns=['date','lastTrafficSource','source2','tag'])

    control_metrika_stats.to_sql("control_metrika_stats", engine, if_exists=mode_write)
    control_ydirect_stats.to_sql("control_ydirect_stats", engine, if_exists=mode_write)
    control_vkads_stats.to_sql("control_vkads_stats", engine, if_exists=mode_write)
    control_mytarget_stats.to_sql("control_mytarget_stats", engine, if_exists=mode_write)
    control_google_stats.to_sql("control_google_stats", engine, if_exists=mode_write)
    control_assists_camp.to_sql("control_assists_camp", engine, if_exists=mode_write)
    control_assists_sour.to_sql("control_assists_sour", engine, if_exists=mode_write)

    dateFrom = "2023-04-05"

    # Метрика - источники/конверсии. Обязательный пункт, может быть несколько счетчиков.
    counter = [
        {'counter':'88269306', 
        'token':'XXXXXXXXXXXXXXXX', 
        'conv':['290534052','290532335','290531916'],
        'dateFrom':dateFrom,
        'page_filter': "", #"EXISTS(ym:s:startURL=*'*tariff*')", #OR EXISTS(ym:pv:URL=*'*about*')" - выбрать визиты, где был просмотр URL содержащих tariffs. 
        'tag':'aquapark' # тег для куска данных. необходим для объединения с данными по рекламе
        },

        {'counter':'88269279', 
        'token':'XXXXXXXXXXXXXXXX', 
        'conv':['291210068','291212676','291214181'],
        'dateFrom':dateFrom,
        'page_filter':'', # "EXISTS(ym:pv:URL=*'*tariff*') OR EXISTS(ym:pv:URL=*'*about*')" - выбрать визиты, где был просмотр URL содержащих tariffs. 
        'tag':'therm' # тег для куска данных. необходим для объединения с данными по рекламе
        }
    ]

    # Директ - расходы. Можно удалить если источник не используется

    custom_dimension = "LocationOfPresenceName" # Дополнительный кастомный срез по просьбе клиента

    yd_clientLogin = [
        {'login':'Oceanisaquapark-riverstart', 
            'token':'XXXXXXXXXXXXXXXX',
            'dateFrom':dateFrom,
            'conv':['290534052','290532335','290531916'],
            'camp_filter':[], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
            'tag':'aquapark'
        },
        {'login':'Oceanistherm-riverstart', 
            'token':'XXXXXXXXXXXXXXXX',
            'dateFrom':dateFrom,
            'conv':['291210068','291212676','291214181'],
            'camp_filter':[], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
            'tag':'therm'
        }
    ]

    # VK - click.ru расходы. Можно удалить если источник не используется
    vk_c = [
        {'uid':'1675402',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom,
        'camp_filter':[], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
        'tag':'aquapark'
        },
        {'uid':'1675401',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom,
        'camp_filter':[''], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
        'tag':'therm'
        }
    ]

    # MT - click.ru расходы. Можно удалить если источник не используется
    mt_c = [
        {'uid':'1641088',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom,
        'camp_filter':[], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
        'tag':'therm'
        }
    ]

    # GA - click.ru расходы. Можно удалить если источник не используется
    ga_c = [
        {'uid':'1654462',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom,
        'camp_filter':[], # Укажите рекламные кампании, которые относятся к текущему клиенту. В противном случае будут получены все кампании
        'tag':'therm'
        }
    ]

    ##############################################################################################################
    #################################################### Яндекс Метрика

    for i in counter:
        fields_dim = 'dim_control_all' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        fields_met = 'met_control_all' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

        report_fields_dim = config['yandex_metrika_names'][fields_dim]
        report_fields_met = config['yandex_metrika_names'][fields_met]

        if i['conv'] == [] or i['conv'] == ['']:
            pass
        else:
            goals_ym = ym.convert_goals_s(i['conv'])
            report_fields_met = report_fields_met + ',' + goals_ym

        if i['page_filter'] == [] or i['page_filter'] == ['']:
            metrika_dataset = ym.get_ym_stat(i['counter'],"",i['token'],i['dateFrom'],dateTo,report_fields_met,report_fields_dim)
            metrika_dataset.rename(columns=lambda x: x.strip().replace("ym_s_goal", "Conversions_"), inplace=True)
            metrika_dataset.rename(columns=lambda x: x.strip().replace("ym_s_", ""), inplace=True)
        else:
            values_to_filter = i['page_filter'] #ym:s:startURL
            metrika_dataset = ym.get_ym_stat_filter(i['counter'],"",i['token'],i['dateFrom'],dateTo,report_fields_met,report_fields_dim,values_to_filter)
            metrika_dataset.rename(columns=lambda x: x.strip().replace("ym_s_goal", "Conversions_"), inplace=True)
            metrika_dataset.rename(columns=lambda x: x.strip().replace("ym_s_", ""), inplace=True)
        
        metrika_dataset['tag'] = i['tag']
        control_metrika_stats = pd.concat([control_metrika_stats, metrika_dataset], ignore_index=True)

    ##############################################################################################################
    #################################################### Яндекс Метрика - Ассоциированные конверсии

    for i in counter:
        ass_camp = ym.associate_conv_camp(i['counter'],i['token'],i['dateFrom'],dateTo,i['conv'])
        ass_camp['tag'] = i['tag']
        control_assists_camp = pd.concat([control_assists_camp, ass_camp], ignore_index=True)

        ass_sour = ym.associate_conv_sour(i['counter'],i['token'],i['dateFrom'],dateTo,i['conv'])
        ass_sour['tag'] = i['tag']
        control_assists_sour = pd.concat([control_assists_sour, ass_sour], ignore_index=True)

    ##############################################################################################################
    #################################################### Яндекс Директ

    for i in yd_clientLogin:
        fields = 'yd_control_all' # набор полей которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        report_fields = list(config['yandex_direct_names'][fields].split(","))
        report_fields.append(custom_dimension)

        if i['conv'] == [] or i['conv'] == ['']:
            direct_dataset = yd.get_data_yd(i['token'], i['login'], '', i['dateFrom'], dateTo, 60, report_fields)
        else:
            direct_dataset = yd.get_data_yd(i['token'], i['login'], i['conv'], i['dateFrom'], dateTo, 60, report_fields)
        
        if i['camp_filter'] == [] or i['camp_filter'] == ['']:
            pass
        else:
            direct_dataset = direct_dataset[direct_dataset['CampaignId'].isin(i['camp_filter'])]

        direct_dataset['tag'] = i['tag']
        direct_dataset['login'] = i['tag']
        get_states = yd.get_ads(i['token'], i['login'])
        get_states = get_states.groupby('CampaignId')['State'].first().reset_index()
        direct_dataset = direct_dataset.astype({'CampaignId':'int64'})
        direct_dataset = pd.merge(get_states, direct_dataset, on='CampaignId')
        direct_dataset = direct_dataset.rename(columns={custom_dimension: "custom_dimension"})
        control_ydirect_stats = pd.concat([control_ydirect_stats, direct_dataset], ignore_index=True) # кладем полученный датасет в общий, чтобы сохранить

    ##############################################################################
    ############################################################### VK Ads CLICK.RU
    for i in vk_c:
        vk_click_dataset = click.click_vk(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        vk_click_dataset = vk_click_dataset.rename(columns={'account':'Login'})
        vk_click_dataset = vk_click_dataset[['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName']]
        vk_click_dataset = vk_click_dataset.astype({'CampaignId':'string'})

        if i['camp_filter'] == [] or i['camp_filter'] == ['']:
            pass
        else:
            vk_click_dataset = vk_click_dataset[vk_click_dataset['CampaignId'].isin(i['camp_filter'])]

        vk_click_dataset['tag'] = i['tag']
        control_vkads_stats = pd.concat([control_vkads_stats, vk_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### MyTarget CLICK.RU
    for i in mt_c:
        mt_click_dataset = click.click_mt(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        mt_click_dataset = mt_click_dataset.rename(columns={'account':'Login'})
        mt_click_dataset = mt_click_dataset[['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName']]
        mt_click_dataset = mt_click_dataset.astype({'CampaignId':'string'})

        if i['camp_filter'] == [] or i['camp_filter'] == ['']:
            pass
        else:
            mt_click_dataset = mt_click_dataset[mt_click_dataset['CampaignId'].isin(i['camp_filter'])]

        mt_click_dataset['tag'] = i['tag']
        control_mytarget_stats = pd.concat([control_mytarget_stats, mt_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Google Ads CLICK.RU
    for i in ga_c:
        ga_click_dataset = click.click_ga(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        ga_click_dataset = ga_click_dataset.rename(columns={'account':'Login'})
        ga_click_dataset = ga_click_dataset[['Date','Impressions','Clicks','Cost','Login','CampaignId','adUrl','state','CampaignName']]
        ga_click_dataset = click.usd_to_rub(i['dateFrom'],dateTo,ga_click_dataset)
        ga_click_dataset = ga_click_dataset.astype({'CampaignId':'string'})

        if i['camp_filter'] == [] or i['camp_filter'] == ['']:
            pass
        else:
            ga_click_dataset = ga_click_dataset[ga_click_dataset['CampaignId'].isin(i['camp_filter'])]

        ga_click_dataset['tag'] = i['tag']
        control_google_stats = pd.concat([control_google_stats, ga_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Пишем данные в базу

    control_metrika_stats.to_sql("control_metrika_stats", engine, if_exists=mode_write)
    control_ydirect_stats.to_sql("control_ydirect_stats", engine, if_exists=mode_write)
    control_vkads_stats.to_sql("control_vkads_stats", engine, if_exists=mode_write)
    control_mytarget_stats.to_sql("control_mytarget_stats", engine, if_exists=mode_write)
    control_google_stats.to_sql("control_google_stats", engine, if_exists=mode_write)
    control_assists_camp.to_sql("control_assists_camp", engine, if_exists=mode_write)
    control_assists_sour.to_sql("control_assists_sour", engine, if_exists=mode_write)

    a = 'Success!'
    return(a)


# Для каждой функции копируем блок operation и вписываем функцию в python_callable

operation = PythonOperator(
    task_id = 'data_load',
    python_callable=script,
    dag=get_data
    )

# Если функций несколько, раскомментировать часть кода ниже для определения очередности выполнения

operation #>> operation2 >> operation3