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
    "agency_dash", # Название задачи в airflow. по умолчанию - идентификатор клиента_control
    start_date=days_ago(0, 0, 0, 0, 0) # когда запускать задачу
    )

def client1():
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

    ###########################################################################################
    ################################################################ укажите параметры скрипта
    # начальная дата
    dateTo = str(yesterday.getYesterday()) #конечная дата
    dataset = "agency_riverstart" # целевой датасет или база данных для записи

    mode_write = "replace" # перезапись данных
    #mode_write = "append" # добавление данных

    # PostgreSQL - коннект
    engine = create_engine("postgresql://" + config_db['postgresql']['username'] + ':' + config_db['postgresql']['password'] + '@' + config_db['postgresql']['host'] + ':' + config_db['postgresql']['port'] + "/" + dataset)

    ########################################################################################
    ################################################################ Итоговые датафреймы (в них собираются данные)
    agency_costs = pd.DataFrame()
    agency_conv = pd.DataFrame()

    # Нейминг клиента
    name_client = "M.House"
    dateFrom = "2023-01-01"

    # Метрика - источники/конверсии. Обязательный пункт, может быть несколько счетчиков.
    counter = [
        {'counter':'23382340', 
        'token':'XXXXXXXXXXXXXXXX', 
        'conv':['221844571','221844344','221844334','62246119','59192935','59192911','41972125','227067489','41929222','41970658','41970718','41970883','41970898','41971087'],
        'dateFrom':dateFrom
        }
    ]

    # Директ - расходы. Можно удалить если источник не используется
    yd_clientLogin = [
        {'login':'mhouse-riverstart', 
        'token':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    # VK - click.ru расходы. Можно удалить если источник не используется
    vk_c = [
        {'uid':'1578482',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    # MT - click.ru расходы. Можно удалить если источник не используется
    mt_c = [
        {'uid':'1578482',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    ga_c = [
        {'uid':'1654462',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    for i in counter:
        fields_dim = 'dim_agency' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        fields_met = 'met_agency' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

        report_fields_dim = config['yandex_metrika_names'][fields_dim]
        report_fields_met = config['yandex_metrika_names'][fields_met]

        goals_ym = ym.convert_goals_s(i['conv'])
        report_fields_met = report_fields_met + ',' + goals_ym

        metrika_dataset = ym.get_ym_stat(i['counter'],"",i['token'],i['dateFrom'],dateTo,report_fields_met,report_fields_dim)
        value_cols = metrika_dataset.filter(like='ym_s_goal').columns
        metrika_dataset['Macroconversions'] = metrika_dataset[value_cols].sum(axis=1)
        metrika_dataset = metrika_dataset.drop(columns=value_cols)
        metrika_dataset['Client'] = name_client
        agency_conv = pd.concat([agency_conv, metrika_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Яндекс директ
    for i in yd_clientLogin:
        fields = 'yd_agency' # набор полей которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        report_fields = list(config['yandex_direct_names'][fields].split(","))
        direct_dataset = yd.get_data_yd(i['token'], i['login'], "", i['dateFrom'], dateTo, 60, report_fields) # функция обращается к api директа и собирает данные
        direct_dataset['Client'] = name_client
        direct_dataset['TrafficType'] = 'Контекст'
        direct_dataset['service'] = 'YANDEX_DIRECT'
        agency_costs = pd.concat([agency_costs, direct_dataset], ignore_index=True) # кладем полученный датасет в общий, чтобы сохранить

    ##############################################################################
    ############################################################### VK Ads CLICK.RU
    for i in vk_c:
        vk_click_dataset = click.click_vk(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        vk_click_dataset['Client'] = name_client
        vk_click_dataset = vk_click_dataset.rename(columns={'account':'Login'})
        vk_click_dataset = vk_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        agency_costs = pd.concat([agency_costs, vk_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### MyTarget CLICK.RU
    for i in mt_c:
        mt_click_dataset = click.click_mt(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        mt_click_dataset['Client'] = name_client
        mt_click_dataset = mt_click_dataset.rename(columns={'account':'Login'})
        mt_click_dataset = mt_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        agency_costs = pd.concat([agency_costs, mt_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Google Ads CLICK.RU
    for i in ga_c:
        ga_click_dataset = click.click_ga(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        ga_click_dataset['Client'] = name_client
        ga_click_dataset = ga_click_dataset.rename(columns={'account':'Login'})
        ga_click_dataset = ga_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        ga_click_dataset = click.usd_to_rub(i['dateFrom'],dateTo,ga_click_dataset)
        agency_costs = pd.concat([agency_costs, ga_click_dataset], ignore_index=True)

    ###################################################################################################
    ################################################################ Отправляем полученные данные - POSTGRESQL
    agency_costs.to_sql("agency_costs", engine, if_exists=mode_write)
    agency_conv.to_sql("agency_conv", engine, if_exists=mode_write)

    a = "Success!"
    return(a)

def client2():
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

    ###########################################################################################
    ################################################################ укажите параметры скрипта
    # начальная дата
    dateTo = str(yesterday.getYesterday()) #конечная дата
    dataset = "agency_riverstart" # целевой датасет или база данных для записи

    mode_write = "append" # добавление данных

    # PostgreSQL - коннект
    engine = create_engine("postgresql://" + config_db['postgresql']['username'] + ':' + config_db['postgresql']['password'] + '@' + config_db['postgresql']['host'] + ':' + config_db['postgresql']['port'] + "/" + dataset)

    ########################################################################################
    ################################################################ Итоговые датафреймы (в них собираются данные)
    agency_costs = pd.DataFrame()
    agency_conv = pd.DataFrame()

    # Нейминг клиента
    name_client = "M.House"
    dateFrom = "2023-01-01"

    # Метрика - источники/конверсии. Обязательный пункт, может быть несколько счетчиков.
    counter = [
        {'counter':'23382340', 
        'token':'XXXXXXXXXXXXXXXX', 
        'conv':['221844571','221844344','221844334','62246119','59192935','59192911','41972125','227067489','41929222','41970658','41970718','41970883','41970898','41971087'],
        'dateFrom':dateFrom
        }
    ]

    # Директ - расходы. Можно удалить если источник не используется
    yd_clientLogin = [
        {'login':'mhouse-riverstart', 
        'token':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    # VK - click.ru расходы. Можно удалить если источник не используется
    vk_c = [
        {'uid':'1578482',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    # MT - click.ru расходы. Можно удалить если источник не используется
    mt_c = [
        {'uid':'1578482',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    ga_c = [
        {'uid':'1654462',
        'tkn':'XXXXXXXXXXXXXXXX',
        'dateFrom':dateFrom
        }
    ]

    for i in counter:
        fields_dim = 'dim_agency' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        fields_met = 'met_agency' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

        report_fields_dim = config['yandex_metrika_names'][fields_dim]
        report_fields_met = config['yandex_metrika_names'][fields_met]

        goals_ym = ym.convert_goals_s(i['conv'])
        report_fields_met = report_fields_met + ',' + goals_ym

        metrika_dataset = ym.get_ym_stat(i['counter'],"",i['token'],i['dateFrom'],dateTo,report_fields_met,report_fields_dim)
        value_cols = metrika_dataset.filter(like='ym_s_goal').columns
        metrika_dataset['Macroconversions'] = metrika_dataset[value_cols].sum(axis=1)
        metrika_dataset = metrika_dataset.drop(columns=value_cols)
        metrika_dataset['Client'] = name_client
        agency_conv = pd.concat([agency_conv, metrika_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Яндекс директ
    for i in yd_clientLogin:
        fields = 'yd_agency' # набор полей которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
        report_fields = list(config['yandex_direct_names'][fields].split(","))
        direct_dataset = yd.get_data_yd(i['token'], i['login'], "", i['dateFrom'], dateTo, 60, report_fields) # функция обращается к api директа и собирает данные
        direct_dataset['Client'] = name_client
        direct_dataset['TrafficType'] = 'Контекст'
        direct_dataset['service'] = 'YANDEX_DIRECT'
        agency_costs = pd.concat([agency_costs, direct_dataset], ignore_index=True) # кладем полученный датасет в общий, чтобы сохранить

    ##############################################################################
    ############################################################### VK Ads CLICK.RU
    for i in vk_c:
        vk_click_dataset = click.click_vk(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        vk_click_dataset['Client'] = name_client
        vk_click_dataset = vk_click_dataset.rename(columns={'account':'Login'})
        vk_click_dataset = vk_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        agency_costs = pd.concat([agency_costs, vk_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### MyTarget CLICK.RU
    for i in mt_c:
        mt_click_dataset = click.click_mt(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        mt_click_dataset['Client'] = name_client
        mt_click_dataset = mt_click_dataset.rename(columns={'account':'Login'})
        mt_click_dataset = mt_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        agency_costs = pd.concat([agency_costs, mt_click_dataset], ignore_index=True)

    ##############################################################################
    ############################################################### Google Ads CLICK.RU
    for i in ga_c:
        ga_click_dataset = click.click_ga(i['dateFrom'],dateTo,i['uid'],i['tkn'])
        ga_click_dataset['Client'] = name_client
        ga_click_dataset = ga_click_dataset.rename(columns={'account':'Login'})
        ga_click_dataset = ga_click_dataset[['Date','Impressions','Clicks','Cost','Login','Client','TrafficType','service']]
        ga_click_dataset = click.usd_to_rub(i['dateFrom'],dateTo,ga_click_dataset)
        agency_costs = pd.concat([agency_costs, ga_click_dataset], ignore_index=True)

    ###################################################################################################
    ################################################################ Отправляем полученные данные - POSTGRESQL
    agency_costs.to_sql("agency_costs", engine, if_exists=mode_write)
    agency_conv.to_sql("agency_conv", engine, if_exists=mode_write)

# Для каждой функции копируем блок operation и вписываем функцию в python_callable

func_client1 = PythonOperator(
    task_id = 'data_load_1',
    python_callable=client1,
    dag=get_data
    )

func_client2 = PythonOperator(
    task_id = 'data_load_2',
    python_callable=client2,
    dag=get_data
    )

# Если функций несколько, раскомментировать часть кода ниже для определения очередности выполнения

func_client1 >> func_client2 #>> operation3