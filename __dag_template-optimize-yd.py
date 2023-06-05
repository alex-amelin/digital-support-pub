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
    "mhid_optimize_yd", # Название задачи в airflow
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

    ###########################################################################################
    ################################################################ укажите параметры скрипта
    dateFrom = "2022-01-01" # начальная дата
    dateTo = str(yesterday.getYesterday()) #конечная дата
    dataset = "optimize_mhid_riverstart" # целевой датасет или база данных для записи

    mode_write = "replace" # перезапись данных
    #mode_write = "append" # добавление данных

    engine = create_engine("postgresql://" + config_db['postgresql']['username'] + ':' + config_db['postgresql']['password'] + '@' + config_db['postgresql']['host'] + ':' + config_db['postgresql']['port'] + "/" + dataset)

    ########################################################################################
    ################################################################ Итоговые датафреймы (в них собираются данные)
    res_df_metrika_hours = pd.DataFrame()
    res_df_yd_optimize_camp = pd.DataFrame()
    res_df_yd_optimize_network = pd.DataFrame()
    res_df_yd_optimize_group = pd.DataFrame()
    res_df_yd_optimize_device = pd.DataFrame()
    res_df_yd_optimize_os = pd.DataFrame()
    res_df_yd_optimize_adform = pd.DataFrame()
    res_df_yd_optimize_agegen = pd.DataFrame()
    res_df_yd_optimize_tarcat = pd.DataFrame()
    res_df_yd_optimize_income = pd.DataFrame()
    res_df_yd_optimize_criteria = pd.DataFrame()
    res_df_yd_optimize_critype = pd.DataFrame()
    res_df_yd_optimize_location = pd.DataFrame()
    res_df_yd_optimize_match = pd.DataFrame()
    res_df_yd_optimize_clcktype = pd.DataFrame()
    res_df_yd_optimize_slot = pd.DataFrame()
    res_df_yd_optimize_adid = pd.DataFrame()
    res_df_yd_optimize_placement = pd.DataFrame()
    res_df_df_ads_text = pd.DataFrame()

    # в названии переменной:
    ##############################################################################
    ############################################################### Яндекс директ
    token = "XXXXXXXXXXXXXXXX" # токен директа
    clientLogin = 'mcintra-riverstart' # Логины клиента
    goals_yd = ["273937979","159281410","57342526","59934022"] # цели

    count_days_per_req = 60 # кол-во дней для 1 запроса к апи директа

    ##############################################################################################################
    #################################################### Сбор данных Яндекс Директа

    fields = 'yd_optimize_camp' # набор полей которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields) # функция обращается к api директа и собирает данные
    res_df_yd_optimize_camp = pd.concat([res_df_yd_optimize_camp, direct_dataset], ignore_index=True) # кладем полученный датасет в общий, чтобы сохранить


    fields = 'yd_optimize_network'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_network = pd.concat([res_df_yd_optimize_network, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_group'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_group = pd.concat([res_df_yd_optimize_group, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_device'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_device = pd.concat([res_df_yd_optimize_device, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_os'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_os = pd.concat([res_df_yd_optimize_os, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_adform'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_adform = pd.concat([res_df_yd_optimize_adform, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_agegen'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_agegen = pd.concat([res_df_yd_optimize_agegen, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_tarcat'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_tarcat = pd.concat([res_df_yd_optimize_tarcat, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_income'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_income = pd.concat([res_df_yd_optimize_income, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_criteria'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_criteria = pd.concat([res_df_yd_optimize_criteria, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_critype'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_critype = pd.concat([res_df_yd_optimize_critype, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_location'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_location = pd.concat([res_df_yd_optimize_location, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_match'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_match = pd.concat([res_df_yd_optimize_match, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_clcktype'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_clcktype = pd.concat([res_df_yd_optimize_clcktype, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_slot'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_slot = pd.concat([res_df_yd_optimize_slot, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_adid'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_adid = pd.concat([res_df_yd_optimize_adid, direct_dataset], ignore_index=True)


    fields = 'yd_optimize_placement'
    report_fields = list(config['yandex_direct_names'][fields].split(","))
    direct_dataset = yd.get_data_yd(token, clientLogin, goals_yd, dateFrom, dateTo, count_days_per_req, report_fields)
    res_df_yd_optimize_placement = pd.concat([res_df_yd_optimize_placement, direct_dataset], ignore_index=True)


    direct_dataset = yd.get_ads(token, clientLogin)
    res_df_df_ads_text = pd.concat([res_df_df_ads_text, direct_dataset], ignore_index=True)

    ###############################################################################
    ############################################################### Яндекс метрика
    token_ym = "XXXXXXXXXXXXXXXX"
    clientLogin = 'mcintra-riverstart'
    counter = '55226488'
    goals_metrika = ["273937979","159281410","57342526","59934022"]

    #########################################################################################
    ############################################# Сбор данных Метрики - РАСХОД ПО ЧАСАМ СУТОК

    fields_dim = 'dim_yd_optimize_hours' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
    fields_met = 'met_yd_optimize_hours' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

    report_fields_dim = config['yandex_metrika_names'][fields_dim]
    report_fields_met = config['yandex_metrika_names'][fields_met]

    # если необходимо получить конверсии, то раскоментируйте следующие строки

    goals_ym = ym.convert_goals_ad(goals_metrika)
    report_fields_met = report_fields_met + ',' + goals_ym

    metrika_dataset = ym.get_ym_stat(counter,clientLogin,token_ym,dateFrom,dateTo,report_fields_met,report_fields_dim)
    res_df_metrika_hours = pd.concat([res_df_metrika_hours, metrika_dataset], ignore_index=True)

    #############################################################################################
    ########################################################### Сбор данных Метрики - ТОРГОВЫЕ РК

    fields_dim = 'dim_yd_optimize_merch_camp' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
    fields_met = 'met_yd_optimize_merch_camp' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

    report_fields_dim = config['yandex_metrika_names'][fields_dim]
    report_fields_met = config['yandex_metrika_names'][fields_met]

    # если необходимо получить конверсии, то раскоментируйте следующие строки

    goals_ym = ym.convert_goals_ad(goals_metrika)
    report_fields_met = report_fields_met + ',' + goals_ym

    metrika_dataset = ym.get_merch_stat(counter,clientLogin,token_ym,dateFrom,dateTo,report_fields_met,report_fields_dim)
    res_df_yd_optimize_camp = pd.concat([res_df_yd_optimize_camp, metrika_dataset], ignore_index=True)

    ###########################################################################################
    ############################################### Сбор данных Метрики - РЕГИОНЫ В ТОРГОВЫХ РК

    fields_dim = 'dim_yd_optimize_merch_location' # набор параметров, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника
    fields_met = 'met_yd_optimize_merch_location' # набор метрик, которые требуется получить. для создания нового набора добавьте в файл data_fields.ini новый список полей для соответствующего источника

    report_fields_dim = config['yandex_metrika_names'][fields_dim]
    report_fields_met = config['yandex_metrika_names'][fields_met]

    # если необходимо получить конверсии, то раскоментируйте следующие строки

    goals_ym = ym.convert_goals_ad(goals_metrika)
    report_fields_met = report_fields_met + ',' + goals_ym

    metrika_dataset = ym.get_merch_stat(counter,clientLogin,token_ym,dateFrom,dateTo,report_fields_met,report_fields_dim)
    res_df_yd_optimize_location = pd.concat([res_df_yd_optimize_location, metrika_dataset], ignore_index=True)

    # dataframe - название датафрейма с данными для записи
    res_df_yd_optimize_camp.to_sql("yd_optimize_camp_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_network.to_sql("yd_optimize_network_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_group.to_sql("yd_optimize_group_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_device.to_sql("yd_optimize_device_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_os.to_sql("yd_optimize_os_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_adform.to_sql("yd_optimize_adform_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_agegen.to_sql("yd_optimize_agegen_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_tarcat.to_sql("yd_optimize_tarcat_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_income.to_sql("yd_optimize_income_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_criteria.to_sql("yd_optimize_criteria_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_critype.to_sql("yd_optimize_critype_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_location.to_sql("yd_optimize_location_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_match.to_sql("yd_optimize_match_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_clcktype.to_sql("yd_optimize_clcktype_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_slot.to_sql("yd_optimize_slot_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_placement.to_sql("yd_optimize_placement_stat", engine, if_exists=mode_write)
    res_df_yd_optimize_adid.to_sql("yd_optimize_adid_stat", engine, if_exists=mode_write)
    res_df_df_ads_text.to_sql("yandex_ads", engine, if_exists=mode_write)
    res_df_metrika_hours.to_sql("yd_optimize_hours_stat", engine, if_exists=mode_write)

    a = "Success!"
    return(a)


# Для каждой функции копируем блок operation и вписываем функцию в python_callable

operation = PythonOperator(
    task_id = 'data_load',
    python_callable=script,
    dag=get_data
    )

# Если функций несколько, раскомментировать часть кода ниже для определения очередности выполнения

operation #>> operation2 >> operation3