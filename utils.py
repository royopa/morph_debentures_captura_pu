#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import csv
import datetime
import os
import random
import shutil
import time
from datetime import timedelta

import pandas as pd
import requests
from bizdays import Calendar, load_holidays
from tqdm import tqdm


def load_useragents():
    uas = []
    with open("user-agents.txt", 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[0:-1-0])
    random.shuffle(uas)
    return uas


def check_download(dt_referencia, file_name):
    if not isbizday(dt_referencia):
        print(dt_referencia, 'não é dia útil')
        return False
    if os.path.exists(file_name):
        print(file_name, 'arquivo já baixado')
        return False
    return True


def download(url, params, file_name):
    headers = {'User-Agent': random.choice(load_useragents())}
    try:
        response = requests.get(
            url, params=params, stream=True, headers=headers
        )
    except Exception:
        time.sleep(30)
        try:
            response = requests.get(
                url, params=params, stream=True, headers=headers
            )
        except Exception:
            time.sleep(60)
            response = requests.get(
                url, params=params, stream=True, headers=headers
            )

    if response.status_code != 200:
        'Nenhum arquivo encontrado nessa url'
        return False
    with open(file_name, "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()


def xrange(x):
    return iter(range(x))


def datetime_range(start=None, end=None):
    span = end - start
    for i in xrange(span.days + 1):
        yield start + timedelta(days=i)


def get_calendar():
    holidays = load_holidays('ANBIMA.txt')
    return Calendar(holidays, ['Sunday', 'Saturday'])


def isbizday(dt_referencia):
    cal = get_calendar()
    return cal.isbizday(dt_referencia)


def prepare_download_folder(name_download_folder):
    path_download = os.path.join('downloads')
    if os.path.exists(path_download):
        shutil.rmtree(path_download)
        os.makedirs(path_download)
    if not os.path.exists(path_download):
        os.makedirs(path_download)
    return path_download
