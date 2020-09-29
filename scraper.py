# -*- coding: utf-8 -*-
import csv
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from bs4 import BeautifulSoup

import importa_arquivos
import utils


def get_links(data_inicial):
    url_base = 'http://www.debentures.com.br'
    url_pu = f'{url_base}/exploreosnd/consultaadados/emissoesdedebentures/'
    url = url_pu+'puhistorico_f.asp'
    res = requests.get(url)

    while res.status_code != 200:
        res = requests.get(url)

    soup = BeautifulSoup(res.text, "html.parser")
    select = soup.find("select", {"name": "ativo"})

    urls = []
    for option in select.find_all('option'):
        ativo = option['value'].strip()

        if len(ativo) < 6:
            continue

        ativo = ativo.replace(' ', '+')

        today = datetime.today()

        url_compl = '/exploreosnd/consultaadados/emissoesdedebentures/'
        url = f'{url_base}{url_compl}'
        url = f'{url}puhistorico_e.asp?'
        url = f'{url}op_exc=False&dt_ini={data_inicial}&Submit.x=34&Submit.y=13'
        url = f"{url}&dt_fim={today.strftime('%d/%m/%Y')}&ativo={ativo}++++"
        urls.append({'ativo': ativo, 'url': url})

    return urls


def main():
    utils.prepare_download_folder('downloads')

    # pega a data máxima de referência
    file_path = os.path.join('bases', 'debentures.csv')
    data_inicial = ''
    if os.path.exists(file_path):
        df_base = pd.read_csv(file_path)
        df_base['data_referencia'] = pd.to_datetime(df_base['data_referencia'])
        print('Máxima data de referência', df_base['data_referencia'].max())
        data_inicial = df_base['data_referencia'].max()
        data_inicial = data_inicial + timedelta(days=1)
        data_inicial = data_inicial.strftime('%d/%m/%Y')

    urls = get_links(data_inicial)

    tamanho = len(urls)
    for index, url in enumerate(urls):
        name_file = url['ativo']+'.csv'
        path_file = os.path.join('downloads', name_file)
        print(
            f'{index+1} de {tamanho}',
            ' Baixando arquivo do ativo',
            url['ativo'], name_file
        )

        utils.download(url['url'], None, path_file)
        time.sleep(1)

        if index > 0 and index % 50 == 0:
            print('Aguardando 30 segundos, para evitar timeout')
            time.sleep(30)

    print('Consolidando arquivos baixados')
    importa_arquivos.main()


if __name__ == '__main__':
    main()
    time.sleep(60)
    # rename file
    os.rename('scraperwiki.sqlite', 'data.sqlite')
