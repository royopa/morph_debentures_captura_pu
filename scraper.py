# -*- coding: utf-8 -*-
from __future__ import print_function
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import scraperwiki

def prepare_download_folder(folder_name):
    folder_path = os.path.join('downloads', folder_name)
    return prepare_folder(folder_path)


def prepare_folder(folder_path):
    if not os.path.exists(folder_path):
        Path(folder_path).mkdir(parents=True, exist_ok=True)

    return folder_path


def download_file(url, file_path):
    file_path_csv = file_path.replace('.ZIP', '.CSV')
    if os.path.exists(file_path) or os.path.exists(file_path_csv):
        print('Arquivo já baixado anteriormente', file_path)
        return False

    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        print('Arquivo não encontrado', url, response.status_code)
        return False

    with open(file_path, "wb") as handle:
        print('Downloading', url)
        for data in response.iter_content():
            handle.write(data)
    handle.close()
    return True



def create_download_folder():
    # Create directory
    dirName = os.path.join('downloads')
 
    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory", dirName, "Created ")
    except FileExistsError:
        print("Directory", dirName, "already exists")


def get_urls():
    url_base = 'http://www.debentures.com.br/exploreosnd/consultaadados/emissoesdedebentures/'
    url = url_base+'puhistorico_f.asp'
    res = requests.get(url)

    while res.status_code != 200:
      res = requests.get(url)

    soup = BeautifulSoup(res.text,"html.parser")
    select = soup.find("select", {"name":"ativo"})

    urls = []
    for option in select.find_all('option'):
        ativo = option['value'].strip()
        
        if len(ativo) < 6:
          continue
        
        url_download = url_base + 'puhistorico_e.asp?op_exc=False&dt_ini=&dt_fim=&Submit.x=34&Submit.y=13&ativo='+ativo+'++++'
        urls.append({'ativo': ativo,'url':url_download})
    
    return urls


def download_files_debentures(urls):
    for url in urls:
        try:
            print('Baixando arquivo do ativo', url['ativo'])
            name_file = url['ativo']+'.csv'
            path_file = os.path.join('downloads', name_file)
            # download file
            download_file(url['url'], path_file)
        except:
            print('Erro', url)
            continue


def process_files_debentures():
    download_path = os.path.join('downloads')
    for file_name in os.listdir(download_path):
        path_file = os.path.join(download_path, file_name)
        print('Processando arquivo', path_file)
        process_file(path_file)
        # remove processed file
        os.remove(path_file)


def process_file(file_path):
    df = pd.read_csv(
        file_path,
        skiprows=2,
        encoding='iso-8859-1',
        sep='\t'
    )

    print('Importing {} items'.format(len(df)))

    # remove as linhas com problemas
    df = df[df['Ativo'].notnull()]

    # remove unnamed columns
    df.drop('Unnamed: 8', axis=1, inplace=True)

    #print(df.tail())

    for index, row in df.iterrows():
        try:
            data = {
                'data': row['Data do PU'],
                'ativo': row['Ativo'],
                'valor_nominal': row['Valor Nominal'],
                'valor_juros': row['Juros'],
                'valor_premio': row['Prêmio'],
                'preco_unitario': row['Preço Unitário'],
                'criterio_calculo': row['Critério de Cálculo'],
                'situacao': row['Situação']
            }
            scraperwiki.sqlite.save(unique_keys=['data', 'ativo'], data=data)
        except Exception as e:
            print("Error occurred:", e)
            return False
    return True


def main():
    # create download folder
    create_download_folder()

    urls = get_urls()
    download_files_debentures(urls)
    process_files_debentures()

    # rename file
    os.rename('scraperwiki.sqlite', 'data.sqlite')


if __name__ == '__main__':
    main()
