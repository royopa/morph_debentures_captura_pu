# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import scraperwiki
from tqdm import tqdm


def download_file(url, file_name):
    response = requests.get(url, stream=True)
    with open(file_name, "wb") as handle:
        for data in tqdm(response.iter_content()):
            handle.write(data)
    handle.close()


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


def process_files_debentures(urls):
    for url in urls:
        print('Baixando arquivo do ativo', url['ativo'])
        name_file = url['ativo']+'.csv'
        path_file = os.path.join('downloads', name_file)
        # download file
        download_file(url['url'], path_file)
        # process file
        print('Processando arquivo', name_file)
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

    for index, row in df.iterrows():
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


def main():
    # format the name of database used for morph.io
    os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
    
    # create download folder
    create_download_folder()

    urls = get_urls()
    process_files_debentures(urls)


if __name__ == '__main__':
    main()
