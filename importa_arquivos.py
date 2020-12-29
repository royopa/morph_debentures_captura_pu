#!/usr/bin/env python
# coding: utf-8
import os

import dask.dataframe as dd
import numpy as np
import pandas as pd
import scraperwiki
from sqlalchemy import create_engine

os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'


def main():
    # pega a data máxima de referência
    file_path = os.path.join('bases', 'debentures.csv')
    if os.path.exists(file_path):
        df_base = pd.read_csv(file_path)
        df_base = df_base[df_base['data_referencia'] != 'data_referencia']
        df_base['data_referencia'] = pd.to_datetime(
            df_base['data_referencia']).dt.date
        print('Máxima data de referência', df_base['data_referencia'].max())

    for file_name in os.listdir('downloads'):
        if file_name.endswith('.csv') is False:
            continue

        file_path = os.path.join('downloads', file_name)

        reader = pd.read_csv(
            file_path,
            encoding='latin1',
            skiprows=2,
            sep='\t',
            chunksize=75000
        )

        for df in reader:

            df = df.dropna(subset=['Ativo'])

            del df['Unnamed: 8']

            df['Data do PU'] = pd.to_datetime(
                df['Data do PU'], format='%d/%m/%Y', errors='ignore').dt.date

            df.rename(columns={
                'Data do PU': 'data_referencia',
                'Ativo': 'ativo',
                'Valor Nominal': 'vr_nominal',
                'Juros': 'vr_juros',
                'Prêmio': 'vr_premio',
                'Preço Unitário': 'vr_preco_unitario',
                'Critério de Cálculo': 'criterio_calculo',
                'Situação': 'situacao'},
                inplace=True
            )

            df['vr_nominal'] = df['vr_nominal'].apply(
                lambda x: x.replace('-', ''))
            df['vr_juros'] = df['vr_juros'].apply(lambda x: x.replace('-', ''))
            df['vr_premio'] = df['vr_premio'].apply(
                lambda x: x.replace('-', ''))
            df['vr_preco_unitario'] = df['vr_preco_unitario'].apply(
                lambda x: x.replace('-', ''))

            df['vr_nominal'] = df['vr_nominal'].apply(
                lambda x: x.replace('.', ''))
            df['vr_juros'] = df['vr_juros'].apply(lambda x: x.replace('.', ''))
            df['vr_premio'] = df['vr_premio'].apply(
                lambda x: x.replace('.', ''))
            df['vr_preco_unitario'] = df['vr_preco_unitario'].apply(
                lambda x: x.replace('.', ''))

            df['vr_nominal'] = df['vr_nominal'].apply(
                lambda x: x.replace(',', '.'))
            df['vr_juros'] = df['vr_juros'].apply(
                lambda x: x.replace(',', '.'))
            df['vr_premio'] = df['vr_premio'].apply(
                lambda x: x.replace(',', '.'))
            df['vr_preco_unitario'] = df['vr_preco_unitario'].apply(
                lambda x: x.replace(',', '.'))

            df['vr_nominal'] = df['vr_nominal'].apply(lambda x: x.strip())
            df['vr_juros'] = df['vr_juros'].apply(lambda x: x.strip())
            df['vr_premio'] = df['vr_premio'].apply(lambda x: x.strip())
            df['vr_preco_unitario'] = df['vr_preco_unitario'].apply(
                lambda x: x.strip())

            df['vr_nominal'] = pd.to_numeric(df['vr_nominal'], errors='coerce')
            df['vr_juros'] = pd.to_numeric(df['vr_juros'], errors='coerce')
            df['vr_premio'] = pd.to_numeric(df['vr_premio'], errors='coerce')
            df['vr_preco_unitario'] = pd.to_numeric(
                df['vr_preco_unitario'], errors='coerce')

            # salva o arquivo de saída
            print('Salvando resultado capturado no arquivo', file_path)
            df.to_csv(file_path, mode='a', index=False)

            engine = create_engine('sqlite:///data.sqlite', echo=False)
            sqlite_connection = engine.connect()
            print('Importando usando pandas to_sql')

            # df.to_sql(
            # 'swdata',
            # sqlite_connection,
            # if_exists='append',
            # index=False,
            # chunksize=50000
            # )

            for row in df.to_dict('records'):
                try:
                    scraperwiki.sqlite.save(
                        unique_keys=['data_referencia', 'ativo'],
                        data=row
                    )
                except Exception as e:
                    print("Error occurred:", e)
                    continue


if __name__ == '__main__':
    main()
