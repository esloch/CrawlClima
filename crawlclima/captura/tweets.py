#!/usr/bin/env python
import csv
import sys
import psycopg2
import requests
from io import StringIO
from pathlib import Path
from loguru import logger
from itertools import islice
from crawlclima import config
from datetime import datetime


with open(f'{Path(__file__).parent.parent}/utils/municipios') as f:
    municipios = f.read().split('\n')

municipios = list(filter(None, municipios))


def fetch_tweets(self, inicio, fim, cidades=None, CID10='A90'):
    """
    Tarefa para capturar dados do Observatorio da dengue para uma ou mais cidades

    :param CID10: código CID10 para a doença. default: dengue clássico
    :param inicio: data de início da captura: yyyy-mm-dd
    :param fim: data do fim da captura: yyyy-mm-dd
    :param cidades: lista de cidades identificadas pelo geocódico(7 dig.) do IBGE - lista de strings.
    :return:
    """
    conn = psycopg2.connect(**config.DB_CONNECTION)

    geocodigos = []
    for c in cidades:
        if c == '':
            continue
        if len(str(c)) == 7:
            geocodigos.append((c, c[:-1]))
        else:
            geocodigos.append((c, c))
    cidades = [c[1] for c in geocodigos]  # using geocodes with 6 digits

    params = (
        'cidade='
        + '&cidade='.join(cidades)
        + '&inicio='
        + str(inicio)
        + '&fim='
        + str(fim)
        + '&token='
        + config.INWEB_TOKEN
    )
    try:
        resp = requests.get('?'.join([config.INWEB_URL, params]))
        logger.info('URL ==> ' + '?'.join([config.INWEB_URL, params]))
    except requests.RequestException as e:
        logger.error(f'Request retornou um erro: {e}')
        raise self.retry(exc=e, countdown=60)
    except ConnectionError as e:
        logger.error(f'Conexão ao Observ. da Dengue falhou com erro {e}')
        raise self.retry(exc=e, countdown=60)
    try:
        cur = conn.cursor()
    except NameError as e:
        logger.error(
            'Not saving data because connection to database could not be established.'
        )
        raise e
    header = ['data'] + cidades
    fp = StringIO(resp.text)
    data = list(csv.DictReader(fp, fieldnames=header))
    for i, c in enumerate(geocodigos):
        sql = """
            INSERT INTO "Municipio"."Tweet" (
                "Municipio_geocodigo",
                data_dia ,
                numero,
                "CID10_codigo")
                VALUES(%s, %s, %s, %s);
        """
        for r in data[1:]:
            try:
                cur.execute(
                    """
                    SELECT * FROM "Municipio"."Tweet"
                    WHERE "Municipio_geocodigo"=%s
                    AND data_dia=%s;""",
                    (int(c[0]), datetime.strptime(r['data'], '%Y-%m-%d')),
                )
            except ValueError as e:
                print(c, r)
                raise e
            res = cur.fetchall()
            if res:
                continue
            cur.execute(
                sql,
                (
                    c[0],
                    datetime.strptime(r['data'], '%Y-%m-%d').date(),
                    r[c[1]],
                    CID10,
                ),
            )
    conn.commit()
    cur.close()

    with open('/opt/services/log/capture-pegatweets.log', 'w+') as f:
        f.write('{}'.format(resp.text))

    return resp.status_code


def chunk(it, size):
    """
    divide a long list into sizeable chuncks
    :param it: iterable
    :param size: chunk size
    :return:
    """
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())
