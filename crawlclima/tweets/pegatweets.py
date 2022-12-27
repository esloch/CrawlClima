#!/usr/bin/env python
"""
Fetch a week of tweets
Once a week go over the entire year to fill in possible gaps in the local database
requires celery worker to be up and running
but this script will actually be executed by cron
"""
import sys
from datetime import date, timedelta
from itertools import islice

from crawlclima.config.settings import local
from crawlclima.celery.tasks import pega_tweets

sys.path.insert(0, local)


# Data inicial da captura
today = date.fromordinal(date.today().toordinal())
week_ago = date.fromordinal(date.today().toordinal()) - timedelta(8)
year_start = date(date.today().year, 1, 1)

with open(f'{local}/crawlclima/municipios') as f:
    municipios = f.read().split('\n')

municipios = list(filter(None, municipios))


def pega_tweets(self, inicio, fim, cidades=None, CID10='A90'):
    """
    Tarefa para capturar dados do Observatorio da dengue para uma ou mais cidades

    :param CID10: código CID10 para a doença. default: dengue clássico
    :param inicio: data de início da captura: yyyy-mm-dd
    :param fim: data do fim da captura: yyyy-mm-dd
    :param cidades: lista de cidades identificadas pelo geocódico(7 dig.) do IBGE - lista de strings.
    :return:
    """
    conn = get_connection()
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
        + token
    )
    try:
        resp = requests.get('?'.join([base_url, params]))
        logger.info('URL ==> ' + '?'.join([base_url, params]))
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


if today.isoweekday() == 5:
    date_start = year_start
else:
    date_start = week_ago

if len(sys.argv) > 1:
    data = sys.argv[1].split('-')
    date_start = date(int(data[0]), int(data[1]), int(data[2]))

for cidades in chunk(municipios, 50):
    pega_tweets.delay(
        date_start.isoformat(), today.isoformat(), cidades, 'A90'
    )
