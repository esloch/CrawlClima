import sys
import psycopg2
import requests
from pathlib import Path
from loguru import logger
from crawlclima.config import settings
from datetime import datetime, timedelta


log_path = Path(__file__).parent / 'logs' / 'cemaden.log'
logger.add(log_path, colorize=False, retention=timedelta(days=15))
logger.add(sys.stderr, colorize=True)


def coleta_dados_cemaden(self, codigo, inicio, fim, by='uf'):
    """
    Esta tarefa captura dados climáticos de uma estação do CEMADEN, salvando os dados em um banco local.
    :param self: Instancia da task do Celery.
    :param inicio: data-hora (UTC) de inicio da captura %Y%m%d%H%M
    :param fim: data-hora (UTC) de fim da captura %Y%m%d%H%M
    :param codigo: Código da estação de coleta do CEMADEN ou código de duas letras da uf: 'SP' ou 'RJ' ou...
    :param by: uf|estacao
    :return: Status code da tarefa
    """
    conn = psycopg2.connect(**settings.DB_CONNECTION)

    if isinstance(inicio, datetime):
        inicio = inicio.strftime('%Y%m%d%H%M')

    if isinstance(fim, datetime):
        fim = fim.strftime('%Y%m%d%H%M')

    try:
        assert datetime.strptime(inicio, '%Y%m%d%H%M') < datetime.strptime(
            fim, '%Y%m%d%H%M'
        )
    except AssertionError:
        logger.error('data de início posterior à de fim.')
        raise AssertionError
    except ValueError as e:
        logger.error('Data mal formatada: {}'.format(e))
        raise ValueError

    # Check for latest records in the database
    cur = conn.cursor()
    cur.execute(
        'select datahora from "Municipio"."Clima_cemaden" ORDER BY datahora DESC '
    )
    ultima_data = cur.fetchone()
    inicio = datetime.strptime(inicio, '%Y%m%d%H%M')
    fim = datetime.strptime(fim, '%Y%m%d%H%M')
    if ultima_data is not None:
        if ultima_data[0] > inicio:
            inicio = ultima_data[0]
        if inicio >= fim:
            return

    if by == 'uf':
        url = settings.CEMADEN_DADOS_REDE
        pars = {
            'chave': settings.CEMADEN_KEY,
            'inicio': inicio.strftime('%Y%m%d%H%M'),
            'fim': fim.strftime('%Y%m%d%H%M'),
            'uf': codigo,
        }
    elif by == 'estacao':
        url = settings.CEMADEN_DADOS_PCD
        pars = {
            'chave': settings.CEMADEN_KEY,
            'inicio': inicio.strftime('%Y%m%d%H%M'),
            'fim': fim.strftime('%Y%m%d%H%M'),
            'codigo': codigo,
        }

    # puxa os dados do servidor do CEMADEN
    if fim - inicio > timedelta(hours=23, minutes=59):
        fim_t = inicio + timedelta(hours=23, minutes=59)
        data = []
        while fim_t < fim:
            pars['fim'] = fim_t.strftime('%Y%m%d%H%M')
            results = fetch_results(pars, url)
            try:
                vnames = results.text.splitlines()[1].strip().split(';')
            except IndexError as e:
                logger.warning(
                    'empty response from cemaden on {}-{}'.format(
                        inicio.strftime('%Y%m%d%H%M'),
                        fim_t.strftime('%Y%m%d%H%M'),
                    )
                )
            if not results.status_code == 200:
                continue  # try again
            data += results.text.splitlines()[2:]
            fim_t += timedelta(hours=23, minutes=59)
            inicio += timedelta(hours=23, minutes=59)
            pars['inicio'] = inicio.strftime('%Y%m%d%H%M')
    else:
        results = fetch_results(pars, url)
        if isinstance(results, Exception):
            raise self.retry(exc=results, countdown=60)
        try:
            vnames = results.text.splitlines()[1].strip().split(';')
        except IndexError:
            logger.warning(
                'empty response from cemaden on {}-{}'.format(
                    inicio.strftime('%Y%m%d%H%M'), fim.strftime('%Y%m%d%H%M')
                )
            )
        if not results.status_code == 200:
            logger.error(
                'Request to CEMADEN server failed with code: {}'.format(
                    results.status_code
                )
            )
            raise self.retry(exc=requests.RequestException(), countdown=60)
        data = results.text.splitlines()[2:]
    
    try:
        save_to_cemaden_db(cur, data, vnames)

    except Exception as e:
        logger.error(e)

    finally: 
        conn.commit()
        cur.close()
        return results.status_code


def save_to_cemaden_db(cur, data, vnames):
    """
    Saves the rceived data to the "Clima_cemaden" table
    :param cur: db cursor
    :param data: data to be saved
    :param vnames: variable names in the server's response
    :return: None
    """
    vnames = [v.replace('.', '_') for v in vnames]
    sql = 'insert INTO "Municipio"."Clima_cemaden" (valor,sensor,datahora,"Estacao_cemaden_codestacao") values(%s, %s, %s, %s);'
    for linha in data:
        doc = dict(zip(vnames, linha.strip().split(';')))
        doc['latitude'] = float(doc['latitude'])
        doc['longitude'] = float(doc['longitude'])
        doc['valor'] = float(doc['valor'])
        doc['datahora'] = datetime.strptime(
            doc['datahora'], '%Y-%m-%d %H:%M:%S'
        )
        cur.execute(
            sql,
            (doc['valor'], doc['sensor'], doc['datahora'], doc['cod_estacao']),
        )


# Disable capture
def fetch_results(pars, url):
    try:
        results = requests.get(url, params=pars)
        return results
    except requests.RequestException as e:
        logger.error('Request retornou um erro: {}'.format(e))
        results = e
    except requests.ConnectionError as e:
        logger.error('Conexão falhou com erro {}'.format(e))
        results = e
    