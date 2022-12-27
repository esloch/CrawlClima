def coleta_dados_cemaden(codigo, inicio, fim, by='uf'):
    """
    Esta tarefa captura dados climáticos de uma estação do CEMADEN, salvando os dados em um banco local.
    :param inicio: data-hora (UTC) de inicio da captura %Y%m%d%H%M
    :param fim: data-hora (UTC) de fim da captura %Y%m%d%H%M
    :param codigo: Código da estação de coleta do CEMADEN ou código de duas letras da uf: 'SP' ou 'RJ' ou...
    :param by: uf|estacao
    :return: Status code da tarefa
    """
    conn = get_connection()
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
        url = cemaden.url_rede
        pars = {
            'chave': cemaden.chave,
            'inicio': inicio.strftime('%Y%m%d%H%M'),
            'fim': fim.strftime('%Y%m%d%H%M'),
            'uf': codigo,
        }
    elif by == 'estacao':
        url = cemaden.url_pcd
        pars = {
            'chave': cemaden.chave,
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

    save_to_cemaden_db(cur, data, vnames)

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
