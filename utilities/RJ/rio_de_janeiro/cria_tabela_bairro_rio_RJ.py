import logging
import os
import sys

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('criabairro')

try:
    conn = psycopg2.connect(
        "dbname='{}' user='{}' host='{}' port='{}' password='{}'".format(
            os.getenv('PSQL_DB'),
            os.getenv('PSQL_USER'),
            os.getenv('PSQL_HOST'),
            os.getenv('PSQL_PORT'),
            os.getenv('PSQL_PASSWORD'),
        )
    )
    cur = conn.cursor()
except Exception as e:
    logger.error('Unable to connect to Postgresql: {}'.format(e))
    sys.exit()


if __name__ == '__main__':
    populações = pd.read_csv('popAPRJ.csv', header=0)
    bairrosAP = pd.read_csv('bairro2AP.csv', header=0)
    APS = populações.APS
    loc_id = dict([(str(k), int(j)) for j, k in APS.iteritems()])
    sql = 'insert into "Municipio"."Bairro" (bairro_id, nome, "Localidade_id") values(%s,%s,%s);'
    for i, bairro in bairrosAP.iterrows():
        registro = (int(i), bairro.bairro, loc_id[str(bairro.APS)])
        cur.execute(sql, registro)
    conn.commit()
    cur.close
