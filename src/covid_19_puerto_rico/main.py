#/usr/bin/env/python3

import logging
import pandas as pd
import sqlalchemy

def main():
    # TODO: actuall do something with the data
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)
    logging.info('Hello World!')
    engine = create_db()
    with engine.connect() as connection:
        announcement = pd.read_sql_table('announcement', connection,
                                         index_col=['bulletin_date'])
        logging.info("announcement = %s", announcement)

        bitemporal_analysis = pd.read_sql_table('bitemporal_analysis', connection,
                                                index_col=['bulletin_date', 'datum_date'])
        logging.info("bitemporal_analysis = %s", bitemporal_analysis)

def create_db():
    url = sqlalchemy.engine.url.URL(
        drivername = 'postgres',
        username = 'postgres',
        password = 'password',
        host = 'localhost')
    return sqlalchemy.create_engine(url)

if __name__ == '__main__':
    main()