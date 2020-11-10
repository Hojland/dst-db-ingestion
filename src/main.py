#%load_ext autoreload
#%autoreload 2
import pandas as pd
import re
from datetime import datetime, timedelta
import asyncio
import aiomysql
import logging
import glob

from utils import utils, sql_utils
from dst import DST
import settings


async def main():
    logger = utils.get_logger('printyboi.log')
    metadata_filelst = glob.glob(settings.METADATA_PATH.absolute().as_posix() + '/*.json')
    dst = DST()

    loop = asyncio.get_event_loop()
    mysql_engine_pool = await sql_utils.async_mysql_create_engine(loop=loop, db_config=settings.MARIADB_CONFIG, db_name=settings.MARIADB_CONFIG['db'])
    
    for metadata_file in metadata_filelst:
        logging.info(f'working on {metadata_file}')
        metadata = utils.read_json(metadata_file)

        if await sql_utils.table_exists_notempty(mysql_engine_pool, 'input', f"dst_{metadata['table_id'].lower()}"):
            latest_date = await sql_utils.get_latest_date_in_table(mysql_engine_pool, f"input.dst_{metadata['table_id'].lower()}", date_col='time')
            latest_date = datetime.strptime(str(latest_date), '%Y')
            pass
        else:
            latest_date = datetime.strptime('2014-01-01', '%Y-%m-%d')
            # should then be used to change the call dynamically to this period
        
        #await dst.get_table_info("BEBRIT08")

        time_end = re.search('(M|K)\d{1,2}', metadata['dst_variables']['Tid'][0])
        if time_end:
            metadata['dst_variables']['Tid'] = [f">{latest_date.year}{time_end[0]}"]
        else:
            metadata['dst_variables']['Tid'] = [f">{latest_date.year}"]

        try:
            df = await dst.get_table(metadata['table_id'], metadata['dst_variables'], request_type='GET', out_format=metadata['format'])
        except AssertionError as e:
            logging.info(f'failed with {e}, if concerning Tid, then it is probably the stuff in prod')
            continue

        # NOT PIVOTING OTHER THAN ON DEMAND, SINCE IT RELIES ON WHAT CAN BE INDEXED IN SINGLE DATASET, AND THAT MAY BE LESS THAN ONE VARIABLE
        # THEREFORE INDHOLD IS ALSO NOT CHANGED
        #cols = [col for col in list(df) if col not in [metadata['pivot_col'], 'INDHOLD']]
        #df = pd.pivot(df, index=cols, columns=metadata['pivot_col'], values='People').reset_index()

        dtype_trans_dct = sql_utils.get_dtype_trans(df)
        await sql_utils.create_table(mysql_engine_pool, f"input.dst_{metadata['table_id'].lower()}", col_datatype_dct=dtype_trans_dct, index_lst=metadata['index_vars'])
        await sql_utils.df_to_sql_split(mysql_engine_pool, df, f"input.dst_{metadata['table_id'].lower()}", chunksize=1000)


if __name__ == '__main__':
    asyncio.run(main())
    #await main()