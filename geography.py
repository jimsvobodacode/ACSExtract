import os, logging, pyodbc
from repository import Repository
import pandas as pd
from utility import Utility

class Geography():

    def __init__(self):
        self._util = Utility()
        self._directory = os.getcwd() + '\data\\geo'

    def Process(self):
        logging.info('Loading Geography Table')
        self.ExtractData()

    # load census tracts and census blocks in geography table
    def ExtractData(self):
        dffull = None
        files = [x for x in os.listdir(self._directory) if x.endswith(".xlsx")]
        for filename in files:
            df = pd.read_excel(os.path.join(self._directory, filename), converters={'State':str,'Logical Record Number':str,'Geography ID':str})    
            dfpartial = df.iloc[:, [0,1,2]]
            dffull = pd.concat([dffull, dfpartial]) 
            logging.info(f'Loaded {filename}')
        dffull.rename(columns={ dffull.columns[0]: "STATE" }, inplace = True)
        dffull.rename(columns={ dffull.columns[1]: "LOGRECNO" }, inplace = True)
        dffull.rename(columns={ dffull.columns[2]: "GID" }, inplace = True)
        r = Repository()
        r.conn.cursor().execute("delete from geography")
        dffull.to_sql(name="GEOGRAPHY", con=r.conn, if_exists='append', index=False)
        r.conn.cursor().execute("delete from GEOGRAPHY where gid not like '15000US%' and gid not like '14000US%'")
        r.conn.cursor().execute("update GEOGRAPHY set GID_TYPE = 'T' where gid like '14000US%'")
        r.conn.cursor().execute("update GEOGRAPHY set GID_TYPE = 'B' where gid like '15000US%'")
        r.conn.cursor().execute("update GEOGRAPHY set GID = replace(replace(GID, '15000US', ''), '14000US', '')")
        r.conn.commit()
        r.Dispose()