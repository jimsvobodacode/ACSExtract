import os, logging, re
import pandas as pd
from utility import Utility
from repository import Repository

class Worker():

    def __init__(self):
        self._util = Utility()
        self._dataDirectory = os.getcwd() + '\data'
        self._extractDirectory = os.getcwd() + '\data\extract'
        self._indexDirectory = os.getcwd() + '\data\index'

    def Process(self, columns):
        for column in columns:
            logging.info(f"Processing Column {column}")
            logging.info(f"-finding column {column}")
            columnDetails = self.FindColumn(column)
            #columnDetails = ('0042', 124)
            if columnDetails is not None:
                logging.info(f"-extracting files")
                self.UnZipSpecificFiles(columnDetails[0])
                logging.info(f"-extracting data")
                self.AddRowForAllGeographies()
                df = self.ExtractData(columnDetails[0], columnDetails[1])
                self.UpdateACSData(column, df)
    
    # extract given column's data from every states' file
    def ExtractData(self, fileIndex, columnIndex):
        files = [x for x in os.listdir(self._extractDirectory) if re.match('e\d{5}\w{2}' + fileIndex + '000.txt', x)]
        dffull = None
        for file in files:
            df = pd.read_csv(os.path.join(self._extractDirectory, file), header=None, dtype=str)    
            dfpartial = df.iloc[:, [2, 5, columnIndex]]
            dfpartial = dfpartial.loc[dfpartial.iloc[:, 2] != '.']  # remove rows with empty data
            dffull = pd.concat([dffull, dfpartial]) 
            #break   # testing
        dffull.rename(columns={ dffull.columns[0]: "STATE" }, inplace = True)
        dffull.rename(columns={ dffull.columns[1]: "LOGRECNO" }, inplace = True)
        dffull.rename(columns={ dffull.columns[2]: "VALUE" }, inplace = True)
        dffull["STATE"] = dffull["STATE"].str.upper()
        dffull["VALUE"] = pd.to_numeric(dffull["VALUE"])
        return dffull

    def UpdateACSData(self, column, df):
        r = Repository()
        r.conn.cursor().execute("delete from TEMP_IMPORT")
        r.conn.commit()
        df.to_sql(name="TEMP_IMPORT", con=r.conn, if_exists='append', index=False)
        r.conn.cursor().execute(f"""UPDATE ACS_DATA
            SET {column} = VALUE 
            FROM temp_import
            WHERE temp_import.STATE = ACS_DATA.STATE and temp_import.LOGRECNO = ACS_DATA.LOGRECNO""")
        r.conn.commit()

    def AddRowForAllGeographies(self):
        r = Repository()
        r.conn.cursor().execute("""INSERT INTO ACS_DATA (STATE, LOGRECNO, GEOGRAPHY_ID, GEOGRAPHY_TYPE)
            SELECT STATE, LOGRECNO, GID, GID_TYPE FROM GEOGRAPHY
            WHERE NOT EXISTS (SELECT * FROM ACS_DATA 
            WHERE GEOGRAPHY.STATE = ACS_DATA.STATE AND 
            GEOGRAPHY.LOGRECNO = ACS_DATA.LOGRECNO)""")
        r.conn.commit()

    # unzip specific files that match a certain file name
    def UnZipSpecificFiles(self, fileIndex):
        import zipfile
        self._util.ClearFolder(self._extractDirectory)
        files = [x for x in os.listdir(self._dataDirectory) if x.endswith(".zip")]
        for filename in files:
            with zipfile.ZipFile((self._dataDirectory + "\\" + filename), 'r') as zip_ref:
                files = [x for x in zip_ref.namelist() if re.match('e\d{5}\w{2}' + fileIndex + '000.txt', x)]
                for efile in files:
                    zip_ref.extract(efile, self._extractDirectory)
                    
    # file which file number a given column exists in using the index files
    def FindColumn(self, columnName):
        files = [x for x in os.listdir(self._indexDirectory) if x.endswith(".xlsx")]
        for filename in files:
            df = pd.read_excel(self._indexDirectory + "\\" + filename)
            columns = df.columns.values.tolist()
            for column in columns:
                if column == columnName:
                    m = re.match("seq(?P<value>\d+)\.xlsx", filename)
                    #print(f"{columnName}, {df.iloc[0, columns.index(column)]}".replace("%",":"))
                    return (m.group("value").zfill(4), columns.index(column))