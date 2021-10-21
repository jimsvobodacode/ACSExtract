import sqlite3, os, logging

class Repository():
    conn = None

    def __init__(self):
        self.conn = sqlite3.connect('acs_data.db')

    def Dispose(self):
        self.conn.close()

    def Configure(self, columns):
        sql = """CREATE TABLE "ACS_DATA" (
                "STATE"	TEXT NOT NULL,
                "LOGRECNO"	TEXT NOT NULL,
                "GEOGRAPHY_ID"	TEXT,
                "GEOGRAPHY_TYPE"	TEXT,
                PRIMARY KEY("STATE","LOGRECNO")
            );"""
        cmd = self.conn.cursor()
        cmd.execute(sql)
        self.conn.commit()

        for column in columns:
            sql = f"alter table ACS_DATA add column {column} INTEGER"
            cmd = self.conn.cursor()
            cmd.execute(sql)
            self.conn.commit()

        sql = """CREATE TABLE "GEOGRAPHY" (
                "STATE"	TEXT,
                "LOGRECNO"	TEXT,
                "GID"	TEXT,
                "GID_TYPE"	TEXT,
                PRIMARY KEY("STATE","LOGRECNO")
            ); """
        cmd.execute(sql)
        self.conn.commit()

        sql = """CREATE TABLE "TEMP_IMPORT" (
                "STATE"	TEXT,
                "LOGRECNO"	TEXT,
                "VALUE"	INTEGER,
                PRIMARY KEY("STATE","LOGRECNO")
            ); """
        cmd.execute(sql)
        self.conn.commit()

    def ExportToMSSQL(self):
        import pandas as pd
        import pyodbc, shutil
        logging.info('Loading Data to MSSQL')

        # save main table as tab delimited text file
        filename = "acs_data.txt"
        df = pd.read_sql_query("SELECT * FROM ACS_DATA", self.conn)
        df = df.astype(str)
        df = df.replace(to_replace = "\.0+$",value = "", regex = True)  # remove trailing decimal (.0) added by pandas
        df = df.replace(to_replace = "nan", value = "", regex = True)   # set null fields to blanks
        df.to_csv(filename, index=False, sep = "\t")

        # bulk import data into SQL server
        server_directory = r"[network share here]"
        shutil.copyfile(os.path.join(os.getcwd(), filename), os.path.join(server_directory, filename))
        conn = pyodbc.connect("[SQL connection string here]")
        conn.cursor().execute("delete from ACS_DATA")
        sql = f"""bulk insert
            ACS_DATA
            from
            '{os.path.join(server_directory, filename)}'
            with
            (firstrow = 2,
            fieldterminator = '\\t');"""
        conn.cursor().execute(sql)
        conn.commit()