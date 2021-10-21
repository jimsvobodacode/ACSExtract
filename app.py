import logging
from worker import Worker
from repository import Repository
from geography import Geography
from utility import Utility
from config import Config

# Jim Svoboda - 9/3/21
# config
# - approximate runtime is 1 hour (not including ACS file download)
# required packages
# - pip install pyodbc, pandas, openpyxl
class App:

    def __init__(self):  
        self._util = Utility()
        self._util.ConfigureLogging()
        self._uniqueColumns = []

        #specify columns to extract from ACS files
        self._columnsBIRD = ["B01001_001",
            "B15003_001","B15003_002","B15003_003","B15003_004","B15003_005","B15003_006","B15003_007","B15003_008","B15003_009","B15003_010",
            "B15003_011","B15003_012","B15003_013","B15003_014","B15003_015","B15003_016","B15003_017","B15003_018","B15003_019","B15003_020",
            "B15003_021","B15003_022","B15003_023","B15003_024","B15003_025",
            "B17017_001","B17017_002",
            "B19013_001",
            "B19057_001","B19057_002",
            "B23007_001","B23007_026",
            "B23022_002","B23022_026"]
        self._columnsCRANE = ["B01001_001","B01001_002","B01001_003","B01001_004","B01001_005","B01001_006","B01001_007","B01001_008","B01001_009","B01001_010",
            "B01001_020","B01001_021","B01001_022","B01001_023","B01001_024","B01001_025","B01001_026","B01001_027","B01001_028","B01001_029",
            "B01001_030","B01001_031","B01001_032","B01001_033","B01001_034","B01001_035","B01001_036","B01001_037","B01001_038","B01001_039",
            "B01001_040","B01001_041","B01001_042","B01001_043","B01001_044","B01001_045","B01001_046","B01001_047","B01001_048","B01001_049",
            "B15003_001","B15003_002","B15003_003","B15003_004","B15003_005","B15003_006","B15003_007","B15003_008","B15003_009","B15003_010",
            "B15003_011","B15003_012","B15003_013","B15003_014","B15003_015","B15003_016","B15003_017","B15003_018","B15003_019","B15003_020",
            "B15003_021","B15003_022","B15003_023","B15003_024","B15003_025",
            "B01001G_001","B01001H_001","B01001I_001","B01002B_001","B01001C_001","B01001D_001","B01001E_001","B01001F_001",
            "B19058_001","B19058_002","B08122_001","B08122_002","B08122_003","B08122_004",
            "B23025_001","B23025_002",
            "B25070_001","B25070_007","B25070_008","B25070_009","B25070_010","B25010_001",
            "B25034_001","B25034_007","B25034_008","B25034_009","B25034_010","B25034_011",
            "B25048_001","B25048_003",
            "B25052_001","B25052_003",
            "B28001_001","B28001_002","B28001_011",
            "B28002_001","B28001_004",
            "B08136_001","B08301_002","B08301_010","B08301_017","B08301_018","B08301_019",
            "B08126_002","B08126_003","B08126_004","B08126_005","B08126_006","B08126_007","B08126_008","B08126_009","B08126_010","B08126_011","B08126_012","B08126_013","B08126_014","B08126_015",
            "B992513_001","B992513_003",
            "C18120_001","C18120_002"]
        self._uniqueColumns = list(set(self._columnsBIRD + self._columnsCRANE))
        self._uniqueColumns.sort()

    def Process(self):
        try:
            logging.info('*** Start ***')
            #Config().Process()                          # download all files from ACS FTP (comment out after files are downloaded)
            Repository().Configure(self._uniqueColumns) # create local db.  delete acs_data.db (if exists) file for a clean start
            Geography().Process()                       # load geography table
            Worker().Process(self._uniqueColumns)       # extract all the specified column data into sqlite db
            #Repository().ExportToMSSQL()               # (optional) export sqlite data to text file and to MSSQL
            logging.info('*** Stop ***')
        except:
            logging.exception("")


app = App()
app.Process()