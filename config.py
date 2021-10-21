import os, re, logging, pyodbc
from ftplib import FTP

class Config():

    # update locations when ACS data is updated
    def __init__(self):
        self._ftpURL = "ftp2.census.gov"
        self._ftpBaseDirectory = "programs-surveys/acs/summary_file/2019/data"
        self._templatefiles = "2019_5yr_Summary_FileTemplates.zip"
        self._geofiles = "2019_ACS_Geography_Files.zip"

    def Process(self):
      logging.info(f"Configuring local data directories")
      self.ConfigureDirectory()
      logging.info(f"Download files from ACS")
      self.DownloadFilesFromACS()

    # approximate runtime 2.5 hours
    def DownloadFilesFromACS(self):
      with FTP(self._ftpURL) as ftp:
        ftp.login()
        ftp.cwd(self._ftpBaseDirectory)
        logging.info(f"Downloading {self._templatefiles}")
        localPath = os.path.join(os.getcwd(),'data\index',self._templatefiles)
        with open(localPath, 'wb') as fp:
          ftp.retrbinary(f'RETR {self._templatefiles}', fp.write)
        self.UnZipFile(localPath)
        ftp.cwd('./5_year_entire_sf') 
        logging.info(f"Downloading {self._geofiles}")
        localPath = os.path.join(os.getcwd(),'data\geo',self._geofiles)
        with open(localPath, 'wb') as fp:
          ftp.retrbinary(f'RETR {self._geofiles}', fp.write)
        self.UnZipFile(localPath)
        ftp.cwd('../5_year_by_state')
        files = []
        ftp.dir(files.append)
        fileNames = []
        for line in [x for x in files if re.search("_Tracts_Block_Groups_Only", x) is not None and re.search("UnitedStates", x) is None]:
          fields = line.strip().split(" ")
          fileNames.append(fields[len(fields) -1])
        for file in fileNames:
          logging.info(f"Downloading {file}")
          localPath = os.path.join(os.getcwd(),'data',file)
          with open(localPath, 'wb') as fp:
            ftp.retrbinary(f'RETR {file}', fp.write)

    def ConfigureDirectory(self):
      path = os.getcwd() + '\data'
      if not os.path.exists(path):
        os.makedirs(path)
      path = os.getcwd() + '\data\index'
      if not os.path.exists(path):
        os.makedirs(path)
      path = os.getcwd() + '\data\geo'
      if not os.path.exists(path):
        os.makedirs(path)
      path = os.getcwd() + '\data\extract'
      if not os.path.exists(path):
        os.makedirs(path)

    def UnZipFile(self, path):
        import zipfile
        with zipfile.ZipFile(path, 'r') as zip_ref:
          zip_ref.extractall(os.path.dirname(path))
