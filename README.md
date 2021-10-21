<p align="center">

  <h2 align="center">ACS Extract</h3>

  <p align="center">

  </p>
</p>


<!-- ABOUT THE PROJECT -->
## About The Project


ACS Extract will download the current (2019) American Community Survey data files from the United States Census Bureau and parse out select columns and their corresponding geographies into a sqlite3 database. Optionally the data can be stored as a flat file and bulk imported into SQL Server.


### Built With

* Python 3.9.X
* Uses packages sqlite3, pyodbc, pandas (for file export)
* Visual Studio Code


<!-- GETTING STARTED -->
## Getting Started

To get a local copy up and running follow these simple steps.  Process has been tested on Windows 10 & Windows 11.

### Prerequisites

If not done already, download and install the current version of Python from [python.org](https://www.python.org/).

You may also want to install [DB Browser](https://sqlitebrowser.org/) to view the data in the sqlite3 database.


### Installation

1. Clone the repo
   ```
   git clone https://github.com/jimsvobodacode/ACSExtract.git
   ```
2. Install pip packages
   ```
   pip install pandas, pyodbc
   ```


<!-- USAGE EXAMPLES -->
## Usage

1. Open Powershell in Windows
2. Navigate to your project directory
3. Run ```python app.py```

Note: The initial file download from the United States Census Bureau takes approximately 2.5 hours. 


<!-- CONTRIBUTING -->
## Contributing

I'm only accepting bug fixes at this time.




<!-- CONTACT -->
## Contact

Jim Svoboda - jim.svoboda@unmc.edu - email