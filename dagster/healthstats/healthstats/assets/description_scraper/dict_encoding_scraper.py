import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


class NHANES_Descriptions_Downloader:
    def __init__(self, BASE_YEAR=2017, target=os.getcwd()):
        self.BASE_YEAR = BASE_YEAR
        self.target = target
        self.datatypes = ['demographics', 'dietary', 'examination', 'laboratory', 'questionnaire']
        self.linkbase = 'https://wwwn.cdc.gov'
        
        # Define descriptions dataframe
        self.header = ["Label", "Plain Description", "Target", "DataType"]
        self.descDf = pd.DataFrame(columns=self.header)
        self.descDf.index.name = "Variable"

        # Define encoding dataframe
        self.encDf = pd.DataFrame(columns=['Encoding'])
        self.encDf.index.name = "Variable"

        # Flag to grab SEQN data (participant ID) only once per datatype
        self.SEQN_flag = True
        # Count skipped variables
        self.LC_count = 0
        self.AUX_count = 0

    def return_datatypes(self):
        return self.datatypes
    
    def file_filter(self, name):
        # Files to Skip
        skip_list = ("DR1IFF", "DR2IFF", "DSII", "AUXAR", "PAXHR", "PAXMIN", 'DRXFCD', 'DSBI', 'DSPI', 'DS1IDS','DS2IDS','DSQIDS','AUXTYM','AUXWBR','RXQ_RX')
        return not name.startswith(skip_list)

    def var_filter(self, name):
        if name == "SEQN" and not self.SEQN_flag:
                return False
        if name.endswith("LC"):
            self.LC_count += 1
            return False
        lst = ("WBX", "TYX")
        if name.startswith(lst):
            self.AUX_count += 1
            return False
        if name == "SAMPLEID":
            return False
        return True

    def strip(self, s):
        return s.strip().replace('\r', '').replace('\n', '').replace('\t', '')

    def grab_description(self, var, df, datatype):
        info = var.find_all('dd')
        name = self.strip(info[0].text)
        Label = self.strip(info[1].text)
        Plain_Description = self.strip(info[2].text)
        numinfo = len(info) 
        if numinfo >= 4:
            Target = self.strip(info[numinfo - 1].text)
        else:
            Label = None
            Plain_Description = self.strip(info[1].text)
            Target = self.strip(info[2].text)
        df.loc[name] = {self.header[0]: Label, self.header[1]: Plain_Description, self.header[2]: Target, self.header[3]: datatype}

    def grab_encoding(self, var, df):
        # Initialize dictionary for variable encodings
        m = {}
        name = var.find('h3')["id"]
        if name == "SEQN":
            self.SEQN_flag = False
        table = var.find("table")
        if table:
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all(['td'])[:2]
                code = self.strip(cols[0].text)
                if "to" not in code:
                    codeDesc = self.strip(cols[1].text)
                    m[code] = codeDesc
            df.loc[name] = {"Encoding": m}
            
                        
    def grab_info(self, url, datatype):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        vars = soup.find_all("div", class_="pagebreak")
        if len(vars) > 0 and vars[0].find('h3')["id"] == "SEQN":
            for var in vars:
                name = var.find('h3')["id"]
                if self.var_filter(name):
                    self.grab_description(var, self.descDf, datatype)
                    self.grab_encoding(var, self.encDf)
        else:
            return

    def extract_datatype(self, datatype):
        url = f'https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component={datatype}&CycleBeginYear={self.BASE_YEAR}'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find('table')
        rows = table.find_all('tr')

        # Flags to grab SEQN variable only once per datatype
        self.SEQN_flag = True

        # Go row by row and save data
        for row in rows[1:]:
            col = row.find_all(['td'])[1]
            name = col.find('a').text
            if self.file_filter(name):
                filelink = self.linkbase + col.find('a')["href"]
                self.grab_info(filelink, datatype)


