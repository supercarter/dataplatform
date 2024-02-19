import requests
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd
from .pd_combine_dupes import combine_dupes
import os
import re

# Configure logging
#logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)


class NHANESDataDownloader:
    def __init__(self, BASE_YEAR=2017, target=os.getcwd()):
        self.base_url = f"https://wwwn.cdc.gov/nchs/nhanes/continuousnhanes/default.aspx?BeginYear={BASE_YEAR}"
        self.id = 'SEQN'
        self.title = None
        self.target = target
        self.df = pd.DataFrame()

    def get_soup(self, url):
        response = requests.get(url)
        return BeautifulSoup(response.text, 'html.parser')

    def find_data_urls(self):
        soup = self.get_soup(self.base_url)

        # Find all links, then narrow down to data URLs
        links = soup.find_all('a', href=True)

        # Grab title (years of NHANES data) for file-naming purposes
        self.title = soup.find('h1', href=False).text.strip().replace(" ", "_")

        # Grab datatypes (usually [demographics, dietary, examination, laboratory, questionnaire, limited])
        datatypes = [re.split(r'\n|\t', datatype.text.strip())[-1].split(" ")[0].lower() for datatype in links if datatype['href'].startswith(('../search'))]
        data_urls_list = [urljoin(self.base_url, data_url['href']) for data_url in links if data_url['href'].startswith(('../search'))]
        
        # Zip datatype with list of links. Return result
        data_urls = dict(zip(datatypes, data_urls_list))
        # Delete "limited" file if it exists
        data_urls.pop("limited", "")

        return data_urls

    def extract_and_convert_xpt(self, url, datatype):

        #logger.info(f'Extracting {datatype} file...')
        
        soup = self.get_soup(url)

        # Find all links, then narrow down to XPT files
        links = soup.find_all('a', href=True)
        xpt_files = [link['href'] for link in links if link['href'].lower().endswith(('.xpt'))]
        dframes = []

        # Download and convert each file
        for xpt in xpt_files:
            try:
                 # Ignore unimportant files
                if self.is_unimportant_file(xpt):
                    #logger.debug(f"{os.path.basename(xpt)} was skipped because it has duplicate id values or is too large")
                    continue
                # Grab file, and add to queue if passes processing
                xpt_df = self.read_and_process_xpt(url, xpt, datatype)

                dframes.append(xpt_df)

            except Exception as e:
                #logger.debug(f"Did not process file {os.path.basename(xpt)}. Error: {e}")
                pass
        # Clean up data: remove duplicate columns
        df_current = combine_dupes(pd.concat(dframes, axis=1))
        # Replace values smaller than the threshold=10**-30 with 0
        num = df_current._get_numeric_data()
        num[num < 10E-30] = 0

        # Drop "SAMPLEID" column, as it was only relevant to pooled data.
        try:
            df_current.drop("SAMPLEID", axis=1)
        except KeyError:
            pass
        
        # add to main dataframe
        self.df = pd.concat([self.df, df_current], axis=1)


     # These files are not important or are too large
    def is_unimportant_file(self, xpt):
        lst = ["DR1IFF", "DR2IFF", "DSII", "AUXAR", "PAXHR", "PAXMIN"]
        for l in lst:
            if re.search(l, xpt):
                return True
        return False

    def read_and_process_xpt(self, url, xpt, datatype):
        xpt_url = urljoin(url, xpt)
        xpt_df = pd.read_sas(xpt_url)
        filename = os.path.basename(xpt)

        # Function to decode bytes literals to strings
        def decode_text(encoded_text):
            try:
                decoded_text = encoded_text.decode('utf-8') 
                return decoded_text
            except AttributeError:
                return encoded_text 
        # Apply the decoding function to all columns
        for column in xpt_df.columns:
            xpt_df[column] = xpt_df[column].apply(decode_text)

        # For lab data, drop columns ending with "LC". These are comment codes.
        if datatype == "laboratory":
            LC_cols = xpt_df.columns.str.endswith('LC')
            LC_count = LC_cols.sum()
            xpt_df = xpt_df.loc[:, ~LC_cols]
            if LC_count > 0:
                #logger.debug(f"Skipped {LC_count} variables in {filename} because they are large, unimportant comment codes")
                pass
        # For examination data, drop Aux files which are large sensor data.
        if datatype == "examination":
            AUX_cols = xpt_df.columns.str.startswith(("WBX", "TYX"))
            AUX_count = AUX_cols.sum()
            xpt_df = xpt_df.loc[:, ~AUX_cols]
            if AUX_count > 0:
                #logger.debug(f'Skipped {AUX_count} variables in {filename} because they are large, unhelpful sensor data')
                pass
        if self.id not in xpt_df.columns:
            #logger.debug(f"{filename} skipped because it's not based on individual participants")
            return pd.DataFrame()
        if not xpt_df[self.id].duplicated().any():
            xpt_df.set_index(self.id, inplace=True)
            return xpt_df
        else:
            #logger.debug(f"{filename} skipped because it has duplicate id values")
            return pd.DataFrame()

