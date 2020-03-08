import time
import pandas as pd
import datetime
import glob
from pathlib import Path
import os
from selenium import webdriver
from celonis_tools.api import CelonisSession
from celonis_tools.model import *
from sqlalchemy import create_engine
pd.options.display.width = 0

chromedriver = 'downloads\chromedriver' # necessary to run selenium in chrome
link = 'https://yam.telekom.de/docs/DOC-406935?et=watches.email.document'
DATADIR = r"C:\Users\A92362919\Downloads"
user = '92362919'
api_token = 'c4da62b9-6173-4d8f-a0fe-b4fd8f9811d3'
api_secret = 'mC2mYGGUNBzwl56whKhEJWLkvsV3p4ZYUFnuPYCqpDDLs6TfRynDYVwC0jWMkV00'
datamodel = 100699726


def download_file():
    # dowloads .xlms file from YaM page
    driver = webdriver.Chrome(chromedriver)
    driver.get(link)
    time.sleep(5)

    try:
        driver.find_element_by_xpath('//*[@id="jive-body-main"]/div[1]/div/div[5]/section/div[2]/ul/li/div/a[1]').click()
    except Exception:
        print('Element not found')
        pass

    time.sleep(10)
    driver.quit()


def load_data(path):
    # loads data into padas dataframe
    list_of_files = glob.glob(path+'\*elektronisch*')
    latest_file = max(list_of_files, key=os.path.getctime)
    print("Latest file: {}".format(latest_file))
    df_active = pd.read_excel(latest_file, 'aktive Anbindungen')
    df_deactivated = pd.read_excel(latest_file, 'beendete Anbindungen')
    return df_active, df_deactivated


def process_active(df_active):
    # processes data for active vendors
    del df_active['Besonderheiten']
    del df_active['Buchungskreis']
    df_active = df_active.loc[(df_active['System'].isin(['PFS', 'PPL'])) & (df_active['Anbindungsmethode'] == 'Inv EDI MM') & (df_active['Anbindungstechnik'] == 'Ariba')]
    df_active = df_active.loc[df_active['Deaktivierungsdatum'].notna() == False]
    return df_active


def process_deactivated(df_deactivated):
    # processes data for inactive vendors
    del df_deactivated['Buchungskreis']
    df_deactivated = df_deactivated.loc[(df_deactivated['System'].isin(['PFS', 'PPL'])) & (df_deactivated['Anbindungsmethode'] == 'Inv EDI MM') & (df_deactivated['Anbindungstechnik'] == 'Ariba')]
    return df_deactivated


def upload_data(df_active, df_deactivated):
    # uploads data to CELONIS
    with CelonisSession(base_url="https://celonis.one-reporting.telekom.de",
                        username=user,
                        api_token=api_token,
                        api_secret=api_secret) as session:
        print("Connected to Celonis")
        dm = DataModel(datamodel)
        dm.push_table(df_active, "ARIBA_ACTIVE")
        print("ARIBA active table pushed to Celonis.")
        dm.push_table(df_deactivated, "ARIBA_DEACTIVATED")
        print("ARIBA Deactivated table pushed to Celonis.")


def SQL_INSERT_STATEMENT_FROM_DATAFRAME(df_active, df_deactivated):
    engine = create_engine('hana://CELONIS_PYTHON:Cel-Python22@10.171.56.220:38015')

    try:
        engine.execute(' SELECT TOP 100 * FROM "DTAG_DEV_CSBI_CELONIS_PYTHON"."dtag.dev.csbi.celonis.app.vim::ARIBA"e;').fetchall()
    except Exception:
        print('Table doenst exist in the database.')


def run():
    download_file()
    print("Data has been downloaded")
    df_active, df_deactivated = load_data(DATADIR)
    print("Data has been loaded.")
    process_active(df_active)
    print("Data active has been processed")
    process_deactivated(df_deactivated)
    print("Data deactivated has been processed")
    upload_data(df_active, df_deactivated)
    print("Data has been uploaded to CELONIS")
    # SQL_INSERT_STATEMENT_FROM_DATAFRAME(df_active, df_deactivated)
    # print("Data has been inserted into SQL database")


if __name__ == "__main__":
    log = open(r"C:\Users\A92362919\ariba_uploader\logfile.txt", "a")
    log.write("Started: {} \n".format(datetime.datetime.now()))
    run()
    log.write("Finished: {} \n".format(datetime.datetime.now()))
    log.close()