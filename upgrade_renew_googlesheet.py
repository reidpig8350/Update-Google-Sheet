import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
import time

# Make a Copy-------------------------
from datetime import datetime
today = datetime.today().strftime("%Y%m%d")

import shutil
import os

src = "D:\\CSV\\Official Data\\CRM_JOURNEY_UPGRADE.csv"
dst = "D:\\CSV\\Official Data\\history\\upgrade\\CRM_JOURNEY_UPGRADE{timestamp}.csv" .format(timestamp=today)

if not os.path.exists("D:\\CSV\\Official Data\\history\\upgrade"):
    os.mkdir("D:\\CSV\\Official Data\\history")
    os.mkdir("D:\\CSV\\Official Data\\history\\upgrade")
shutil.copyfile(src, dst)

src = "D:\\CSV\\Official Data\\CRM_JOURNEY_RENEW.csv"
dst = "D:\\CSV\\Official Data\\history\\renew\\CRM_JOURNEY_RENEW{timestamp}.csv" .format(timestamp=today)

if not os.path.exists("D:\\CSV\\Official Data\\history\\renew"):
    os.mkdir("D:\\CSV\\Official Data\\history\\renew")
shutil.copyfile(src, dst)
# Make a Copy-------------------------

FORMAT = '%(asctime)s %(levelname)s: %(message)s'

with open('D:\\LOG\\GA\\uptogooglesheet.log', 'a') as file:
    file.write("\n\n＝＝＝＝＝＝＝＝＝＝＝＝＝＝\n")

logging.basicConfig(level=logging.DEBUG, filename='D:\\LOG\\GA\\uptogooglesheet.log', filemode='a', format=FORMAT)

try:

    # UPGRADE SHEET
    sheet_key1 = "1MJHt_f3dTSxbjsunK_i7K-hW02KX-QnfMPkhIEbSQ8Y"
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(".\\TPESE_KEY.json", scopes)
    client = gspread.authorize(credentials)
    upgrade_sheet = client.open_by_key(sheet_key1).sheet1
    # UPGRADE SHEET

    # RENEW SHEET
    sheet_key2 = "1b5I37S8IiVWnftJsDvDf8gcyIpVU250qBZcf0-DJvWw"
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(".\\TPESE_KEY.json", scopes)
    client = gspread.authorize(credentials)
    renew_sheet = client.open_by_key(sheet_key2).sheet1
    # RENEW SHEET

    # ---Route of Latest Data---

    JourneyMessageHistory_Upgrade = "D:\\CSV\\Official Data\\CRM_JOURNEY_UPGRADE.csv"
    JourneyMessageHistory_Renew = "D:\\CSV\\Official Data\\CRM_JOURNEY_RENEW.csv"

    # ---Route of Latest Data---

    def updateSheets(JourneyMessageHistory, google_sheet, update_speed=2500):
        for p in range(0, 5):
            try:
                update_data = pd.read_csv(JourneyMessageHistory, encoding="ANSI").dropna(axis=0, how="all")
                update_data = update_data.applymap(lambda x: "" if str(x)=="nan" else x) # 由於 Google Sheets 無法轉換空白欄位, 將 nan 取代為 ""
                google_sheet.batch_clear(['A2:L'])
                for i in range(0, int(len(update_data.index)/update_speed)+1):

                    update_data_division = update_data[i*update_speed: update_speed+i*update_speed]

                    if (google_sheet.get("1:1") == []):
                        google_sheet.update([update_data.columns.values.tolist()])

                    start_range = ("A{start}:L{end}" .format(start=2+i*update_speed, end=2+update_speed+i*update_speed))
                    google_sheet.update(start_range, update_data_division.values.tolist())
                    time.sleep(3)
            except:
                continue
            break

    updateSheets(JourneyMessageHistory_Upgrade, upgrade_sheet)
    updateSheets(JourneyMessageHistory_Renew, renew_sheet)
    logging.debug('Successfully Updated!')

except:
    logging.debug('Catch an exception.', exc_info=True)