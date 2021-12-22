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

src = "D:\\Data\\SFMC\\JourneyMessageHistory_Others.csv"
dst = "D:\\Data\\SFMC\\history\\JourneyMessageHistory_Others{timestamp}.csv" .format(timestamp=today)

if not os.path.exists("D:\\Data\\SFMC\\history"):
    os.mkdir("D:\\Data\\SFMC\\history")
shutil.copyfile(src, dst)
# Make a Copy-------------------------

FORMAT = '%(asctime)s %(levelname)s: %(message)s'

with open('D:\\LOG\\GA\\update_journey_status.log', 'a') as file:
    file.write("\n\n＝＝＝＝＝＝＝＝＝＝＝＝＝＝\n")

logging.basicConfig(level=logging.DEBUG, filename='D:\\LOG\\GA\\update_journey_status.log', filemode='a', format=FORMAT)

update_speed = 2500

print("開始更新資料")
for i in range(0,3):
    try:
        
        # 與GOOGLE DATA STUDIO 串聯資料
        sheet_key = "1stuPZyyL1stbnOkUXw4QULle5LvKKKeJp40Smu3927Q"

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name("TPESE_KEY.json", scopes)
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(sheet_key).sheet1

        Journey_Status = "D:\\Data\\SFMC\\JourneyMessageHistory_Others.csv"

        update_data = pd.read_csv(Journey_Status, encoding="utf-8")
        update_data = update_data.applymap(lambda x: "" if str(x)=="nan" else x)

        sheet.batch_clear(['A2:H'])

        for i in range(0, int(len(update_data.index)/update_speed)+1):

            update_data_division = update_data[i*update_speed: update_speed+i*update_speed]

            if (sheet.get("1:1") == []):
                sheet.update([update_data.columns.values.tolist()])
                
            start_range = ("A{start}:M{end}" .format(start=2+i*update_speed, end=2+update_speed+i*update_speed))

            sheet.update(start_range, update_data_division.values.tolist())
            time.sleep(3)

        logging.debug('Successfully Updated!')

    except:
        print('Catch an exception during UPDATE PROCEDURE, try again')
        logging.debug('Catch an exception during UPDATE PROCEDURE', exc_info=True)

        continue

    break


print("備份並複製資料")
for j in range(0, 5):
    try:
        
        # 備份資料APPEND
        sheet_key = "1GhNbuF3UL9V0bg7mNNH6vC_kruyryWYkwMCcMBy4hSI"
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name("TPESE_KEY.json", scopes)
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(sheet_key).sheet1

        Journey_Status = "D:\\Data\\SFMC\\JourneyMessageHistory_Others.csv"

        update_data = pd.read_csv(Journey_Status, encoding="utf-8")
        update_data = update_data.applymap(lambda x: "" if str(x)=="nan" else x)

        for i in range(0, int(len(update_data.index)/update_speed)+1):

            update_data_division = update_data[i*update_speed: update_speed+i*update_speed]

            if (sheet.get("1:1") == []):
                sheet.update([update_data.columns.values.tolist()])

            get_sheet_index = sheet.get("A:A")
            start_row = 1 + len(get_sheet_index)
            end_row = 1 + len(get_sheet_index) + len(update_data_division.index)
            start_range = ("A{start}:M{end}" .format(start=start_row, end=end_row))

            sheet.update(start_range, update_data_division.values.tolist())
            time.sleep(3)
        logging.debug('Successfully BACKUP!')

    except:
        print('Catch an exception during BACKUP PROCEDURE', 'try again')
        logging.debug('Catch an exception during BACKUP PROCEDURE', exc_info=True)
        logging.debug('try again')
        continue
    break