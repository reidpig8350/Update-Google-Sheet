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

for p in range(0,2):
    try:
        # 與 GOOGLE DATA STUDIO 串聯資料
        sheet_key = [
            '1zBPL_pnV4MvdeLPKC7V8-aHG8r263-oQhLVrOMZ4WYY',
            '1SOkE5Rhgj_8Gz4Z8CqxOI9gofkMmz4IQxepTFF5x4Zg',
            '1M0PoabiySfcbMUgKOMOKNLqQhKzWUBO4XoSVjonouc0',
            '1Bw2d3julQJlqMgyxKCjrIGE06vJp0puEEXkt3wNvf48',
            '1TknRLmk1_TlMOIH7KJ-3qAOR71lxQuh11G5HY8b-9gY',
            '1KOb7fPyRQhXZ8L6yrCjHa7OU6cxbrZ-CA1wMmZsbfFY',
            '1SuL6ZuRPOCtGDFx-nGur1wEfXevWPXBvdpt0B47Ek-U',
            '1Xu1BcJZijQl5_GZzuNom_6aSBwu8axGNC-KTQ7aCnLs',
            '1iOxtRznybSM_rCvJrzcCWWnrQ5Yz1rw5ezfDFbTUQiY',
            '1bh5u4SfsBsDvutfuhUw8EabFEQI7mFHFaZ84vqLDrvQ'
        ]

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name("TPESE_KEY.json", scopes)
        client = gspread.authorize(credentials)

        Journey_Status = "D:\\Data\\SFMC\\JourneyMessageHistory_Others.csv"

        update_data = pd.read_csv(Journey_Status, encoding="utf-8",
            dtype={'system_id': str,'sent_date__c': str,'content_name__c': str,'journey_content__r_a_b_test__c': str,'type__c': str,'card_no__c': str,'card_type__c': str,'status__c': str,'arrival_station__c': str,'departure_station__c': str,'pnr_number': str,'utm_content__c': str,'birthday_event_date': str})
        update_data = update_data.applymap(lambda x: "" if str(x)=="nan" else x)

        for the_key in sheet_key:
            sheet = client.open_by_key(the_key).sheet1
            sheet.batch_clear(['A2:M'])

        sheet_number = 0
        for i in range(0, int(len(update_data.index)/update_speed)+1):

            if (i!=0 and (i*update_speed%100000 == 0)):
                sheet_number+=1
            sheet = client.open_by_key(sheet_key[sheet_number]).sheet1
   
            if (i*update_speed%100000 == 0):
                
                k = 0
            
            for j in range(0, 3):
                try:
                    update_data_division = update_data[i*update_speed: update_speed+i*update_speed]

                    if (sheet.get("1:1") == []):
                        sheet.update([update_data.columns.values.tolist()])
                        
                    start_range = ("A{start}:M{end}" .format(start=2+k*update_speed, end=2+update_speed+k*update_speed))

                    sheet.update(start_range, update_data_division.values.tolist())
                    k += 1
                    time.sleep(3)
                except:
                    print(i, "Catch an exception, take a timeout for 3 seconds...")
                    time.sleep(3)
                    continue
                break
        
    except:
        print('Catch an exception during UPDATE PROCEDURE, try again')
        logging.debug('Catch an exception during UPDATE PROCEDURE', exc_info=True)
        continue
    break