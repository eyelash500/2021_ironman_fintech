import requests
import pandas
from io import StringIO

import db_connect


class taiex_history:
    """Get the CSV which is the data TAIEX's open, high, low, close.
    name: 發行量加權股價指數歷史資料
    download from: https://www.taifex.com.tw/cht/3/futAndOptDateView?menuid1=03
    code: utf8
    type: CSV
    """

    """
    範本：taiex_20210909.csv
    CSV:
        columns:
            日期,開盤指數,最高指數,最低指數,收盤指數
        data:
            "1100901","17464","17504","17416","17474"
    """

    def __init__(self) -> None:
        self.title = "發行量加權股價指數歷史資料"
        # 預設值，之後如果有修改URL可以用成參數帶入
        self.url = (
            "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=open_data"
        )
        self.df = None  # 把資料從csv轉乘datframe
        self.csv = self.get_csv_data()  # 取得網路上的資料，格式為csv

        if type(self.csv) is False:
            print(f"無法取得資料:{self.title}")

    def get_csv_data(self, url=None) -> bool:
        """取得CSV內的資料，並轉成Dataframe，回傳成功與否

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示去得失敗或是轉換失敗。
        """
        if url:
            self.url = url

        try:
            csv = requests.get(self.url)
            df = pandas.read_csv(StringIO(csv.text))  # 有header
            print(df)  # debug
            self.df = df
        except Exception as exc:
            print(exc)
            return False

        return True

    def insert_mysql(self) -> bool:
        # 計數器：計算新增了幾筆
        counter = 0

        try:
            # 建立connection物件
            my_connt_obj = db_connect.mysql_connect()
            conn = my_connt_obj.connect()
            with conn.cursor() as cursor:
                # 新增SQL語法
                for _, row in self.df.iterrows():
                    trade_date = f"{str(int(row[0]/10000)+1911)}-{str(row[0])[3:5]}-{str(row[0])[5:8]}"
                    cmd = f"""INSERT IGNORE INTO Taiex
                    (TradeDate, Open, High, Low, Close)
                    VALUES('{trade_date}',
                    '{row[1]}', {row[2]}, {row[3]}, {row[4]});"""
                    cursor.execute(cmd)

                    counter += 1
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {counter}==")
        return True


csv_data = taiex_history()
r = csv_data.get_csv_data()
if r:
    r = csv_data.insert_mysql()
else:
    print("False")
