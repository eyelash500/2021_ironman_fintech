import requests
import pandas
from io import StringIO
import os

import db_connect


class daily_market_info:
    """Get the CSV which is the data TAIEX's open, high, low, close.
    name: 盤後資訊 > 每日市場成交資訊
    download from: https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data
    code: utf8
    type: CSV
    """

    """
    範本：每日市場成交資訊_202109.csv
    CSV:
        columns:
            日期,開盤指數,最高指數,最低指數,收盤指數
        data:
            "1100901","17464","17504","17416","17474"
    """

    def __init__(self) -> None:
        self.title = "盤後資訊 > 每日市場成交資訊"
        self.df = None  # 把資料從csv轉乘datframe
        self.csv = self.get_csv_data()  # 取得網路上的資料，格式為csv

        if type(self.csv) is False:
            print(f"無法取得資料:{self.title}")

    def get_csv_data(self, url=None, path=None) -> bool:
        """取得CSV內的資料，並轉成Dataframe，回傳成功與否。

        Args:
            param1 (str): 資料的url
            param2 (str): CSV資料的路徑
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示去得失敗或是轉換失敗。
        """

        try:
            if path is None:
                # 從網路取得CSV檔案
                csv = requests.get(url)
                df = pandas.read_csv(StringIO(csv.text))  # 有header
            else:
                # 從CSV檔取得資料
                df = pandas.read_csv(path, encoding="big5")
            print(df)  # debug
            self.df = df
        except Exception as exc:
            print(exc)
            return False

        return True

    def insert_mysql(self) -> bool:
        try:
            # 建立connection物件
            my_connt_obj = db_connect.mysql_connect()
            conn = my_connt_obj.connect()
            with conn.cursor() as cursor:
                # 新增SQL語法
                for row, _ in self.df.iterrows():
                    if len(row[0]) != 9:
                        continue
                    # data: yyy/mm/dd to yyyy-mm-dd,
                    # e.g. 110/08/02 to 2021-08-02
                    trade_date = f"{str(int(row[0][:3])+1911)}-{str(row[0])[4:6]}-{str(row[0])[7:]}"
                    cmd = f"""INSERT IGNORE INTO StockTransactionInfo
                    (TradeDate,
                    TranscationQty, TranscationAmount, TranscationCount,
                    Taiex, ChangePoint)
                    VALUES('{trade_date}',
                    {row[1].replace(',', '')}, {row[2].replace(',', '')},
                    {row[3].replace(',', '')}, {row[4].replace(',', '')},
                    {row[5].replace(',', '')});"""
                    cursor.execute(cmd)
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {self.title}==")
        return True


csv_data = daily_market_info()
csv_path = os.path.join(os.path.dirname(__file__), "每日市場成交資訊_202101.csv")
r = csv_data.get_csv_data(path=csv_path)
if r:
    r = csv_data.insert_mysql()
else:
    print("False")
