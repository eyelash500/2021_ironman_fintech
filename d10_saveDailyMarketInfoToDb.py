import requests
import pandas
from io import StringIO

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
            日期,成交股數,成交金額,成交筆數,發行量加權股價指數,漲跌點數
        data:
            110/01/04,9,339,297,176,349,548,269,131,2,722,333,14,902.03,169.50
   
    """

    def __init__(self) -> None:
        self.title = "盤後資訊 > 每日市場成交資訊"
        # 預設值，之後如果有修改URL可以用成參數帶入
        self.url = "https://www.twse.com.tw/exchangeReport/FMTQIK?response=open_data"
        self.df = None  # 把資料從csv轉乘datframe
        self.csv = self.get_csv_data()  # 取得網路上的資料，格式為csv

        if type(self.csv) is False:
            print(f"無法取得資料:{self.title}")

    def get_csv_data(self, url=None) -> bool:
        """取得CSV內的資料，並轉成Dataframe，回傳成功與否。

        Args:
            param1 (str): 資料的url
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示取得失敗或是轉換失敗。
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
        try:
            # 建立connection物件
            my_connt_obj = db_connect.mysql_connect()
            conn = my_connt_obj.connect()
            with conn.cursor() as cursor:
                # 新增SQL語法
                for _, row in self.df.iterrows():
                    trade_date = f"{str(int(row[0]/10000)+1911)}-{str(row[0])[3:5]}-{str(row[0])[5:8]}"
                    cmd = f"""INSERT IGNORE INTO StockTransactionInfo
                    (TradeDate,
                    TranscationQty, TranscationAmount, TranscationCount,
                    Taiex, ChangePoint)
                    VALUES('{trade_date}',
                    '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]});"""
                    cursor.execute(cmd)
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {self.title}==")
        return True


csv_data = daily_market_info()
r = csv_data.get_csv_data()
if r:
    r = csv_data.insert_mysql()
else:
    print("False")
