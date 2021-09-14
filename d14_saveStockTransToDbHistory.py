import requests
import pandas
from io import StringIO
import os

import db_connect


class daily_market_info:
    """Get the stocks' info including open, high, low and close price.
    name: 盤後資訊 > 個股日成交資訊
    download from: https://www.twse.com.tw/zh/page/trading/exchange/STOCK_DAY.html
    code: big5
    type: CSV
    """

    """
    範本：個股日成交資訊_2330_202109.csv
    CSV:
        columns:
            日期,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
        data:
            110/09/01	31,242,788	19,126,702,297	614.00	614.00	608.00	613.00	-1.00	30,125
    """

    def __init__(self) -> None:
        self.title = "盤後資訊 > 個股日成交資訊"
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

    def insert_mysql(self, symbol) -> bool:
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
                    cmd = f"""INSERT IGNORE INTO DailyPrice
                    (Symbol, TradeDate, OpenPrice, HighPrice,
                    LowPrice, ClosePrice, Volumn)
                    VALUES('{symbol}',
                    '{trade_date}', {row[3].replace(',', '')},
                    {row[4].replace(',', '')}, {row[5].replace(',', '')},
                    {row[6].replace(',', '')}, {row[1].replace(',', '')});"""
                    cursor.execute(cmd)
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {self.title}==")
        return True


csv_data = daily_market_info()
csv_path = os.path.join(os.path.dirname(__file__), "個股日成交資訊_2330_202109.csv")
r = csv_data.get_csv_data(path=csv_path)
if r:
    r = csv_data.insert_mysql(2330)
else:
    print("False")
