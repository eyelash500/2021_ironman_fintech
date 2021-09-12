import pandas
import os

import db_connect


class legel_daily_future_option_history:
    """Get the CSV which is recorded the data which 3 legal-person traded on
    future and option from Taifex's CSV.
    name: 區分期貨與選擇權二類-依日期（資料下載專區）
    download from: https://www.taifex.com.tw/cht/3/futAndOptDateView?menuid1=03
    code: big5
    type: CSV
    """

    """
    CSV:
        columns:
            日期,身份別,
            期貨多方交易口數,選擇權多方交易口數,
            期貨多方交易契約金額(千元),選擇權多方交易契約金額(千元),
            期貨空方交易口數,選擇權空方交易口數,
            期貨空方交易契約金額(千元),選擇權空方交易契約金額(千元),
            期貨多空交易口數淨額,選擇權多空交易口數淨額,
            期貨多空交易契約金額淨額(千元),選擇權多空交易契約金額淨額(千元),

            期貨多方未平倉口數,選擇權多方未平倉口數,
            期貨多方未平倉契約金額(千元),選擇權多方未平倉契約金額(千元),
            期貨空方未平倉口數,選擇權空方未平倉口數,
            期貨空方未平倉契約金額(千元),選擇權空方未平倉契約金額(千元),
            期貨多空未平倉口數淨額,選擇權多空未平倉口數淨額,
            期貨多空未平倉契約金額淨額(千元),選擇權多空未平倉契約金額淨額(千元)
        data:
            20210713,自營商,
            49823,278180,52199999,
            705529,51537,262514,
            59173868,659540,-1714,
            15666,-6973869,45989,
            113981,169564,115945548,
            954505,145842,145264,
            71963914,921040,-31861,
            24300,43981634,33465
    """

    def __init__(self) -> None:
        self.title = "區分期貨與選擇權二類-依日期（資料下載專區）"
        # 預設值，用來當範本
        self.url = os.path.join(os.path.dirname(__file__), "20180907-20210907.csv")
        self.df = None  # 把資料從csv轉乘datframe

    def get_csv_data(self, url=None) -> bool:
        """取得CSV內的資料，並轉成Dataframe，回傳成功與否

        Args:
            param1 (str): 歷史資料CSV路徑
        Returns:
            bool: 回傳結果. True 表示取得成功，False 表示去得失敗或是轉換失敗。
        """
        if url:
            self.url = url

        try:
            df = pandas.read_csv(self.url, encoding="big5")  # 有header
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
                    trade_date = str(row[0])
                    cmd = f"""INSERT IGNORE INTO LegalDailyFutureOption 
                    (TradeDate, TradeGroup,
                    FutureLongQty, OptionLongQty,
                    FutureLongAmount, OptionLongAmount,
                    FutureShortQty, OptionShortQty,
                    FutureShortAmount, OptionShortAmount,
                    FutureNetQty, OptionNetQty,
                    FutureNetAmount, OptionNetAmount,
                    FutureLongOIQty, OptionLongOIQty,
                    FutureLongOIAmount, OptionLongOIAmount,
                    FutureShortOIQty, OptionShortOIQty,
                    FutureShortOIAmount, OptionShortOIAmount,
                    FutureOINetQty, OptionOINetQty,
                    FutureOINetAmount, OptionOINetAmount)
                    VALUES("{trade_date}", 
                    '{row[1]}',
                    {row[2]}, {row[3]},
                    {row[4]}, {row[5]},
                    {row[6]}, {row[7]},
                    {row[8]}, {row[9]},
                    {row[10]}, {row[11]},
                    {row[12]}, {row[13]},
                    {row[14]}, {row[15]},
                    {row[16]}, {row[17]},
                    {row[18]}, {row[19]},
                    {row[20]}, {row[21]},
                    {row[22]}, {row[23]},
                    {row[24]}, {row[25]});"""
                    cursor.execute(cmd)

                    counter += 1
                conn.commit()
        except Exception as exc:
            print(exc)
            return False

        print(f"===Finish: {counter}==")
        return True


csv_data = legel_daily_future_option_history()
r = csv_data.get_csv_data(
    os.path.join(os.path.dirname(__file__), "3legalhistory_20200101-20210911.csv")
)
if r:
    r = csv_data.insert_mysql()
    # print("success")
else:
    print("False")
