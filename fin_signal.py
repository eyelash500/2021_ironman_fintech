from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas

import db_connect


class fin_signal:
    def __init__(self):
        self.df_taiex = None
        self.df_legal = None
        self.df_stock = None

    def _get_db_data(self):
        # 建立與MySQL的連線
        my_connt_obj = db_connect.mysql_connect()
        conn = my_connt_obj.connect()

        # 一年前的日期
        date_year = datetime.today() - relativedelta(year=1)
        date_year = f"{date_year.year}-{date_year.month}-{date_year.date}"
        # 加權指數資料
        sql_script = (
            f"SELECT * FROM StockTransactionInfo WHERE TradeDate > '{date_year}'"
        )
        self.df_taiex = pandas.read_sql(sql_script, con=conn)

        # 取得三大法人期選的資料
        sql_script = (
            f"SELECT * FROM LegalDailyFutureOption WHERE TradeDate > '{date_year}'"
        )
        self.df_legal = pandas.read_sql(sql_script, con=conn)

        # 取得股價資訊
        sql_script = f"SELECT * FROM DailyPrice WHERE TradeDate > '{date_year}'"
        self.df_stock = pandas.read_sql(sql_script, con=conn)

        # 關閉連線
        conn.close()

    def get_signal(self) -> tuple:
        """Get the signal tuple."""
        r = (
            self._get_signal_1(),
            self._get_signal_2(),
            self._get_signal_3_1(),
            self._get_signal_3_2(),
            self._get_signal_3_3(),
            self._get_signal_4_1(),
            self._get_signal_4_2(),
            self._get_signal_4_3(),
            self._get_signal_5_1(),
            self._get_signal_5_2(),
            self._get_signal_5_3(),
            self._get_signal_6_1(),
            self._get_signal_6_2(),
            self._get_signal_6_3(),
            self._get_signal_7(),
        )

        return r

    def show_data(self):
        print(self.df_taiex.tail())
        print(self.df_legal.tail())
        print(self.df_stock.tail())

    def _get_signal_1(self):
        """signal_1: 最近交易日，是近期六天最大的交易金額
        最近的一個交易日，比前五日的最大的金額更大
        """

        # 取得最近六、五筆資訊
        last_6_data = self.df_taiex[-6:]
        last_5_data = last_6_data["TranscationAmount"][0:-1]

        r = (
            True
            if last_6_data["TranscationAmount"].iloc[-1] > last_5_data.max()
            else False
        )

        return r

    def _get_signal_2(self):
        """signal_2: 顯示交易是否熱絡
        最近的一個交易日，比前五日的最大的金額更大

        Retrun:
            bool: True表示熱絡，False表示不熱絡
        """

        # 取得最近六、五筆資訊
        last_6_data = self.df_taiex[-6:]
        last_5_data = last_6_data["TranscationAmount"][0:-1]

        r = (
            True
            if last_6_data["TranscationAmount"].iloc[-1] > last_5_data.mean()
            else False
        )

        return r

    def _get_signal_3_1(self):
        """signal_3_1: 外資期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Retrun:
            bool: True:多單；False：空單
        """

        # 取得外資 資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return r

    def _get_signal_4_1(self):
        """signal_4_1: 外資期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Retrun:
            bool: True:增加；False：減少
        """

        # 取得外資 資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] >= last_5.iloc[-1] else False

        return r

    def _get_signal_5_1(self) -> int:
        """signal_5_1: 外資期貨留倉數量變化多少
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            int: 顯示為變化數量
        """

        # 取得外資 資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = last_investment.iloc[0] - last_5.iloc[-1]

        return r

    def _get_signal_6_1(self) -> int:
        """signal_6_1: 外資期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            float: 顯示小數點
        """

        # 取得外資 資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "外資及陸資"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = abs((last_investment.iloc[0] - last_5.iloc[-1]) / last_5.iloc[-1])

        return r

    def _get_signal_3_2(self):
        """signal_3_2: 自營商期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Retrun:
            bool: True:多單；False：空單
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return r

    def _get_signal_4_2(self):
        """signal_4_2: 自營商期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Retrun:
            bool: True:增加；False：減少
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] >= last_5.iloc[-1] else False

        return r

    def _get_signal_5_2(self) -> int:
        """signal_5_2: 自營商期貨留倉數量變化多少
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            int: 顯示為變化數量
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = last_investment.iloc[0] - last_5.iloc[-1]

        return r

    def _get_signal_6_2(self) -> int:
        """signal_6_2: 自營商期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            float: 顯示小數點
        """

        # 取得自營商資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "自營商"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = abs((last_investment.iloc[0] - last_5.iloc[-1]) / last_5.iloc[-1])

        return r

    def _get_signal_3_3(self):
        """signal_3_3: 投信商期貨留倉是否空單還是多單
        最近一次交易日資料做比較

        Retrun:
            bool: True:多單；False：空單
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] > 0 else False

        return r

    def _get_signal_4_3(self):
        """signal_4_3: 投信期貨留倉數量是否增加
        最近一次交易日與前一次的期貨留倉數量做比較

        Retrun:
            bool: True:增加；False：減少
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = True if last_investment.iloc[0] >= last_5.iloc[-1] else False

        return r

    def _get_signal_5_3(self) -> int:
        """signal_5_3: 投信期貨留倉數量變化多少
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            int: 顯示為變化數量
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = last_investment.iloc[0] - last_5.iloc[-1]

        return r

    def _get_signal_6_3(self) -> int:
        """signal_6_3: 投信期貨留倉數變動比例
        最近一次交易日與前一次的期貨留倉數量做相減

        Retrun:
            float: 顯示小數點
        """

        # 取得投信資料
        df_investment = self.df_legal.loc[self.df_legal["TradeGroup"] == "投信"]
        # 取得不包含最後一筆之再前五筆資料
        last_5 = df_investment["FutureOINetQty"][-6:-1]

        # 取得最後一筆資料
        last_investment = df_investment["FutureOINetQty"][-1:]
        r = abs((last_investment.iloc[0] - last_5.iloc[-1]) / last_5.iloc[-1])

        return r

    def _get_signal_7(self) -> int:
        """signal_7: 5MA > 20MA
        近期5日平均比20日還要高

        Retrun:
            bool: True:高；False：低
        """

        # 取得台積電資料，代號：2330
        df_2330 = self.df_stock.loc[self.df_stock["Symbol"] == "2330"]

        # 製作5日移動平均數
        close_price_5 = df_2330["ClosePrice"].rolling(5, min_periods=1).mean()

        # 製作20日移動平均數
        close_price_20 = df_2330["ClosePrice"].rolling(20, min_periods=1).mean()

        r = True if close_price_5.iloc[-1] > close_price_20.iloc[-1] else False

        return r


""""實作測試"""

signal = fin_signal()
signal._get_db_data()
signal.show_data()
r = signal.get_signal()

print(r)
