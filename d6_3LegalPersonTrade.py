import requests
import pandas
from io import StringIO


class legel_daily_future_option:

    """Get the CSV which is recorded the data which 3 legal-person traded on
    future and option from open data.
    URL: https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate
    code: big5
    type: CSV
    """

    """
    CSV:
        columns:
            日期,身份別,
            期貨多方交易口數,選擇權多方交易口數,期貨多方交易契約金額(千元),
            選擇權多方交易契約金額(千元),期貨空方交易口數,選擇權空方交易口數,
            期貨空方交易契約金額(千元),選擇權空方交易契約金額(千元),期貨多空交易口數淨額,
            選擇權多空交易口數淨額,期貨多空交易契約金額淨額(千元),選擇權多空交易契約金額淨額(千元),
            期貨多方未平倉口數,選擇權多方未平倉口數,期貨多方未平倉契約金額(千元),
            選擇權多方未平倉契約金額(千元),期貨空方未平倉口數,選擇權空方未平倉口數,
            期貨空方未平倉契約金額(千元),選擇權空方未平倉契約金額(千元),期貨多空未平倉口數淨額,
            選擇權多空未平倉口數淨額,期貨多空未平倉契約金額淨額(千元),選擇權多空未平倉契約金額淨額(千元)
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
        self.url = "https://www.taifex.com.tw/data_gov/taifex_open_data.asp?data_name=MarketDataOfMajorInstitutionalTradersDividedByFuturesAndOptionsBytheDate"
        self.get_csv_data()

    def get_csv_data(self, url=None) -> str:
        if url:
            self.url = url

        csv = requests.get(self.url)
        print(csv.text)
        df = pandas.read_csv(StringIO(csv.text))  # 有header
        print(df)

        return df


proc = legel_daily_future_option()
