import requests
import pandas
import pymysql
import datetime
from io import StringIO


def create_new_header(orignal_headers):
    new_headers = []

    for column in orignal_headers:
        data = str(column)
        if data == "證券代號":
            new_headers.append("stock_symbol")
        elif data == "證券名稱":
            new_headers.append("stock_name")
        elif data == "成交股數":
            new_headers.append("volume")
        elif data == "成交金額":
            new_headers.append("total_price")
        elif data == "開盤價":
            new_headers.append("open")
        elif data == "最高價":
            new_headers.append("high")
        elif data == "最低價":
            new_headers.append("low")
        elif data == "收盤價":
            new_headers.append("close")
        elif data == "漲跌價差":
            new_headers.append("spread")
        elif data == "成交筆數":
            new_headers.append("transactions_number")

    return new_headers


def save_data_to_mysql_db():
    db_settings = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "root",
        "password": "pwd",
        "db": "finance",
    }
    # 計數器：計算新增了幾筆
    counter = 0

    try:
        # 建立connection物件
        conn = pymysql.connect(**db_settings)
        # 建立cursor物件
        with conn.cursor() as cursor:
            now = datetime.datetime.now().strftime("%Y-%m-%d")

            # 新增SQL語法
            for _, row in df.iterrows():
                try:
                    cmd = """INSERT IGNORE INTO DailyPrice 
                    (StockID, Symbol, TradeDate, OpenPrice, HighPrice,
                    LowPrice, ClosePrice, Volumn)
                    values(%s,%s,%s,%s,%s,%s,%s,%s);"""
                    cursor.execute(
                        cmd,
                        (
                            None,
                            row.stock_symbol,
                            now,
                            row.open if pandas.notnull(row.open) else 0,
                            row.high if pandas.notnull(row.high) else 0,
                            row.low if pandas.notnull(row.low) else 0,
                            row.close if pandas.notnull(row.close) else 0,
                            row.volume if pandas.notnull(row.volume) else 0,
                        ),
                    )
                    conn.commit()
                    counter += 1
                except Exception as e:
                    print(e)
    except Exception as exc:
        print(exc)
        return False

    print(f"=insert: {counter}=")
    return True


# 資料說明：https://data.gov.tw/dataset/11549
# API位置
address = "http://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data"

# 取得資料
response = requests.get(address)

# 欄位：證券代號、證券名稱、成交股數、成交金額、開盤價、最高價、最低價、收盤價、漲跌價差、成交筆數
# 會是："00875","國泰網路資安","1,998,885","51,736,728","25.78","25.98","25.77","25.98","+0.46","484"

# 解析
data = response.text
mystr = StringIO(data)
df = pandas.read_csv(mystr, header=None)

# 建立新dataframe
new_headers = df.iloc[0]  # 第一行當作header
new_headers = create_new_header(new_headers)
df = df[1:]  # 拿掉第一行的資料
df.columns = new_headers  # 設定資料欄位的名稱
print(df)

# save_data_to_azure_db(df)
save_data_to_mysql_db()
print("===finished===")
