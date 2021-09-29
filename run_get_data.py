from datetime import datetime
from legal_person import legal_daily_future_option
from market_info import daily_market_info
from stock_transaction import stock_transaction


class daily_transaction:
    """Save the daily transaction info to database."""

    def save_to_db(self):
        print(f"=Start: {datetime.now().strftime('%H%M%S')}")
        legal_daily = legal_daily_future_option()
        market_data = daily_market_info()
        stock_trans_data = stock_transaction()

        r = legal_daily.get_and_save()
        if r is False:
            print(f"error:{legal_daily.__str__}")

        r = market_data.get_and_save()
        if r is False:
            print(f"error:{market_data.__str__}")

        r = stock_trans_data.get_and_save()
        if r is False:
            print(f"error:{stock_trans_data.__str__}")

        print(f"=Finish: {datetime.now().strftime('%H%M%S')}")


"""實作測試"""
# 匯入資料
worker = daily_transaction()
worker.save_to_db()
