from datetime import datetime
from legal_person import legal_daily_future_option
from market_info import daily_market_info
from stock_transaction import stock_transaction
from fin_signal import fin_signal


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


class signal:
    """Show the signal"""

    def __init__(self) -> None:
        self.fin_signal = fin_signal()
        self.fin_signal._get_db_data()  # get the row data

    def show_signal(self) -> None:
        """Create and show all the signal"""
        signal_list = self.fin_signal.get_signal()
        self._show_singnal(signal_list)

    def _show_singnal(self, signal_list) -> None:
        """Do showing the signal function"""
        print(f"時間：{datetime.now().strftime('%Y-%M-%D %H:%M:%S')}")
        print(f"＝＝訊號燈＝＝")
        for title, signal in signal_list:
            print(f"* {title}: {signal}")


"""實作測試"""
# # 匯入資料
# worker = daily_transaction()
# worker.save_to_db()

# 訊號燈印出
signal = signal()
signal.show_signal()
