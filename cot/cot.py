import os
import requests

class DiskWriter:
    DIRECTORY_NAME =  DIRECTORY_NAME = os.path.join(os.path.dirname(__file__), "data")

    def __init__(self, name):
        try:
            os.makedirs(self.DIRECTORY_NAME, exist_ok=True)
            self.file_path = os.path.join(self.DIRECTORY_NAME, name)
            self.writer = open(self.file_path, "a", encoding="utf-8")
            self.reader = open(self.file_path, "r", encoding="utf-8")
        except Exception as e:
            print(e)

    def write_line(self, line, check_duplicate=False):
        if not check_duplicate:
            self._write_line(line)
        else:
            self._write_line_if_unique(line)

    def _write_line(self, line):
        if self.writer:
            self.writer.write(line + "\n")

    def _write_line_if_unique(self, line):
        try:
            self.reader.seek(0)  # Reset reader position
            if any(existing_line.strip() == line for existing_line in self.reader):
                return
            self._write_line(line)
            print("Writing line to file " + line)
        except Exception as e:
            print(e)

    def close(self):
        if self.writer:
            self.writer.close()
        if self.reader:
            self.reader.close()


class CftcCotDataBroker:
    ITEMS = [
        "WHEAT-SRW - CHICAGO BOARD OF TRADE",
        "BITCOIN - CHICAGO MERCANTILE EXCHANGE",
        "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
        "NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE",
        "GOLD - COMMODITY EXCHANGE INC.",
        "SILVER - COMMODITY EXCHANGE INC.",
        "USD INDEX - ICE FUTURES U.S.",
        "DJIA Consolidated - CHICAGO BOARD OF TRADE",
        "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE",
        "CORN - CHICAGO BOARD OF TRADE"
    ]

    FILES = [
        "cot_wheat.csv",
        "cot_btc.csv",
        "cot_sp500.csv",
        "cot_nasdaq.csv",
        "cot_gold.csv",
        "cot_silver.csv",
        "cot_usd.csv",
        "cot_djia.csv",
        "cot_crude_oil.csv",
        "cot_corn.csv"
    ]

    def download_data(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()
            lines = response.text.splitlines()
            
            for line in lines:
                parts = line.split(",")
                id = parts[0].replace("\"", "").strip()
                if id in self.ITEMS:
                    index = self.ITEMS.index(id)
                    line_item = f"{parts[1]}|{id}|{parts[11].strip()}|{parts[12].strip()}"
                    file_writer = DiskWriter(self.FILES[index])
                    file_writer.write_line(line_item, True)
                    file_writer.close()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    broker = CftcCotDataBroker()
    broker.download_data("https://www.cftc.gov/dea/newcot/deafut.txt")
