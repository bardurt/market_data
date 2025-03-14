import os
import requests
import datetime

# Define paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Root directory of script
LOG_FILE = os.path.join(BASE_DIR, "log.txt")  # Log file in root
DATA_DIR = os.path.join(BASE_DIR, "data")  # Data directory

def log_message(message):
    """ Logs a message to log.txt """
    with open(LOG_FILE, "a") as log:
        log.write(f"{datetime.datetime.now()} - {message}\n")

def log_error(error):
    """ Logs an error message to log.txt """
    with open(LOG_FILE, "a") as log:
        log.write(f"{datetime.datetime.now()} - ERROR: {error}\n")

class DiskWriter:
    def __init__(self, name):
        try:
            os.makedirs(DATA_DIR, exist_ok=True)  # Ensure /data directory exists
            self.file_path = os.path.join(DATA_DIR, name)  # File inside /data
            self.writer = open(self.file_path, "a", encoding="utf-8")
            self.reader = open(self.file_path, "r", encoding="utf-8")
            log_message(f"Opened file {self.file_path} for writing.")
        except Exception as e:
            log_error(f"Error initializing DiskWriter: {e}")

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
            self.reader.seek(0)
            if any(existing_line.strip() == line for existing_line in self.reader):
                log_message("Value is not unique")
                return
            self._write_line(line)
            log_message(f"Writing line to file: {line}")
        except Exception as e:
            log_error(f"Error writing line: {e}")

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
            log_message(f"Starting data download. Running from {os.getcwd()}")
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
            
            log_message("Data download completed successfully.")
        except Exception as e:
            log_error(f"Error downloading data: {e}")


if __name__ == "__main__":
    log_message("===========================")
    log_message("Script started.")
    broker = CftcCotDataBroker()
    broker.download_data("https://www.cftc.gov/dea/newcot/deafut.txt")
    log_message("Script finished.")
    log_message("===========================")
