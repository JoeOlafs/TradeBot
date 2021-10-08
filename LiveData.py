import keys
import pandas as pd
import sqlalchemy
from binance.client import Client
from binance import BinanceSocketManager

client = Client(keys.api_key,keys.secret_key)