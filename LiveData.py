import keys
import pandas as pd
import sqlalchemy
import asyncio
from binance.client import Client
from binance import AsyncClient, BinanceSocketManager

async def main():
     client = await AsyncClient.create(keys.api_key, keys.secret_key)
     bsm = BinanceSocketManager(client)
     trade_socket = bsm.trade_socket('ETHUSDC')
     async with trade_socket as tscm:
          while True:
               msg = await tscm.recv()
               print(createframe(msg))
     await client.close_connection()

def createframe(msg):
     df = pd.DataFrame([msg])
     df = df.loc[:,['s', 'E', 'p']]
     df.columns = ['Symbol', 'Time', 'Price']
     df.Price = df.Price.astype(float)
     df.Time = pd.to_datetime(df.Time, unit='ms')
     return df

if __name__ == '__main__':
     loop = asyncio.get_event_loop()
     loop.run_until_complete(main())