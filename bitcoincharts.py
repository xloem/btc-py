import csv
import decimal
import os
import requests
import sqlite3
import threading
import zlib

import config


def markets():
	return requests.get(markets.uri).json()
markets.uri = 'https://api.bitcoincharts.com/v1/markets.json'

def weighted():
	return requests.get(weighted.uri).json()
weighted.uri = 'https://api.bitcoincharts.com/v1/weighted_prices.json'

def _iter_text(iter, enc):
	for chunk in iter:
		yield chunk.decode(enc)

def _iter_lines(iter):
	pending = None
	for chunk in iter:
		if pending is not None:
			chunk = pending + chunk
		lines = chunk.splitlines()
		if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
			pending = lines.pop()
		else:
			pending = None

		for line in lines:
			yield line

def _iter_gunzip(iter):
	obj = zlib.decompressobj(16 | zlib.MAX_WBITS)
	for chunk in iter:
		yield obj.decompress(chunk)

		
class _closable:
	def __init__(self, iter, closee):
		self._iter = iter
		self._closee = closee
	def __iter__(self):
		return self._iter
	def close(self):
		return self._closee.close()

def trades(symbol, start):
	r = requests.get(trades.uri + '?symbol=%s&start=%d' % (symbol, start), stream=True)
	it = r.iter_lines()
	it = _iter_text(it, r.encoding)
	return _closable(csv.reader(it), r)
trades.uri = 'https://api.bitcoincharts.com/v1/trades.csv'
	

def history(symbol):
	r = requests.get(history.uri + '/%s.csv.gz' % symbol, stream=True)
	it = r.iter_content(1024)
	it = _iter_gunzip(it)
	it = _iter_text(it, 'utf-8')
	it = _iter_lines(it)
	return _closable(csv.reader(it), r)
history.uri = 'https://api.bitcoincharts.com/v1/csv'

class Database:
	def __init__(self, connection = None):
		if connection is None:
			connection = sqlite3.connect(os.path.join(config.datafolder, 'bitcoincharts.sql'))
		self._conn = connection
		self._c = self._conn.cursor()

		self._c.execute('create table if not exists exchanges (symbol text primary key, currency text not null, latestTradeKnown integer not null, latestTradeStored integer)')
		self._c.execute('create index if not exists exchangescurrency on exchanges(currency, latestTradeKnown, symbol, latestTradeStored)')
		self._c.execute('create index if not exists exchangeslatestTradeStored on exchanges(latestTradeStored, latestTradeKnown, currency, symbol)')
		self._c.execute('create table if not exists trades (id integer primary key autoincrement, time integer not null, price text not null, volume text not null, symbol text not null references exchanges(symbol))')
		self._c.execute('create index if not exists tradessymboltime on trades(symbol, time, id, price, volume)')
		self._conn.commit()
		
		self.update()
		
	def update(self):
		for market in markets():
			symbol = market.get('symbol')
			latestTrade = int(market.get('latest_trade'))
			row = self._c.execute('select latestTradeKnown from exchanges where symbol = ?', (symbol,)).fetchone()
			if row is None:
				self._c.execute('insert into exchanges(symbol, currency, latestTradeKnown) values (?, ?, ?)', (symbol, market.get('currency'), latestTrade))
			elif row[0] != latestTrade:
				self._c.execute('update exchanges set currency = ?, latestTradeKnown = ? where symbol = ?', (market.get('currency'), latestTrade, symbol))
		self._conn.commit()
		for row in self._c.execute("select symbol from exchanges where latestTradeStored not null and latestTradeStored < latestTradeKnown").fetchall():
			self.updateSymbol(row[0])
			
	def currencies(self):
		return [row[0] for row in self._c.execute('select distinct currency from exchanges')]
	
	def symbols(self, currency = None):
		if currency is None:
			self._c.execute('select symbol from exchanges')
		else:
			self._c.execute('select symbol from exchanges where currency = ?', (currency,))
		return [row[0] for row in self._c]
	
	def trades(self, symbol = None, currency = None, start = 0, end = 1 << 50, reverse = False):
		if reverse:
			dir = 'desc'
		else:
			dir = 'asc'
		if symbol is not None:
			clause = 'symbol = ?'
			clausebind = symbol
		elif currency is not None:
			clause = 'symbol in (select symbol from exchanges where currency = ?)'
			clausebind = currency
		else:
			clause = '?'
			clausebind = True
		c = self._conn.cursor()
		for row in c.execute('select time, price, volume, symbol from trades where ' + clause + ' and time between ? and ? order by time ' + dir +', id ' + dir, (clausebind, start, end)):
			yield((row[0], decimal.Decimal(row[1]), decimal.Decimal(row[2]), row[3]))
		c.close()
	
	def updateCurrency(self, currency):
		for symbol in self.symbols(currency):
			self.updateSymbol(symbol)
	
	def updateSymbol(self, symbol):
		row = self._c.execute('select time from trades where symbol = ? order by time desc limit 1', (symbol,)).fetchone()
		if row is None:
			print ('Downloading entire %s history ...' % symbol)
			count = 0
			for trade in history(symbol):
				trade.append(symbol)
				lastTime = trade[0] = int(trade[0])
				count = count + 1
				self._c.execute('insert into trades(time, price, volume, symbol) values(?,?,?,?)', trade)
				if count % (1 << 20) == 0:
					print('%d ...' % count)
					self._conn.commit()
			self._conn.commit()
			print('%d new %s trades' % (count, symbol))
		else:
			lastTime = row[0]
		
		oldtrades = True
		count = 0
		for trade in trades(symbol, lastTime - 1):
			trade.append(symbol)
			lastTime = trade[0] = int(trade[0])
			if oldtrades:
				if self._c.execute('select id from trades where time = ? and price = ? and volume = ? and symbol = ?', trade).fetchone() is None:
					oldtrades = False
			if not oldtrades:
				count = count + 1
				self._c.execute('insert into trades(time, price, volume, symbol) values(?,?,?,?)', trade)
		if count > 0:
			self._c.execute('update exchanges set latestTradeStored = ? where symbol = ?', (lastTime, symbol))
			self._conn.commit()
			print('%d more %s trades' % (count, symbol))
		else:
			if lastTime != self._c.execute('select latestTradeKnown from exchanges where symbol = ?', (symbol,)).fetchone()[0]:
				print('%s appears to be an incomplete historical archive' % symbol)
				self._c.execute('delete from trades where symbol = ?', (symbol,))
				self._conn.commit()
				return self.updateSymbol(symbol)
		
	def verify(self, symbol):
		self._c.execute('select time, price, volume from trades where symbol = ? order by time, id', (symbol,))
		count = 0
		for remote in history(symbol):
			stored = self._c.fetchone()
			if stored is None:
				print('%s correct for %d but not updated; next trade is %s' % (symbol, count, remote))
				return False
			count = count + 1
			remote[0] = int(remote[0])
			if list(stored) != remote:
				print('%s failed verification at trade %d; local=%s remote=%s' % (symbol, count, stored, remote))
				return False
			if count % (1 << 19) == 0:
				print('%d correct ...' % count)
		extra = self._c.fetchone()
		if extra is not None:
			print('%s correct for %d trades, %d new trades not in remote archive yet' % (symbol,count,1+len(list(self._c))))
		else:
			print('%s correct with %d trades' % (symbol,count))
		return True
	
	

if __name__ == '__main__':
	d = Database()
	d.updateCurrency('USD')

