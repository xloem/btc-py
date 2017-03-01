import csv
import requests
import zlib


def markets():
	return requests.get(markets.uri).json()
markets.uri = 'https://api.bitcoincharts.com/v1/markets.json'

def weighted():
	return requests.get(weighted.uri).json()
weighted.uri = 'https://api.bitcoincharts.com/v1/weighted_prices.json'

class _iter_text:
	def __init__(self, iter, enc):
		self._iter = iter
		self._enc = enc
	def __iter__(self):
		return self
	def __next__(self):
		return self._iter.__next__().decode(self._enc)

class _iter_gunzip:
	def __init__(self, iter):
		self._iter = iter
		self._obj = zlib.decompressobj(16 | zlib.MAX_WBITS)
	def __iter__(self):
		return self
	def __next__(self):
		return self._obj.decompress(self._iter.__next__())
		
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
	return _closable(csv.reader(it), r)
history.uri = 'https://api.bitcoincharts.com/v1/csv'

	
	

if __name__ == '__main__':
	pass
