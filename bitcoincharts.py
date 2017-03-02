import csv
import requests
import zlib


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
		return self
	def __next__(self):
		return self._iter.__next__()
	def close(self):
		return self._closee.close()
	def __del__(self):
		print('garbage collected, closing!')
		return self.close()

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

if __name__ == '__main__':
	print(weighted())
