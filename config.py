import os

datafolder = os.path.join(os.path.expanduser('~'),'.btc-py')

os.makedirs(datafolder, 0o755, True)
