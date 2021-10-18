import zipfile

zfile = zipfile.ZipFile('aws_lambda.zip', 'w')
for path in ['credentials/trader_bot0', 'backtester.py', 'indicators.py', 'lambda_function.py', 'sm_util.py', 'strategy.py']:
    zfile.write(path)

zfile.close()