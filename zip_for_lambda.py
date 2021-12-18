import zipfile

def zip_files(path, file_names):
    zfile = zipfile.ZipFile(path, 'w')
    for path in file_names:
        zfile.write(path)

    zfile.close()

QUERY_FILES = ['credentials/trader_bot0', 'backtester.py', 'indicators.py', 'lambda_function.py', 'sm_util.py', 'strategy.py']
zip_files('query.zip', QUERY_FILES)
AUTOUPDATER_FILES = ['autoupdater.py', 'credentials/alphavantage_key0', 'credentials/admin_credentials', 'backtester.py', 'indicators.py','sm_util.py', 'strategy.py', 'alphavantage.py']
zip_files('autoupdater.zip', AUTOUPDATER_FILES)