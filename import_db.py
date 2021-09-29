import argparse
import alphavantage

def get_args():
    p = argparse.ArgumentParser()
    p.add_argument('--ticker')
    p.add_argument('--key')
    p.add_argument('--update')
    return vars(p.parse_args())


args = get_args()
print(args)
if args.get('key', None) != None:
    alphavantage.alphavantage_key = args.get('key')
if args.get('ticker', None) != None:
    alphavantage.add_symbol_to_db(args.get('ticker'))
if args.get('update', None) != None:
    alphavantage.update_symbols_in_db()