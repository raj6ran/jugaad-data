"""
Trendylin Trading DB - Step 2
Downloads historical price data into trading.db
Run:
  python3 engine/downloader.py --top50     # quick test
  python3 engine/downloader.py --top500    # market hours
  python3 engine/downloader.py --all       # evening run
"""
import sqlite3, os, time, sys
from datetime import date, timedelta
from jugaad_data.nse import stock_df

DB = os.path.expanduser('~/stock_project/db/trading.db')

def get_symbols(mode='top50'):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    if mode == 'top50':
        # Top 50 by market cap (already sorted in DB)
        c.execute('SELECT symbol FROM stocks WHERE is_active=1 LIMIT 50')
    elif mode == 'top500':
        c.execute('SELECT symbol FROM stocks WHERE is_active=1 LIMIT 500')
    else:
        c.execute('SELECT symbol FROM stocks WHERE is_active=1')
    symbols = [r[0] for r in c.fetchall()]
    conn.close()
    return symbols

def already_downloaded(symbol, days=5):
    """Check if stock already has recent data"""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM prices WHERE symbol=? AND date >= date("now", ?)',
              (symbol, f'-{days} days'))
    count = c.fetchone()[0]
    conn.close()
    return count > 0

def save_prices(symbol, df_data):
    """Save a dataframe of prices into DB"""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    count = 0
    for _, row in df_data.iterrows():
        try:
            dt = str(row.get('DATE', ''))[:10]
            if not dt:
                continue
            c.execute('''INSERT OR IGNORE INTO prices
                (symbol, date, open, high, low, close, vwap, volume, delivery_qty, delivery_pct)
                VALUES (?,?,?,?,?,?,?,?,?,?)''', (
                symbol,
                dt,
                float(row.get('OPEN', 0) or 0),
                float(row.get('HIGH', 0) or 0),
                float(row.get('LOW', 0) or 0),
                float(row.get('CLOSE', 0) or 0),
                float(row.get('VWAP', 0) or 0),
                int(row.get('VOLUME', 0) or 0),
                int(row.get('DELIVERY QTY', 0) or 0),
                float(row.get('DELIVERY %', 0) or 0),
            ))
            count += 1
        except Exception as e:
            pass
    conn.commit()
    conn.close()
    return count

def download_history(mode='top50', days=365):
    symbols = get_symbols(mode)
    to_date = date.today()
    from_date = to_date - timedelta(days=days)

    print(f"\n{'='*55}")
    print(f"  TRENDYLIN DATA DOWNLOADER")
    print(f"  Mode: {mode.upper()}  |  Stocks: {len(symbols)}")
    print(f"  From: {from_date}  To: {to_date}")
    print(f"{'='*55}")

    success, failed, skipped = [], [], []

    for i, symbol in enumerate(symbols):
        # Skip if already has recent data
        if already_downloaded(symbol, days=3):
            skipped.append(symbol)
            print(f"  [{i+1:4}/{len(symbols)}] {symbol:15} ↷ skipped (already up to date)")
            continue
        try:
            print(f"  [{i+1:4}/{len(symbols)}] {symbol:15}", end=' ', flush=True)
            df = stock_df(
                symbol=symbol,
                from_date=from_date,
                to_date=to_date,
                series="EQ"
            )
            if df is None or len(df) == 0:
                print("⚠ no data")
                failed.append(symbol)
                continue
            saved = save_prices(symbol, df)
            print(f"✓ {saved} days saved")
            success.append(symbol)
            time.sleep(0.4)  # be polite to NSE
        except Exception as e:
            print(f"✗ {str(e)[:40]}")
            failed.append(symbol)
            time.sleep(0.5)

    # Summary
    print(f"\n{'='*55}")
    print(f"  ✓ Downloaded : {len(success)}")
    print(f"  ↷ Skipped   : {len(skipped)} (already had data)")
    print(f"  ✗ Failed    : {len(failed)}")
    if failed:
        print(f"  Failed list : {', '.join(failed[:10])}")

    # Show DB stats
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT COUNT(DISTINCT symbol) FROM prices')
    stocks_with_data = c.fetchone()[0]
    c.execute('SELECT COUNT(*) FROM prices')
    total_rows = c.fetchone()[0]
    conn.close()
    print(f"\n  DB Stats:")
    print(f"  Stocks with price data : {stocks_with_data}")
    print(f"  Total price rows       : {total_rows:,}")
    print(f"{'='*55}\n")

def show_sample():
    """Show sample data from DB"""
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''SELECT symbol, date, open, high, low, close, volume
                 FROM prices ORDER BY date DESC LIMIT 10''')
    rows = c.fetchall()
    conn.close()
    print("\nLatest prices in DB:")
    print(f"  {'Symbol':12} {'Date':12} {'Open':8} {'High':8} {'Low':8} {'Close':8} {'Volume':12}")
    print(f"  {'-'*70}")
    for r in rows:
        print(f"  {r[0]:12} {r[1]:12} {r[2]:8.1f} {r[3]:8.1f} {r[4]:8.1f} {r[5]:8.1f} {r[6]:12,}")

if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv) > 1 else '--top50'
    if   arg == '--top50':  download_history('top50',  365)
    elif arg == '--top500': download_history('top500', 365)
    elif arg == '--all':    download_history('all',    365)
    elif arg == '--sample': show_sample()
    else:
        print("Usage: python3 engine/downloader.py --top50 | --top500 | --all | --sample")

    if arg != '--sample':
        show_sample()
