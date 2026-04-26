"""
Trendylin Trading DB - Step 1
Creates trading.db with all tables and loads 5622 stocks
Run: python3 engine/setup_db.py
"""
import sqlite3, os, pandas as pd

DB = os.path.expanduser('~/stock_project/db/trading.db')

def create_db():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stocks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE, name TEXT,
        nse_code TEXT, bse_code TEXT, isin TEXT,
        sector TEXT, industry TEXT,
        is_active INTEGER DEFAULT 1,
        added_on TEXT DEFAULT CURRENT_DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, date TEXT,
        open REAL, high REAL, low REAL, close REAL,
        vwap REAL, volume INTEGER,
        delivery_qty INTEGER, delivery_pct REAL,
        UNIQUE(symbol, date))''')
    c.execute('''CREATE TABLE IF NOT EXISTS fundamentals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE,
        pe_ratio REAL, pb_ratio REAL, eps REAL,
        roe REAL, roce REAL, debt_equity REAL,
        revenue_growth REAL, profit_growth REAL,
        promoter_holding REAL, market_cap REAL,
        face_value REAL, dividend_yield REAL,
        updated_on TEXT DEFAULT CURRENT_DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS technicals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE,
        rsi REAL, macd REAL, macd_signal REAL,
        ema_20 REAL, ema_50 REAL, ema_200 REAL,
        bb_upper REAL, bb_lower REAL,
        atr REAL, volume_avg REAL, trend TEXT,
        updated_on TEXT DEFAULT CURRENT_DATE)''')
    c.execute('''CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT UNIQUE,
        fund_score REAL, tech_score REAL,
        final_signal TEXT, trading_style TEXT,
        entry_price REAL, target_price REAL,
        stop_loss REAL, reason TEXT,
        updated_on TEXT DEFAULT CURRENT_DATE)''')
    conn.commit()
    conn.close()
    print('✓ Database created:', DB)
    print('✓ Tables: stocks, prices, fundamentals, technicals, signals')

def load_stocks():
    excel = os.path.expanduser('~/stock_project/trendylin_stocks.xlsx')
    if not os.path.exists(excel):
        print('✗ trendylin_stocks.xlsx not found in stock_project folder')
        return
    df = pd.read_excel(excel)
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    count = 0
    for _, row in df.iterrows():
        try:
            symbol = str(row.get('NSE Code') or row.get('NSE code') or '').strip()
            if not symbol or symbol == 'nan':
                continue
            c.execute('''INSERT OR IGNORE INTO stocks
                (symbol, name, nse_code, bse_code, isin, sector, industry)
                VALUES (?,?,?,?,?,?,?)''', (
                symbol,
                str(row.get('Stock Name', '')).strip(),
                symbol,
                str(row.get('BSE Code') or row.get('BSE code') or '').strip(),
                str(row.get('ISIN', '')).strip(),
                str(row.get('sector_name', '')).strip(),
                str(row.get('Industry Name', '')).strip(),
            ))
            count += 1
        except Exception as e:
            pass
    conn.commit()
    conn.close()
    print(f'✓ Loaded {count} stocks into database')

if __name__ == '__main__':
    create_db()
    load_stocks()
    # Verify
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM stocks')
    print(f'✓ Total stocks in DB: {c.fetchone()[0]}')
    c.execute('SELECT symbol, name, sector FROM stocks LIMIT 5')
    print('\nSample stocks:')
    for r in c.fetchall():
        print(f'  {r[0]:15} {str(r[1])[:30]:30} {r[2]}')
    conn.close()
    print('\nStep 1 complete! Ready for Step 2 (data downloader).')
