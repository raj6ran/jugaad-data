"""
Trendylin Trading DB - Step 3
Technical Analysis Engine
Calculates RSI, MACD, EMA, Bollinger Bands, ATR for all stocks
Run: python3 engine/technicals.py
"""
import sqlite3, os, sys
import pandas as pd
import ta

DB = os.path.expanduser('~/stock_project/db/trading.db')

def get_symbols_with_data():
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT DISTINCT symbol FROM prices ORDER BY symbol')
    symbols = [r[0] for r in c.fetchall()]
    conn.close()
    return symbols

def get_price_history(symbol, days=200):
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query(
        'SELECT date, open, high, low, close, volume FROM prices WHERE symbol=? ORDER BY date DESC LIMIT ?',
        conn, params=(symbol, days)
    )
    conn.close()
    if df.empty:
        return None
    df = df.sort_values('date').reset_index(drop=True)
    for col in ['close','high','low','open','volume']:
        df[col] = pd.to_numeric(df[col])
    return df

def calculate_technicals(symbol):
    df = get_price_history(symbol)
    if df is None or len(df) < 30:
        return None

    close = df['close']
    high  = df['high']
    low   = df['low']
    vol   = df['volume']
    result = {}

    # RSI
    try:
        result['rsi'] = round(float(ta.momentum.RSIIndicator(close, window=14).rsi().iloc[-1]), 2)
    except: result['rsi'] = None

    # MACD
    try:
        macd_obj = ta.trend.MACD(close)
        result['macd']        = round(float(macd_obj.macd().iloc[-1]), 2)
        result['macd_signal'] = round(float(macd_obj.macd_signal().iloc[-1]), 2)
    except: result['macd'] = result['macd_signal'] = None

    # EMA 20, 50, 200
    try: result['ema_20']  = round(float(ta.trend.EMAIndicator(close, window=20).ema_indicator().iloc[-1]), 2)
    except: result['ema_20'] = None
    try: result['ema_50']  = round(float(ta.trend.EMAIndicator(close, window=50).ema_indicator().iloc[-1]), 2)
    except: result['ema_50'] = None
    try: result['ema_200'] = round(float(ta.trend.EMAIndicator(close, window=200).ema_indicator().iloc[-1]), 2) if len(df) >= 200 else None
    except: result['ema_200'] = None

    # Bollinger Bands
    try:
        bb = ta.volatility.BollingerBands(close, window=20)
        result['bb_upper'] = round(float(bb.bollinger_hband().iloc[-1]), 2)
        result['bb_lower'] = round(float(bb.bollinger_lband().iloc[-1]), 2)
    except: result['bb_upper'] = result['bb_lower'] = None

    # ATR
    try: result['atr'] = round(float(ta.volatility.AverageTrueRange(high, low, close).average_true_range().iloc[-1]), 2)
    except: result['atr'] = None

    # Volume Average
    try: result['volume_avg'] = round(float(vol.tail(20).mean()), 0)
    except: result['volume_avg'] = None

    # Trend Detection
    try:
        price = close.iloc[-1]
        e20   = result.get('ema_20')
        e50   = result.get('ema_50')
        e200  = result.get('ema_200')
        if e200 and price > e200 and e20 and e50 and e20 > e50:   trend = 'STRONG_UP'
        elif e50 and price > e50:                                   trend = 'UP'
        elif e200 and price < e200 and e20 and e50 and e20 < e50: trend = 'STRONG_DOWN'
        elif e50 and price < e50:                                   trend = 'DOWN'
        else:                                                        trend = 'SIDEWAYS'
        result['trend'] = trend
    except: result['trend'] = 'UNKNOWN'

    return result

def save_technicals(symbol, data):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO technicals
        (symbol, rsi, macd, macd_signal, ema_20, ema_50, ema_200,
         bb_upper, bb_lower, atr, volume_avg, trend, updated_on)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?, date('now'))''', (
        symbol,
        data.get('rsi'), data.get('macd'), data.get('macd_signal'),
        data.get('ema_20'), data.get('ema_50'), data.get('ema_200'),
        data.get('bb_upper'), data.get('bb_lower'),
        data.get('atr'), data.get('volume_avg'), data.get('trend'),
    ))
    conn.commit()
    conn.close()

def run_all():
    symbols = get_symbols_with_data()
    print(f"\n{'='*60}")
    print(f"  TECHNICAL ANALYSIS ENGINE")
    print(f"  Processing {len(symbols)} stocks...")
    print(f"{'='*60}")

    success, failed = [], []
    for i, symbol in enumerate(symbols):
        try:
            data = calculate_technicals(symbol)
            if not data:
                print(f"  [{i+1:3}/{len(symbols)}] {symbol:15} insufficient data")
                failed.append(symbol)
                continue
            save_technicals(symbol, data)
            trend_icon = {'STRONG_UP':'UP++','UP':'UP','SIDEWAYS':'--','DOWN':'DOWN','STRONG_DOWN':'DOWN--'}.get(data['trend'],'?')
            rsi_flag = ' OVERBOUGHT' if data.get('rsi') and data['rsi']>70 else (' OVERSOLD' if data.get('rsi') and data['rsi']<30 else '')
            print(f"  [{i+1:3}/{len(symbols)}] {symbol:15} RSI:{str(data.get('rsi','?')):6} MACD:{str(data.get('macd','?')):8} {trend_icon}{rsi_flag}")
            success.append(symbol)
        except Exception as e:
            print(f"  [{i+1:3}/{len(symbols)}] {symbol:15} ERROR: {e}")
            failed.append(symbol)

    print(f"\n{'='*60}")
    print(f"  Done! Processed: {len(success)}  Failed: {len(failed)}")

    # Summary table
    print(f"\n  {'Symbol':15} {'RSI':6} {'Trend':12} {'EMA20':8} {'EMA50':8} {'ATR':6}")
    print(f"  {'-'*60}")
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('SELECT symbol, rsi, trend, ema_20, ema_50, atr FROM technicals ORDER BY symbol')
    for r in c.fetchall():
        flag = ' <OVERSOLD' if r[1] and r[1]<30 else (' <OVERBOUGHT' if r[1] and r[1]>70 else '')
        print(f"  {r[0]:15} {str(r[1] or '?'):6} {str(r[2] or '?'):12} {str(r[3] or '?'):8} {str(r[4] or '?'):8} {str(r[5] or '?'):6}{flag}")
    conn.close()
    print(f"{'='*60}\n")

if __name__ == '__main__':
    run_all()
