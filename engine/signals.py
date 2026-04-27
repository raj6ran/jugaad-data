"""
Trendylin Trading DB - Step 4
Signals Engine
Generates BUY/HOLD/SELL signals with entry, target, stop loss
Run: python3 engine/signals.py
"""
import sqlite3, os
import pandas as pd

DB = os.path.expanduser('~/stock_project/db/trading.db')

def get_all_technicals():
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query('''
        SELECT t.symbol, t.rsi, t.macd, t.macd_signal,
               t.ema_20, t.ema_50, t.ema_200,
               t.bb_upper, t.bb_lower, t.atr,
               t.volume_avg, t.trend,
               p.close, p.volume, p.high, p.low
        FROM technicals t
        LEFT JOIN (
            SELECT symbol, close, volume, high, low
            FROM prices
            WHERE (symbol, date) IN (
                SELECT symbol, MAX(date) FROM prices GROUP BY symbol
            )
        ) p ON t.symbol = p.symbol
    ''', conn)
    conn.close()
    return df

def calculate_tech_score(row):
    """Score 0-10 based on technical indicators"""
    score = 5.0  # neutral start
    reasons = []

    rsi      = row.get('rsi')
    macd     = row.get('macd')
    macd_sig = row.get('macd_signal')
    trend    = row.get('trend', '')
    close    = row.get('close')
    ema20    = row.get('ema_20')
    ema50    = row.get('ema_50')
    ema200   = row.get('ema_200')
    bb_upper = row.get('bb_upper')
    bb_lower = row.get('bb_lower')
    vol      = row.get('volume')
    vol_avg  = row.get('volume_avg')

    # ── RSI Scoring ──────────────────────────────
    if rsi:
        if rsi < 25:
            score += 2.0
            reasons.append(f'RSI {rsi:.0f} — heavily oversold, strong bounce likely')
        elif rsi < 35:
            score += 1.5
            reasons.append(f'RSI {rsi:.0f} — oversold, watch for reversal')
        elif rsi < 45:
            score += 0.5
            reasons.append(f'RSI {rsi:.0f} — mildly weak')
        elif rsi > 75:
            score -= 2.0
            reasons.append(f'RSI {rsi:.0f} — heavily overbought, avoid fresh entry')
        elif rsi > 65:
            score -= 1.0
            reasons.append(f'RSI {rsi:.0f} — overbought, caution')
        elif 45 <= rsi <= 60:
            score += 0.5
            reasons.append(f'RSI {rsi:.0f} — healthy neutral zone')

    # ── MACD Scoring ──────────────────────────────
    if macd is not None and macd_sig is not None:
        if macd > macd_sig and macd > 0:
            score += 1.5
            reasons.append('MACD bullish crossover above zero — momentum building')
        elif macd > macd_sig and macd < 0:
            score += 0.8
            reasons.append('MACD bullish crossover below zero — early recovery')
        elif macd < macd_sig and macd < 0:
            score -= 1.5
            reasons.append('MACD bearish below zero — downtrend confirmed')
        elif macd < macd_sig and macd > 0:
            score -= 0.8
            reasons.append('MACD bearish crossover — momentum fading')

    # ── Trend Scoring ─────────────────────────────
    trend_scores = {
        'STRONG_UP':   2.0,
        'UP':          1.0,
        'SIDEWAYS':    0.0,
        'DOWN':       -1.0,
        'STRONG_DOWN': -2.0,
    }
    trend_reasons = {
        'STRONG_UP':   'Price above EMA20 > EMA50 > EMA200 — strong uptrend',
        'UP':          'Price above EMA50 — uptrend intact',
        'SIDEWAYS':    'Price range-bound — no clear trend',
        'DOWN':        'Price below EMA50 — downtrend',
        'STRONG_DOWN': 'Price below EMA20 < EMA50 < EMA200 — strong downtrend',
    }
    if trend in trend_scores:
        score += trend_scores[trend]
        reasons.append(trend_reasons[trend])

    # ── Bollinger Band Scoring ────────────────────
    if close and bb_lower and bb_upper:
        bb_range = bb_upper - bb_lower
        if bb_range > 0:
            pct = (close - bb_lower) / bb_range
            if pct < 0.15:
                score += 1.0
                reasons.append('Price near lower Bollinger Band — oversold zone')
            elif pct > 0.85:
                score -= 1.0
                reasons.append('Price near upper Bollinger Band — overbought zone')

    # ── Volume Scoring ────────────────────────────
    if vol and vol_avg and vol_avg > 0:
        vol_ratio = vol / vol_avg
        if vol_ratio > 2.0 and trend in ['STRONG_UP', 'UP']:
            score += 0.5
            reasons.append(f'Volume {vol_ratio:.1f}x above average — strong buying')
        elif vol_ratio > 2.0 and trend in ['STRONG_DOWN', 'DOWN']:
            score -= 0.5
            reasons.append(f'Volume {vol_ratio:.1f}x above average — strong selling')

    return round(min(max(score, 0), 10), 2), reasons

def determine_signal(score, row):
    """Convert score to signal"""
    trend = row.get('trend', '')
    rsi   = row.get('rsi', 50)

    if score >= 8.0:   return 'STRONG BUY'
    elif score >= 6.5: return 'BUY'
    elif score >= 4.5: return 'HOLD'
    elif score >= 3.0: return 'SELL'
    else:              return 'STRONG SELL'

def determine_trading_style(row, score):
    """Recommend trading style based on ATR and trend"""
    atr      = row.get('atr', 0) or 0
    close    = row.get('close', 1) or 1
    vol      = row.get('volume', 0) or 0
    vol_avg  = row.get('volume_avg', 1) or 1
    trend    = row.get('trend', '')
    rsi      = row.get('rsi', 50) or 50

    atr_pct  = (atr / close * 100) if close else 0
    vol_ratio = vol / vol_avg if vol_avg else 1
    styles = []

    # Scalper: low ATR, high volume
    if atr_pct < 1.5 and vol_ratio > 1.5:
        styles.append('SCALPER')

    # Intraday: moderate ATR, trending
    if 1.0 < atr_pct < 3.0 and trend in ['UP','STRONG_UP','DOWN','STRONG_DOWN']:
        styles.append('INTRADAY')

    # Swing: oversold/overbought with trend change signals
    if (rsi < 35 or rsi > 65) and atr_pct > 1.0:
        styles.append('SWING')

    # Positional: strong trend + fundamentally sound
    if trend == 'STRONG_UP' and score >= 6.5:
        styles.append('POSITIONAL')

    return ', '.join(styles) if styles else 'AVOID'

def calculate_levels(row, signal):
    """Calculate entry, target, stop loss"""
    close = row.get('close', 0) or 0
    atr   = row.get('atr', close * 0.02) or (close * 0.02)
    ema20 = row.get('ema_20') or close
    ema50 = row.get('ema_50') or close

    if 'BUY' in signal:
        entry  = round(close, 2)
        sl     = round(close - (1.5 * atr), 2)
        target = round(close + (3.0 * atr), 2)
    elif 'SELL' in signal:
        entry  = round(close, 2)
        sl     = round(close + (1.5 * atr), 2)
        target = round(close - (3.0 * atr), 2)
    else:  # HOLD
        entry  = round(ema20, 2)
        sl     = round(close - (2.0 * atr), 2)
        target = round(close + (2.0 * atr), 2)

    return entry, target, sl

def save_signal(row, score, signal, style, entry, target, sl, reasons):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO signals
        (symbol, tech_score, final_signal, trading_style,
         entry_price, target_price, stop_loss, reason, updated_on)
        VALUES (?,?,?,?,?,?,?,?, date('now'))''', (
        row['symbol'], score, signal, style,
        entry, target, sl,
        ' | '.join(reasons[:3])  # top 3 reasons
    ))
    conn.commit()
    conn.close()

def run_all():
    df = get_all_technicals()
    print(f"\n{'='*75}")
    print(f"  TRENDYLIN SIGNALS ENGINE")
    print(f"  Generating signals for {len(df)} stocks...")
    print(f"{'='*75}")

    signal_counts = {'STRONG BUY':0,'BUY':0,'HOLD':0,'SELL':0,'STRONG SELL':0}

    for _, row in df.iterrows():
        symbol = row['symbol']
        score, reasons = calculate_tech_score(row)
        signal = determine_signal(score, row)
        style  = determine_trading_style(row, score)
        entry, target, sl = calculate_levels(row, signal)
        save_signal(row, score, signal, style, entry, target, sl, reasons)
        signal_counts[signal] = signal_counts.get(signal, 0) + 1

    # Print results table
    print(f"\n  {'Symbol':12} {'Score':6} {'Signal':12} {'Style':25} {'Entry':8} {'Target':8} {'SL':8}")
    print(f"  {'-'*85}")

    conn = sqlite3.connect(DB)
    rows = pd.read_sql_query('''
        SELECT symbol, tech_score, final_signal, trading_style,
               entry_price, target_price, stop_loss, reason
        FROM signals ORDER BY tech_score DESC
    ''', conn)
    conn.close()

    icons = {'STRONG BUY':'🚀','BUY':'📈','HOLD':'➡️ ','SELL':'📉','STRONG SELL':'💀'}
    for _, r in rows.iterrows():
        icon = icons.get(r['final_signal'], ' ')
        print(f"  {r['symbol']:12} {str(r['tech_score']):6} "
              f"{icon}{r['final_signal']:11} {str(r['trading_style'])[:24]:25} "
              f"₹{str(r['entry_price']):7} ₹{str(r['target_price']):7} ₹{str(r['stop_loss'])}")

    # Summary
    print(f"\n{'='*75}")
    print(f"  SIGNAL SUMMARY")
    print(f"  🚀 STRONG BUY  : {signal_counts.get('STRONG BUY',0)}")
    print(f"  📈 BUY         : {signal_counts.get('BUY',0)}")
    print(f"  ➡️  HOLD        : {signal_counts.get('HOLD',0)}")
    print(f"  📉 SELL        : {signal_counts.get('SELL',0)}")
    print(f"  💀 STRONG SELL : {signal_counts.get('STRONG SELL',0)}")

    # Top opportunities
    buys = rows[rows['final_signal'].isin(['STRONG BUY','BUY'])].head(5)
    if not buys.empty:
        print(f"\n  TOP BUY OPPORTUNITIES:")
        for _, r in buys.iterrows():
            print(f"  ► {r['symbol']:12} Score:{r['tech_score']} | {r['reason'][:60]}")
    print(f"{'='*75}\n")

if __name__ == '__main__':
    run_all()
