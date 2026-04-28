"""
Trendylin Trading DB - Fundamentals Engine
Fetches PE, ROE, EPS, Debt, Revenue Growth etc. from Yahoo Finance
Scores each stock 1-10 on fundamentals
Run: python3 engine/fundamentals.py --top50
     python3 engine/fundamentals.py --all
"""
import sqlite3, os, sys, time
import yfinance as yf
import pandas as pd

DB = os.path.expanduser('~/stock_project/db/trading.db')

def get_symbols(mode='top50'):
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    if mode == 'top50':
        c.execute('SELECT symbol FROM stocks WHERE is_active=1 LIMIT 50')
    elif mode == 'top500':
        c.execute('SELECT symbol FROM stocks WHERE is_active=1 LIMIT 500')
    else:
        c.execute('SELECT symbol FROM stocks WHERE is_active=1')
    symbols = [r[0] for r in c.fetchall()]
    conn.close()
    return symbols

def fetch_fundamentals(symbol):
    """Fetch fundamental data from Yahoo Finance"""
    try:
        ticker = yf.Ticker(symbol + '.NS')
        info   = ticker.info
        def g(key): 
            v = info.get(key)
            if v is None: return None
            try: return float(v) if v == v else None
            except: return None

        return {
            'symbol':           symbol,
            'pe_ratio':         g('trailingPE'),
            'forward_pe':       g('forwardPE'),
            'pb_ratio':         g('priceToBook'),
            'eps':              g('trailingEps'),
            'roe':              g('returnOnEquity'),
            'profit_margin':    g('profitMargins'),
            'debt_equity':      g('debtToEquity'),
            'revenue_growth':   g('revenueGrowth'),
            'earnings_growth':  g('earningsGrowth'),
            'promoter_holding': g('heldPercentInsiders'),
            'inst_holding':     g('heldPercentInstitutions'),
            'market_cap':       g('marketCap'),
            'book_value':       g('bookValue'),
            'dividend_yield':   g('dividendYield'),
            'beta':             g('beta'),
            'face_value':       g('floatShares'),
            '52w_high':         g('fiftyTwoWeekHigh'),
            '52w_low':          g('fiftyTwoWeekLow'),
            'avg_volume':       g('averageVolume'),
            'sector':           info.get('sector',''),
            'industry':         info.get('industry',''),
            'name':             info.get('longName',''),
        }
    except Exception as e:
        return None

def score_fundamentals(data):
    """Score fundamentals 0-10. Higher = better quality stock"""
    if not data: return 0.0, ['No data available']
    score   = 5.0
    reasons = []

    pe   = data.get('pe_ratio')
    pb   = data.get('pb_ratio')
    roe  = data.get('roe')
    de   = data.get('debt_equity')
    rg   = data.get('revenue_growth')
    eg   = data.get('earnings_growth')
    pm   = data.get('profit_margin')
    prm  = data.get('promoter_holding')
    beta = data.get('beta')

    # ── PE Ratio ──────────────────────────────────────────
    if pe:
        if pe < 0:
            score -= 1.5
            reasons.append(f'Negative PE ({pe:.1f}) — company making losses')
        elif pe < 15:
            score += 1.5
            reasons.append(f'Low PE {pe:.1f} — potentially undervalued')
        elif pe < 25:
            score += 0.5
            reasons.append(f'Fair PE {pe:.1f} — reasonably valued')
        elif pe < 40:
            score -= 0.5
            reasons.append(f'High PE {pe:.1f} — growth expected but priced in')
        else:
            score -= 1.5
            reasons.append(f'Very high PE {pe:.1f} — expensive, high risk')

    # ── ROE ───────────────────────────────────────────────
    if roe:
        roe_pct = roe * 100
        if roe_pct > 25:
            score += 2.0
            reasons.append(f'Excellent ROE {roe_pct:.1f}% — very efficient business')
        elif roe_pct > 15:
            score += 1.0
            reasons.append(f'Good ROE {roe_pct:.1f}% — efficient use of capital')
        elif roe_pct > 8:
            score += 0.3
            reasons.append(f'Average ROE {roe_pct:.1f}%')
        else:
            score -= 1.0
            reasons.append(f'Poor ROE {roe_pct:.1f}% — weak returns on equity')

    # ── Debt/Equity ────────────────────────────────────────
    if de is not None:
        if de < 0.3:
            score += 1.5
            reasons.append(f'Very low debt ({de:.2f}x) — strong balance sheet')
        elif de < 1.0:
            score += 0.5
            reasons.append(f'Manageable debt ({de:.2f}x)')
        elif de < 2.0:
            score -= 0.5
            reasons.append(f'Moderate debt ({de:.2f}x) — watch interest costs')
        else:
            score -= 1.5
            reasons.append(f'High debt ({de:.2f}x) — risky balance sheet')

    # ── Revenue Growth ────────────────────────────────────
    if rg:
        rg_pct = rg * 100
        if rg_pct > 20:
            score += 1.5
            reasons.append(f'Strong revenue growth {rg_pct:.1f}%')
        elif rg_pct > 10:
            score += 0.8
            reasons.append(f'Good revenue growth {rg_pct:.1f}%')
        elif rg_pct > 0:
            score += 0.2
            reasons.append(f'Modest revenue growth {rg_pct:.1f}%')
        else:
            score -= 1.0
            reasons.append(f'Revenue declining {rg_pct:.1f}% — concerning')

    # ── Earnings Growth ────────────────────────────────────
    if eg:
        eg_pct = eg * 100
        if eg_pct > 25:
            score += 1.0
            reasons.append(f'Excellent earnings growth {eg_pct:.1f}%')
        elif eg_pct > 10:
            score += 0.5
            reasons.append(f'Good earnings growth {eg_pct:.1f}%')
        elif eg_pct < 0:
            score -= 1.0
            reasons.append(f'Earnings declining {eg_pct:.1f}%')

    # ── Profit Margin ──────────────────────────────────────
    if pm:
        pm_pct = pm * 100
        if pm_pct > 20:
            score += 0.5
            reasons.append(f'High margin {pm_pct:.1f}% — pricing power')
        elif pm_pct < 5:
            score -= 0.5
            reasons.append(f'Thin margin {pm_pct:.1f}% — vulnerable')

    # ── Promoter Holding ──────────────────────────────────
    if prm:
        prm_pct = prm * 100
        if prm_pct > 50:
            score += 0.5
            reasons.append(f'High promoter holding {prm_pct:.0f}% — skin in game')
        elif prm_pct < 25:
            score -= 0.3
            reasons.append(f'Low promoter holding {prm_pct:.0f}% — less conviction')

    return round(min(max(score, 0), 10), 2), reasons

def save_fundamentals(data, fund_score, reasons):
    conn = sqlite3.connect(DB)
    c = conn.cursor()

    # Save to fundamentals table
    c.execute('''INSERT OR REPLACE INTO fundamentals
        (symbol, pe_ratio, pb_ratio, eps, roe, debt_equity,
         revenue_growth, profit_growth, promoter_holding,
         market_cap, dividend_yield, updated_on)
        VALUES (?,?,?,?,?,?,?,?,?,?,?, date('now'))''', (
        data['symbol'],
        data.get('pe_ratio'),
        data.get('pb_ratio'),
        data.get('eps'),
        data.get('roe'),
        data.get('debt_equity'),
        data.get('revenue_growth'),
        data.get('earnings_growth'),
        data.get('promoter_holding'),
        data.get('market_cap'),
        data.get('dividend_yield'),
    ))

    # Update signals table with fund_score
    existing = c.execute('SELECT tech_score, final_signal FROM signals WHERE symbol=?',
                         (data['symbol'],)).fetchone()
    if existing:
        tech_score = existing[0] or 5.0
        # Combined score
        combined = round((fund_score * 0.4 + tech_score * 0.6), 2)
        if combined >= 8.0:   final = 'STRONG BUY'
        elif combined >= 6.5: final = 'BUY'
        elif combined >= 4.5: final = 'HOLD'
        elif combined >= 3.0: final = 'SELL'
        else:                  final = 'STRONG SELL'
        # Update reason with fundamental reasons
        fund_reason = ' | '.join(reasons[:2])
        c.execute('''UPDATE signals SET
            fund_score=?, final_signal=?,
            reason = reason || ' || F: ' || ?
            WHERE symbol=?''',
            (fund_score, final, fund_reason, data['symbol']))

    conn.commit()
    conn.close()

def run_all(mode='top50'):
    symbols = get_symbols(mode)
    print(f"\n{'='*60}")
    print(f"  FUNDAMENTALS ENGINE")
    print(f"  Fetching {len(symbols)} stocks from Yahoo Finance...")
    print(f"{'='*60}\n")

    success, failed = [], []
    for i, symbol in enumerate(symbols):
        print(f"  [{i+1:3}/{len(symbols)}] {symbol:15}", end=' ', flush=True)
        data = fetch_fundamentals(symbol)
        if not data:
            print('✗ no data')
            failed.append(symbol)
            continue
        score, reasons = score_fundamentals(data)
        save_fundamentals(data, score, reasons)

        pe_str  = f"PE:{data.get('pe_ratio'):.1f}" if data.get('pe_ratio') else 'PE:--'
        roe_str = f"ROE:{data.get('roe')*100:.0f}%" if data.get('roe') else 'ROE:--'
        flag    = ' ⭐' if score >= 7 else (' ⚠' if score <= 3 else '')
        print(f"✓ Score:{score} {pe_str} {roe_str}{flag}")
        success.append(symbol)
        time.sleep(0.3)  # rate limit

    # Summary
    print(f"\n{'='*60}")
    print(f"  ✓ Done: {len(success)}  ✗ Failed: {len(failed)}")

    # Top fundamentally strong stocks
    conn = sqlite3.connect(DB)
    c = conn.cursor()
    c.execute('''SELECT s.symbol, s.fund_score, s.final_signal,
                        f.pe_ratio, f.roe, f.debt_equity, f.revenue_growth
                 FROM signals s JOIN fundamentals f ON s.symbol=f.symbol
                 ORDER BY s.fund_score DESC LIMIT 15''')
    rows = c.fetchall()
    conn.close()

    if rows:
        print(f"\n  TOP FUNDAMENTALLY STRONG STOCKS:")
        print(f"  {'Symbol':12} {'FScore':7} {'Signal':12} {'PE':6} {'ROE%':7} {'D/E':6} {'RevGr%':7}")
        print(f"  {'-'*60}")
        for r in rows:
            pe  = f"{r[3]:.1f}" if r[3] else '--'
            roe = f"{r[4]*100:.0f}%" if r[4] else '--'
            de  = f"{r[5]:.2f}" if r[5] else '--'
            rg  = f"{r[6]*100:.0f}%" if r[6] else '--'
            print(f"  {r[0]:12} {str(r[1]):7} {str(r[2]):12} {pe:6} {roe:7} {de:6} {rg:7}")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    arg = sys.argv[1] if len(sys.argv)>1 else '--top50'
    if   arg == '--top50':  run_all('top50')
    elif arg == '--top500': run_all('top500')
    elif arg == '--all':    run_all('all')
    else: print("Usage: python3 engine/fundamentals.py --top50 | --top500 | --all")
