import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("MEXC_ScalpEngine")

def format_precision(val: float, precision: int = 8) -> Decimal:
    """معالجة الدقة الرقمية وتفادي أخطاء التقريب العلمي"""
    d = Decimal(str(val))
    return d.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

class ProductionScalpEngine:
    def __init__(self, initial_capital: float = 500.0, paper_trading: bool = True):
        self.paper_trading = paper_trading
        self.initial_capital = initial_capital
        self.cash_usdt = initial_capital
        
        # إدارة رأس المال والمراكز
        self.slot_size = 100.0          # 100 USDT ثابتة لكل مركز
        self.max_open_positions = 3     # 3 مراكز متزامنة كحد أقصى
        self.target_symbols = ["ETH/USDT", "SOL/USDT", "XRP/USDT"]
        
        # أهداف الربح والمخاطرة (R:R)
        self.take_profit_pct = 0.0075   # هدف الربح: +0.75%
        self.hard_stop_loss_pct = 0.010 # وقف الخسارة الصارم: -1.0%
        self.time_sl_threshold = 0.0035 # تصفية جبرية إذا استمر التراجع بعد انتهاء المهلة (-0.35%)
        self.max_holding_minutes = 25   # أقصى مدة بقاء للمركز (25 دقيقة)
        
        # هيكل رسوم التداول الخاص بـ MEXC Spot
        self.taker_fee = 0.001   # 0.10% عند الدخول الفوري بأمر السوق
        self.maker_fee = 0.000   # 0.0% عمولة صانع السوق (Maker) في MEXC
        
        # تتبع المراكز وحالة التداول
        self.open_positions = {}
        self.cooldown_tracker = {}
        self.closed_trades = []
        
        # إعداد الاتصال بمنصة MEXC وقراءة المفاتيح من متغيرات البيئة
        api_key = os.getenv("MEXC_API_KEY", "")
        api_secret = os.getenv("MEXC_API_SECRET", "")
        
        self.exchange = ccxt.mexc({
            'apiKey': api_key,
            'secret': api_secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot',
                'createMarketBuyOrderRequiresPrice': False
            }
        })

    async def auto_heal(self):
        """محرك المعالجة الذاتية: إلغاء الأوامر العالقة ومزامنة السيولة عند الإقلاع"""
        logger.info("🛠️ [Auto-Heal] جاري فحص الإقلاع ومزامنة المحفظة مع MEXC...")
        if not self.paper_trading:
            try:
                open_orders = await self.exchange.fetch_open_orders()
                for o in open_orders:
                    await self.exchange.cancel_order(o['id'], o['symbol'])
                    logger.warning(f"⚠️ تم إلغاء أمر معلق: {o['id']} على {o['symbol']}")
                
                balance = await self.exchange.fetch_balance()
                self.cash_usdt = float(balance['free'].get('USDT', 0.0))
            except Exception as e:
                logger.error(f"خطأ أثناء المزامنة الحية مع MEXC: {e}")
                raise
        logger.info(f"✅ [Auto-Heal] تمت المزامنة | الكاش المتاح: ${self.cash_usdt:,.2f} USDT")

    async def check_btc_shield(self) -> bool:
        """درع اتجاه البيتكوين: حظر الشراء إذا كان BTC تحت EMA20 على فريم 15m"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv('BTC/USDT', timeframe='15m', limit=30)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['ema20'] = ta.ema(df['c'], length=20)
            
            last_price = df['c'].iloc[-1]
            last_ema = df['ema20'].iloc[-1]
            
            is_bullish = last_price >= last_ema
            if not is_bullish:
                logger.warning(f"🛡️ [BTC Shield] مفعل: سعر BTC (${last_price:,.1f}) أدنى من EMA20 (${last_ema:,.1f}). الدخول محظور.")
            return is_bullish
        except Exception as e:
            logger.error(f"خطأ أثناء قراءة درع BTC: {e}")
            return False

    async def fetch_signals(self, symbol: str):
        """تحليل الشموع على فريم 1m لاقتناص فرص EMA السريعة"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=30)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['ema_fast'] = ta.ema(df['c'], length=5)
            df['ema_slow'] = ta.ema(df['c'], length=12)
            df['rsi'] = ta.rsi(df['c'], length=14)

            curr = df.iloc[-1]
            prev = df.iloc[-2]

            signal = None
            if (prev['ema_fast'] <= prev['ema_slow']) and (curr['ema_fast'] > curr['ema_slow']):
                if 42 <= curr['rsi'] <= 65:
                    signal = 'BUY'

            return signal, float(curr['c'])
        except Exception as e:
            logger.error(f"خطأ في جلب بيانات {symbol}: {e}")
            return None, 0.0

    async def open_position(self, symbol: str, current_price: float):
        """فتح مركز جديد مع خصم رسوم الدخول وحساب مستويات الخروج"""
        if self.cash_usdt < self.slot_size:
            return

        now = datetime.now(timezone.utc)
        if symbol in self.cooldown_tracker and now < self.cooldown_tracker[symbol]:
            return

        cost = self.slot_size
        entry_fee = cost * self.taker_fee
        net_invested = cost - entry_fee
        crypto_qty = float(format_precision(net_invested / current_price, 6))

        tp = current_price * (1 + self.take_profit_pct)
        sl = current_price * (1 - self.hard_stop_loss_pct)

        self.cash_usdt -= cost
        self.open_positions[symbol] = {
            'entry_price': current_price,
            'qty': crypto_qty,
            'cost': cost,
            'take_profit': tp,
            'stop_loss': sl,
            'entry_time': now,
            'entry_fee': entry_fee
        }

        logger.info(f"🟢 [دخول صفقة] {symbol} | السعر: ${current_price:,.4f} | التكلفة: ${cost:.2f} | الرسوم: ${entry_fee:.3f}")

    async def close_position(self, symbol: str, current_price: float, reason: str, is_maker: bool = False):
        """إغلاق المركز وتحديث الأرباح الصافية بدقة مع الاستفادة من صفر عمولة Maker"""
        pos = self.open_positions.pop(symbol)
        gross_value = pos['qty'] * current_price
        exit_fee = gross_value * (self.maker_fee if is_maker else self.taker_fee)
        net_return = gross_value - exit_fee
        
        net_pnl = net_return - pos['cost']
        net_pnl_pct = (net_pnl / pos['cost']) * 100

        self.cash_usdt += net_return
        now = datetime.now(timezone.utc)

        if net_pnl < 0:
            self.cooldown_tracker[symbol] = now + timedelta(minutes=20)
            logger.warning(f"❄️ تفعيل Cooldown لمدة 20 دقيقة على {symbol}.")

        self.closed_trades.append({
            'symbol': symbol,
            'reason': reason,
            'pnl_usd': net_pnl,
            'pnl_pct': net_pnl_pct,
            'exit_price': current_price
        })

        icon = "🎯" if net_pnl > 0 else "🛑"
        logger.info(f"{icon} [إغلاق مركز] {symbol} | السبب: {reason} | السعر: ${current_price:,.4f}")
        logger.info(f"💵 صافي PnL: {'+$' if net_pnl >= 0 else '-$'}{abs(net_pnl):.2f} ({net_pnl_pct:+.2f}%) | الرسوم الكلية: ${pos['entry_fee'] + exit_fee:.3f}")

    async def monitor_open_positions(self):
        """مراقبة الأهداف وتطبيق الخروج الزمني وحساب إجمالي القيمة الحية (MTM)"""
        now = datetime.now(timezone.utc)
        total_open_value = 0.0

        for symbol in list(self.open_positions.keys()):
            pos = self.open_positions[symbol]
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            
            total_open_value += (pos['qty'] * current_price)
            duration_minutes = (now - pos['entry_time']).total_seconds() / 60
            pnl_pct = (current_price - pos['entry_price']) / pos['entry_price']

            # جني الأرباح (Maker = صفر رسوم في MEXC)
            if current_price >= pos['take_profit']:
                await self.close_position(symbol, current_price, "Take Profit (Maker 0% Fee)", is_maker=True)
            # وقف الخسارة الصارم
            elif current_price <= pos['stop_loss']:
                await self.close_position(symbol, current_price, "Hard Stop Loss (Market)", is_maker=False)
            # الخروج الزمني لمنع احتجاز السيولة
            elif duration_minutes >= self.max_holding_minutes and pnl_pct <= -self.time_sl_threshold:
                await self.close_position(symbol, current_price, "Time SL (Decay Protection)", is_maker=False)

        return total_open_value

    def display_dashboard(self, total_mtm_equity: float):
        """شاشة المتابعة الرقمية الشفافة"""
        total_realized = sum(t['pnl_usd'] for t in self.closed_trades)
        wins = sum(1 for t in self.closed_trades if t['pnl_usd'] > 0)
        total_trades = len(self.closed_trades)
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

        headers = ["المعيار", "القيمة"]
        rows = [
            ["المنصة المربوطة", "MEXC Spot"],
            ["الكاش المتاح (USDT)", f"${self.cash_usdt:,.2f}"],
            ["إجمالي حقوق الملكية (MTM Equity)", f"${total_mtm_equity:,.2f}"],
            ["إجمالي النمو الصافي ($ PnL)", f"{'+$' if (total_mtm_equity - self.initial_capital) >= 0 else '-$'}{abs(total_mtm_equity - self.initial_capital):.2f}"],
            ["المراكز المفتوحة", f"{len(self.open_positions)} / {self.max_open_positions}"],
            ["الصفقات المغلقة", f"{total_trades} (Win Rate: {win_rate:.1f}%)"],
            ["الربح المحقق الصافي", f"${total_realized:,.2f}"]
        ]
        print("\n" + tabulate(rows, headers=headers, tablefmt="fancy_grid"))

    async def run(self):
        await self.auto_heal()
        logger.info("🚀 تم تشغيل المحرك على MEXC بنجاح. بدء المراقبة الدورية...")
        
        cycle_count = 0
        while True:
            try:
                btc_safe = await self.check_btc_shield()
                open_mtm_value = await self.monitor_open_positions()
                total_equity = self.cash_usdt + open_mtm_value

                if btc_safe and len(self.open_positions) < self.max_open_positions:
                    for symbol in self.target_symbols:
                        if symbol not in self.open_positions:
                            signal, price = await self.fetch_signals(symbol)
                            if signal == 'BUY':
                                await self.open_position(symbol, price)

                cycle_count += 1
                if cycle_count % 6 == 0:  # تحديث الجدول كل 30 ثانية
                    self.display_dashboard(total_equity)

                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"خطأ في دورة التداول: {e}")
                await asyncio.sleep(5)

    async def shutdown(self):
        await self.exchange.close()

if __name__ == "__main__":
    # تشغيل افتراضي على وضع الورق الحي (Paper Trading = True)
    bot = ProductionScalpEngine(initial_capital=500.0, paper_trading=True)
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("إيقاف آمن للبوت...")
    finally:
        asyncio.run(bot.shutdown())
