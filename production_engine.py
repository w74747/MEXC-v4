import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
import aiohttp
import ccxt.async_support as ccxt
import pandas as pd
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("MEXC_v4")

def format_precision(val: float, precision: int = 8) -> Decimal:
    """معالجة الدقة الرقمية وتفادي أخطاء التقريب العلمي"""
    d = Decimal(str(val))
    return d.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

class ProductionScalpEngine:
    def __init__(self):
        # قراءة المتغيرات البيئية من Railway
        self.system_name = os.getenv("SYSTEM_NAME", "MEXC-v4")
        self.paper_trading = os.getenv("PAPER_TRADING", "True").lower() == "true"
        self.initial_capital = float(os.getenv("INITIAL_CAPITAL", "500.0"))
        self.cash_usdt = self.initial_capital
        
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        
        # إدارة التخصيص والمخاطر
        self.slot_size = 100.0          # 100 USDT ثابتة لكل مركز
        self.max_open_positions = 3     # 3 مراكز متزامنة كحد أقصى
        self.target_symbols = ["ETH/USDT", "SOL/USDT", "XRP/USDT"]
        
        self.take_profit_pct = 0.0075   # هدف الربح: +0.75%
        self.hard_stop_loss_pct = 0.010 # وقف الخسارة الصارم: -1.0%
        self.time_sl_threshold = 0.0035 # تصفية جبرية إذا استمر التراجع بعد انتهاء المهلة (-0.35%)
        self.max_holding_minutes = 25   # أقصى مدة بقاء للمركز (25 دقيقة)
        
        # هيكل رسوم التداول الخاص بـ MEXC Spot
        self.taker_fee = 0.001   # 0.10% عند الدخول الفوري بأمر السوق
        self.maker_fee = 0.000   # 0.0% عمولة صانع السوق (Maker) في MEXC
        
        # عدادات وتتبع الصفقات والأرباح الشهرية
        self.trade_counter = 0
        self.current_month = datetime.now(timezone.utc).month
        self.monthly_realized_pnl = 0.0
        
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

    async def send_telegram_alert(self, message: str):
        """إرسال إشعار لحظي عبر تيليجرام"""
        if not self.tg_token or not self.tg_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"فشل إرسال رسالة تيليجرام: {await resp.text()}")
        except Exception as e:
            logger.error(f"خطأ أثناء الاتصال بتيليجرام: {e}")

    def check_monthly_reset(self):
        """تصفير الأرباح التراكمية تلقائياً عند حلول أول دقيقة من كل شهر جديد"""
        now = datetime.now(timezone.utc)
        if now.month != self.current_month:
            logger.info(f"🔄 بداية شهر جديد ({now.strftime('%B')})! تصفير العداد التراكمي للأرباح.")
            self.current_month = now.month
            self.monthly_realized_pnl = 0.0
            asyncio.create_task(
                self.send_telegram_alert(
                    f"📅 *[{self.system_name}] إشعار دوري: بداية شهر جديد*\n"
                    f"تم تصفير عداد الأرباح الشهرية التراكمية لشهر: *{now.strftime('%B %Y')}*."
                )
            )

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
            df['c'] = df['c'].astype(float)
            df['ema20'] = df['c'].ewm(span=20, adjust=False).mean()
            
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
        """تحليل الشموع على فريم 1m باستخدام pandas النقي دون الحاجة لمكتبات خارجية"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=40)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['c'] = df['c'].astype(float)

            # حساب EMA 5 و EMA 12 عبر pandas مباشرة
            df['ema_fast'] = df['c'].ewm(span=5, adjust=False).mean()
            df['ema_slow'] = df['c'].ewm(span=12, adjust=False).mean()

            # حساب RSI 14 بدقة قياسية
            delta = df['c'].diff()
            gain = (delta.where(delta > 0, 0.0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
            rs = gain / (loss.replace(0, float('nan')))
            df['rsi'] = 100 - (100 / (1 + rs))

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
        """فتح مركز جديد وحساب مستويات الخروج وإرسال إشعار تيليجرام"""
        if self.cash_usdt < self.slot_size:
            return

        now = datetime.now(timezone.utc)
        if symbol in self.cooldown_tracker and now < self.cooldown_tracker[symbol]:
            return

        self.trade_counter += 1
        trade_id = f"TRD-{self.trade_counter:04d}"
        
        cost = self.slot_size
        entry_fee = cost * self.taker_fee
        net_invested = cost - entry_fee
        crypto_qty = float(format_precision(net_invested / current_price, 6))

        tp = current_price * (1 + self.take_profit_pct)
        sl = current_price * (1 - self.hard_stop_loss_pct)

        self.cash_usdt -= cost
        self.open_positions[symbol] = {
            'trade_id': trade_id,
            'entry_price': current_price,
            'qty': crypto_qty,
            'cost': cost,
            'take_profit': tp,
            'stop_loss': sl,
            'entry_time': now,
            'entry_fee': entry_fee
        }

        alert_msg = (
            f"🟢 *[{self.system_name}] صفقة جديدة مفتوحة*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 *رقم الصفقة:* `{trade_id}`\n"
            f"🪙 *الزوج:* `{symbol}`\n"
            f"💵 *سعر الدخول:* `${current_price:,.4f}`\n"
            f"📦 *حجم المركز:* `${cost:.2f}`\n"
            f"🎯 *الهدف (TP):* `${tp:,.4f}` (+0.75%)\n"
            f"🛑 *وقف الخسارة (SL):* `${sl:,.4f}` (-1.00%)\n"
            f"💼 *الكاش المتبقي:* `${self.cash_usdt:,.2f}`"
        )
        logger.info(f"🟢 [دخول صفقة] {trade_id} على {symbol} | السعر: ${current_price:,.4f}")
        await self.send_telegram_alert(alert_msg)

    async def close_position(self, symbol: str, current_price: float, reason: str, is_maker: bool = False):
        """إغلاق المركز وتحديث الأرباح الصافية وإرسال تقرير تيليجرام"""
        pos = self.open_positions.pop(symbol)
        gross_value = pos['qty'] * current_price
        exit_fee = gross_value * (self.maker_fee if is_maker else self.taker_fee)
        net_return = gross_value - exit_fee
        
        net_pnl = net_return - pos['cost']
        net_pnl_pct = (net_pnl / pos['cost']) * 100

        self.cash_usdt += net_return
        self.monthly_realized_pnl += net_pnl
        now = datetime.now(timezone.utc)
        duration_min = (now - pos['entry_time']).total_seconds() / 60

        if net_pnl < 0:
            self.cooldown_tracker[symbol] = now + timedelta(minutes=20)
            logger.warning(f"❄️ تفعيل Cooldown لمدة 20 دقيقة على {symbol}.")

        self.closed_trades.append({
            'trade_id': pos['trade_id'],
            'symbol': symbol,
            'reason': reason,
            'pnl_usd': net_pnl,
            'pnl_pct': net_pnl_pct,
            'exit_price': current_price
        })

        pnl_icon = "🟢 ربح" if net_pnl >= 0 else "🔴 خسارة"
        month_name = now.strftime('%B')
        alert_msg = (
            f"🔒 *[{self.system_name}] إغلاق صفقة*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 *رقم الصفقة:* `{pos['trade_id']}`\n"
            f"🪙 *الزوج:* `{symbol}`\n"
            f"📌 *السبب:* {reason}\n"
            f"⏱️ *مدة الاحتفاظ:* {duration_min:.1f} دقيقة\n"
            f"💵 *سعر الخروج:* `${current_price:,.4f}`\n"
            f"📊 *النتيجة:* {pnl_icon} `{'+$' if net_pnl >= 0 else '-$'}{abs(net_pnl):.2f}` ({net_pnl_pct:+.2f}%)\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📈 *صافي أرباح شهر {month_name}:* `{'+$' if self.monthly_realized_pnl >= 0 else '-$'}{abs(self.monthly_realized_pnl):.2f}`\n"
            f"💼 *رصيد الكاش الحالي:* `${self.cash_usdt:,.2f}`"
        )
        logger.info(f"🔒 [إغلاق مركز] {pos['trade_id']} | صافي: ${net_pnl:+.2f}")
        await self.send_telegram_alert(alert_msg)

    async def monitor_open_positions(self):
        """مراقبة الأهداف وتطبيق الخروج الزمني وحساب القيمة السوقية الحية"""
        now = datetime.now(timezone.utc)
        total_open_value = 0.0

        for symbol in list(self.open_positions.keys()):
            pos = self.open_positions[symbol]
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            
            total_open_value += (pos['qty'] * current_price)
            duration_minutes = (now - pos['entry_time']).total_seconds() / 60
            pnl_pct = (current_price - pos['entry_price']) / pos['entry_price']

            # جني الأرباح (Maker = صفر عمولة في MEXC)
            if current_price >= pos['take_profit']:
                await self.close_position(symbol, current_price, "Take Profit (Maker 0% Fee)", is_maker=True)
            # وقف الخسارة الصارم
            elif current_price <= pos['stop_loss']:
                await self.close_position(symbol, current_price, "Hard Stop Loss (Market)", is_maker=False)
            # الخروج الزمني لمنع تجميد السيولة
            elif duration_minutes >= self.max_holding_minutes and pnl_pct <= -self.time_sl_threshold:
                await self.close_position(symbol, current_price, "Time SL (Decay Protection)", is_maker=False)

        return total_open_value

    def display_dashboard(self, total_mtm_equity: float):
        """لوحة المتابعة الشفافة في السجلات"""
        total_trades = len(self.closed_trades)
        wins = sum(1 for t in self.closed_trades if t['pnl_usd'] > 0)
        win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0

        headers = ["المعيار", "القيمة"]
        rows = [
            ["النظام", self.system_name],
            ["الكاش المتاح", f"${self.cash_usdt:,.2f}"],
            ["إجمالي حقوق الملكية (MTM)", f"${total_mtm_equity:,.2f}"],
            ["أرباح الشهر التراكمية", f"${self.monthly_realized_pnl:,.2f}"],
            ["المراكز المفتوحة", f"{len(self.open_positions)} / {self.max_open_positions}"],
            ["الصفقات المغلقة", f"{total_trades} (Win Rate: {win_rate:.1f}%)"]
        ]
        print("\n" + tabulate(rows, headers=headers, tablefmt="fancy_grid"))

    async def run(self):
        await self.auto_heal()
        logger.info(f"🚀 تم تشغيل {self.system_name} بنجاح. بدء المراقبة...")
        
        mode_str = "تجريبي (Paper Trading)" if self.paper_trading else "🔴 حقيقي (Live Money)"
        await self.send_telegram_alert(
            f"🚀 *[{self.system_name}] تم تشغيل النظام بنجاح*\n"
            f"الوضع: `{mode_str}`\n"
            f"رأس المال الابتدائي: `${self.initial_capital:,.2f}`"
        )
        
        cycle_count = 0
        while True:
            try:
                self.check_monthly_reset()
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
                if cycle_count % 6 == 0:
                    self.display_dashboard(total_equity)

                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"خطأ في دورة التداول: {e}")
                await asyncio.sleep(5)

    async def shutdown(self):
        await self.exchange.close()

if __name__ == "__main__":
    bot = ProductionScalpEngine()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("إيقاف آمن للبوت...")
    finally:
        asyncio.run(bot.shutdown())
