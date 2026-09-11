import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_DOWN
import aiohttp
from aiohttp import web
import asyncpg
import ccxt.async_support as ccxt
import pandas as pd
from tabulate import tabulate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("MEXC_v4_DB")

def format_precision(val: float, precision: int = 8) -> Decimal:
    """معالجة الدقة الرقمية وتفادي أخطاء التقريب العلمي"""
    d = Decimal(str(val))
    return d.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

class ProductionScalpEngine:
    def __init__(self):
        # 1. المتغيرات البيئية من Railway
        self.system_name = os.getenv("SYSTEM_NAME", "MEXC-v4")
        self.paper_trading = os.getenv("PAPER_TRADING", "True").lower() == "true"
        self.initial_capital = float(os.getenv("INITIAL_CAPITAL", "500.0"))
        self.cash_usdt = self.initial_capital
        
        self.max_open_positions = int(os.getenv("MAX_SLOTS", "4"))
        self.slot_size = float(os.getenv("FIXED_TRADE_USD", "100.0"))
        self.max_total_capital = float(os.getenv("MAX_TOTAL_CAPITAL", "400.0"))
        self.db_url = os.getenv("DATABASE_URL", "")
        
        self.target_symbols = ["ETH/USDT", "SOL/USDT", "XRP/USDT", "BNB/USDT", "ADA/USDT"]
        
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        
        # أهداف الربح والمخاطرة
        self.take_profit_pct = 0.0075
        self.hard_stop_loss_pct = 0.010
        self.time_sl_threshold = 0.0035
        self.max_holding_minutes = 25
        
        # عمولات MEXC Spot
        self.taker_fee = 0.001
        self.maker_fee = 0.000
        
        # التتبع والأرباح
        self.trade_counter = 0
        self.current_month = datetime.now(timezone.utc).month
        self.monthly_realized_pnl = 0.0
        
        self.open_positions = {}
        self.cooldown_tracker = {}
        self.closed_trades = []
        self.db_pool = None
        self.web_runner = None
        
        # إعداد عميل MEXC
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

    async def start_dummy_server(self):
        """خادم ويب خفيف للرد على فحص الحياة (Healthcheck) في Railway لمنع إيقاف الحاوية"""
        try:
            app = web.Application()
            app.router.add_get('/', lambda r: web.Response(text="Bot is running!"))
            app.router.add_get('/health', lambda r: web.Response(text="OK"))
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()
            port = int(os.getenv("PORT", 8080))
            site = web.TCPSite(self.web_runner, '0.0.0.0', port)
            await site.start()
            logger.info(f"🌐 [Web Server] الخادم المصغر يعمل بنجاح على المنفذ: {port}")
        except Exception as e:
            logger.error(f"فشل تشغيل خادم الويب المصغر: {e}")

    async def init_database(self):
        """الاتصال بـ PostgreSQL وإنشاء جدول الصفقات إذا لم يكن موجوداً"""
        if not self.db_url:
            logger.warning("⚠️ لم يتم تعيين DATABASE_URL! سيعمل البوت بالذاكرة المؤقتة فقط.")
            return

        try:
            self.db_pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=5)
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id SERIAL PRIMARY KEY,
                        trade_id VARCHAR(50) UNIQUE NOT NULL,
                        system_name VARCHAR(50) NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        status VARCHAR(20) NOT NULL,
                        entry_price NUMERIC(18, 8) NOT NULL,
                        exit_price NUMERIC(18, 8),
                        cost_usd NUMERIC(18, 4) NOT NULL,
                        crypto_qty NUMERIC(18, 8) NOT NULL,
                        entry_fee NUMERIC(18, 6) NOT NULL,
                        exit_fee NUMERIC(18, 6) DEFAULT 0,
                        take_profit NUMERIC(18, 8) NOT NULL,
                        stop_loss NUMERIC(18, 8) NOT NULL,
                        pnl_usd NUMERIC(18, 4) DEFAULT 0,
                        pnl_pct NUMERIC(18, 4) DEFAULT 0,
                        holding_duration_sec INT DEFAULT 0,
                        exit_reason VARCHAR(100),
                        running_monthly_pnl NUMERIC(18, 4) DEFAULT 0,
                        balance_snapshot NUMERIC(18, 4) DEFAULT 0,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        closed_at TIMESTAMP WITH TIME ZONE
                    );
                    CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
                    CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at);
                """)
            logger.info("✅ [PostgreSQL] تم الاتصال بقاعدة البيانات والتحقق من الجداول بنجاح.")
        except Exception as e:
            logger.error(f"فشل الاتصال بقاعدة البيانات: {e}")

    async def auto_heal(self):
        """استعادة ومزامنة المراكز المفتوحة والأرباح التراكمية عند الإقلاع"""
        logger.info("🛠️ [Auto-Heal] بدء استعادة ومزامنة حالة النظام...")
        
        if not self.paper_trading:
            try:
                open_orders = await self.exchange.fetch_open_orders()
                for o in open_orders:
                    await self.exchange.cancel_order(o['id'], o['symbol'])
                balance = await self.exchange.fetch_balance()
                self.cash_usdt = float(balance['free'].get('USDT', 0.0))
            except Exception as e:
                logger.error(f"خطأ أثناء مزامنة المنصة: {e}")
                raise

        if self.db_pool:
            try:
                now = datetime.now(timezone.utc)
                start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

                async with self.db_pool.acquire() as conn:
                    # 1. استرجاع أرباح الشهر الحالي
                    sum_row = await conn.fetchrow("""
                        SELECT COALESCE(SUM(pnl_usd), 0) as month_pnl, COUNT(*) as total_cnt
                        FROM trades 
                        WHERE status = 'CLOSED' AND closed_at >= $1
                    """, start_of_month)
                    self.monthly_realized_pnl = float(sum_row['month_pnl'])
                    
                    # 2. تحديد آخر رقم تسلسلي
                    count_row = await conn.fetchrow("SELECT COUNT(*) as total FROM trades")
                    self.trade_counter = int(count_row['total'])

                    # 3. استعادة الصفقات المفتوحة التي انقطعت
                    open_rows = await conn.fetch("SELECT * FROM trades WHERE status = 'OPEN'")
                    for r in open_rows:
                        self.open_positions[r['symbol']] = {
                            'trade_id': r['trade_id'],
                            'entry_price': float(r['entry_price']),
                            'qty': float(r['crypto_qty']),
                            'cost': float(r['cost_usd']),
                            'take_profit': float(r['take_profit']),
                            'stop_loss': float(r['stop_loss']),
                            'entry_time': r['created_at'],
                            'entry_fee': float(r['entry_fee'])
                        }
                        self.cash_usdt -= float(r['cost_usd'])
                        logger.warning(f"🔄 [استعادة مركز مفتوح] {r['trade_id']} على {r['symbol']} مستمر في التداول!")

                logger.info(f"📊 [Postgres] تم استعادة أرباح الشهر: ${self.monthly_realized_pnl:,.2f} | صفقات مستعادة: {len(self.open_positions)}")
            except Exception as e:
                logger.error(f"خطأ استعادة بيانات قاعدة البيانات: {e}")

        logger.info(f"✅ [Auto-Heal] اكتملت الجاهزية | الكاش المتاح: ${self.cash_usdt:,.2f} USDT")

    async def send_telegram_alert(self, message: str):
        """إرسال إشعار لحظي عبر تيليجرام"""
        if not self.tg_token or not self.tg_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {"chat_id": self.tg_chat_id, "text": message, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"فشل إرسال تيليجرام: {await resp.text()}")
        except Exception as e:
            logger.error(f"خطأ اتصال تيليجرام: {e}")

    def check_monthly_reset(self):
        """تصفير الأرباح التراكمية في بداية كل شهر جديد"""
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

    async def check_btc_shield(self) -> bool:
        """درع اتجاه البيتكوين: حظر الشراء إذا كان BTC أدنى من EMA20 على 15m"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv('BTC/USDT', timeframe='15m', limit=30)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['c'] = df['c'].astype(float)
            df['ema20'] = df['c'].ewm(span=20, adjust=False).mean()
            return df['c'].iloc[-1] >= df['ema20'].iloc[-1]
        except Exception as e:
            logger.error(f"خطأ في درع BTC: {e}")
            return False

    async def fetch_signals(self, symbol: str):
        """تحليل الشموع على فريم 1m باستخدام pandas"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe='1m', limit=40)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['c'] = df['c'].astype(float)

            df['ema_fast'] = df['c'].ewm(span=5, adjust=False).mean()
            df['ema_slow'] = df['c'].ewm(span=12, adjust=False).mean()

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
            logger.error(f"خطأ مؤشرات {symbol}: {e}")
            return None, 0.0

    async def open_position(self, symbol: str, current_price: float):
        """فتح مركز جديد مع تطبيق قيود المخاطر وحفظ البيانات في Postgres"""
        allocated_capital = sum(p['cost'] for p in self.open_positions.values())
        if (allocated_capital + self.slot_size) > self.max_total_capital or self.cash_usdt < self.slot_size:
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

        # حفظ الصفقة في قاعدة البيانات
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trades (
                            trade_id, system_name, symbol, status, entry_price, 
                            cost_usd, crypto_qty, entry_fee, take_profit, stop_loss, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """, trade_id, self.system_name, symbol, 'OPEN', entry_price, 
                         cost, crypto_qty, entry_fee, tp, sl, now)
            except Exception as e:
                logger.error(f"فشل إدراج الصفقة في DB: {e}")

        alert_msg = (
            f"🟢 *[{self.system_name}] صفقة جديدة مفتوحة*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 *رقم الصفقة:* `{trade_id}`\n"
            f"🪙 *الزوج:* `{symbol}`\n"
            f"💵 *سعر الدخول:* `${current_price:,.4f}`\n"
            f"📦 *حجم المركز:* `${cost:.2f}`\n"
            f"🎯 *الهدف (TP):* `${tp:,.4f}` (+0.75%)\n"
            f"🛑 *وقف الخسارة (SL):* `${sl:,.4f}` (-1.00%)\n"
            f"💼 *الكاش المتبقي:* `${self.cash_usdt:,.2f}`\n"
            f"📊 *المراكز النشطة:* `{len(self.open_positions)}/{self.max_open_positions}`"
        )
        logger.info(f"🟢 [فتح مركز] {trade_id} على {symbol}")
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
        duration_sec = int((now - pos['entry_time']).total_seconds())
        duration_min = duration_sec / 60

        if net_pnl < 0:
            self.cooldown_tracker[symbol] = now + timedelta(minutes=20)

        # تحديث الصفقة في PostgreSQL
        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE trades SET 
                            status = 'CLOSED',
                            exit_price = $1,
                            exit_fee = $2,
                            pnl_usd = $3,
                            pnl_pct = $4,
                            holding_duration_sec = $5,
                            exit_reason = $6,
                            running_monthly_pnl = $7,
                            balance_snapshot = $8,
                            closed_at = $9
                        WHERE trade_id = $10
                    """, current_price, exit_fee, net_pnl, net_pnl_pct, duration_sec, 
                         reason, self.monthly_realized_pnl, self.cash_usdt, now, pos['trade_id'])
            except Exception as e:
                logger.error(f"فشل تحديث الصفقة في DB: {e}")

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
            f"💼 *رصيد الكاش اللحظي:* `${self.cash_usdt:,.2f}`"
        )
        logger.info(f"🔒 [إغلاق مركز] {pos['trade_id']} | صافي: ${net_pnl:+.2f}")
        await self.send_telegram_alert(alert_msg)

    async def monitor_open_positions(self):
        """مراقبة الأهداف وتطبيق الخروج الزمني وحساب إجمالي حقوق الملكية MTM"""
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
            # الخروج الزمني لمنع احتجاز السيولة
            elif duration_minutes >= self.max_holding_minutes and pnl_pct <= -self.time_sl_threshold:
                await self.close_position(symbol, current_price, "Time SL (Decay Protection)", is_maker=False)

        return total_open_value

    def display_dashboard(self, total_mtm_equity: float):
        """لوحة المتابعة الشفافة في السجلات"""
        allocated_cap = sum(p['cost'] for p in self.open_positions.values())
        headers = ["المعيار", "القيمة"]
        rows = [
            ["النظام", self.system_name],
            ["الكاش المتاح", f"${self.cash_usdt:,.2f}"],
            ["رأس المال المحجوز بالصفقات", f"${allocated_cap:,.2f} / ${self.max_total_capital:,.2f}"],
            ["إجمالي حقوق الملكية (MTM)", f"${total_mtm_equity:,.2f}"],
            ["أرباح الشهر التراكمية (DB)", f"${self.monthly_realized_pnl:,.2f}"],
            ["المراكز المفتوحة النشطة", f"{len(self.open_positions)} / {self.max_open_positions}"],
            ["إجمالي الصفقات المنفذة", f"{self.trade_counter}"]
        ]
        print("\n" + tabulate(rows, headers=headers, tablefmt="fancy_grid"))

    async def run(self):
        # 1. تشغيل خادم الرد السريع لمنع إيقاف الحاوية
        await self.start_dummy_server()
        
        # 2. تهيئة الاتصال بقاعدة البيانات ومزامنة الحالة
        await self.init_database()
        await self.auto_heal()
        logger.info(f"🚀 تم تشغيل {self.system_name} بنجاح.")
        
        mode_str = "تجريبي (Paper Trading)" if self.paper_trading else "🔴 حقيقي (Live Money)"
        await self.send_telegram_alert(
            f"🚀 *[{self.system_name}] إقلاع النظام والمزامنة*\n"
            f"الوضع: `{mode_str}`\n"
            f"المراكز المستعادة: `{len(self.open_positions)}`\n"
            f"الأرباح الشهرية السابقة: `${self.monthly_realized_pnl:,.2f}`\n"
            f"الكاش الحالي: `${self.cash_usdt:,.2f}`"
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
        if self.web_runner:
            await self.web_runner.cleanup()
        if self.db_pool:
            await self.db_pool.close()
        await self.exchange.close()

if __name__ == "__main__":
    bot = ProductionScalpEngine()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("إيقاف آمن للبوت...")
    finally:
        asyncio.run(bot.shutdown())
