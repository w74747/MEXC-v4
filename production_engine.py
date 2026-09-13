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
logger = logging.getLogger("MEXC_v4_Institutional")

def format_precision(val: float, precision: int = 8) -> Decimal:
    """معالجة الدقة الرقمية وتفادي أخطاء التقريب العلمي"""
    d = Decimal(str(val))
    return d.quantize(Decimal(10) ** -precision, rounding=ROUND_DOWN)

class ProductionScalpEngine:
    def __init__(self):
        # 1. المتغيرات البيئية الأساسية من Railway
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
        
        # 2. القواعد المؤسسية وإدارة المخاطر (فريم 5m)
        self.timeframe = "5m"
        self.base_stop_loss_pct = 0.0060        # وقف الخسارة الابتدائي الصارم (-0.60%)
        self.min_atr_pct = 0.0025               # فلتر التقلب: حظر الدخول إذا كان ATR أقل من 0.25%
        self.daily_max_loss_pct = 0.03          # قاطع الدائرة اليومي: إيقاف التداول عند خسارة 3%
        self.hard_time_cap_minutes = 60         # سقف زمني 60 دقيقة لفريم 5m لتحرير رأس المال
        
        # نسب الوقف المتحرك المتدرج وقفل الأرباح (Trailing Stop Steps)
        self.be_trigger_pct = 0.0040            # تأمين الدخول عند +0.40%
        self.lock_step1_trigger_pct = 0.0080    # عند +0.80% ربح
        self.lock_step1_profit_pct = 0.0045     # نقفل ربح صافي +0.45%
        self.trail_activation_pct = 0.0120      # بدء التتبع الصاروخي المفتوح عند +1.20%
        self.trail_callback_pct = 0.0030        # ارتداد 0.30% فقط عن القمة للخروج
        
        # هيكل عمولات MEXC Spot
        self.taker_fee = 0.001                  # 0.10% Taker
        self.maker_fee = 0.000                  # 0.0% Maker (صفر عمولة)
        
        # التتبع والأرباح
        self.trade_counter = 0
        self.current_month = datetime.now(timezone.utc).month
        self.current_day = datetime.now(timezone.utc).day
        self.monthly_realized_pnl = 0.0
        self.daily_realized_pnl = 0.0
        self.circuit_breaker_triggered = False
        
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
        """خادم ويب خفيف للرد على Healthcheck في Railway لمنع إيقاف الحاوية"""
        try:
            app = web.Application()
            app.router.add_get('/', lambda r: web.Response(text="MEXC-v4 Institutional Engine is active!"))
            app.router.add_get('/health', lambda r: web.Response(text="OK"))
            self.web_runner = web.AppRunner(app)
            await self.web_runner.setup()
            port = int(os.getenv("PORT", 8080))
            site = web.TCPSite(self.web_runner, '0.0.0.0', port)
            await site.start()
            logger.info(f"🌐 [Web Server] خادم الفحص المصغر يعمل على المنفذ: {port}")
        except Exception as e:
            logger.error(f"فشل تشغيل خادم الويب: {e}")

    async def init_database(self):
        """الاتصال بـ PostgreSQL والتحقق من الجداول"""
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
            logger.info("✅ [PostgreSQL] تم الاتصال بقاعدة البيانات بنجاح.")
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
                start_of_day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)

                async with self.db_pool.acquire() as conn:
                    # أرباح الشهر
                    sum_month = await conn.fetchrow("""
                        SELECT COALESCE(SUM(pnl_usd), 0) as month_pnl
                        FROM trades WHERE status = 'CLOSED' AND closed_at >= $1
                    """, start_of_month)
                    self.monthly_realized_pnl = float(sum_month['month_pnl'])

                    # أرباح اليوم لقاطع الدائرة
                    sum_day = await conn.fetchrow("""
                        SELECT COALESCE(SUM(pnl_usd), 0) as day_pnl
                        FROM trades WHERE status = 'CLOSED' AND closed_at >= $1
                    """, start_of_day)
                    self.daily_realized_pnl = float(sum_day['day_pnl'])

                    # عداد الصفقات
                    cnt_row = await conn.fetchrow("SELECT COUNT(*) as total FROM trades")
                    self.trade_counter = int(cnt_row['total'])

                    # استعادة الصفقات المفتوحة
                    open_rows = await conn.fetch("SELECT * FROM trades WHERE status = 'OPEN'")
                    for r in open_rows:
                        entry_p = float(r['entry_price'])
                        self.open_positions[r['symbol']] = {
                            'trade_id': r['trade_id'],
                            'entry_price': entry_p,
                            'qty': float(r['crypto_qty']),
                            'cost': float(r['cost_usd']),
                            'stop_loss': float(r['stop_loss']),
                            'entry_time': r['created_at'],
                            'entry_fee': float(r['entry_fee']),
                            'peak_price': entry_p,
                            'trailing_stage': 0  # 0: ابتدائي، 1: تأمين دخول، 2: قفل ربح، 3: تتبع مفتوح
                        }
                        self.cash_usdt -= float(r['cost_usd'])
                        logger.warning(f"🔄 [استعادة مركز مفتوح] {r['trade_id']} على {r['symbol']} مستمر في التتبع!")

                logger.info(f"📊 [Postgres] أرباح الشهر: ${self.monthly_realized_pnl:,.2f} | صفقات مستعادة: {len(self.open_positions)}")
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

    def check_period_resets(self):
        """تصفير الأرباح التراكمية اليومية والشهرية وإعادة تفعيل قاطع الدائرة"""
        now = datetime.now(timezone.utc)
        
        # تصفير شهري
        if now.month != self.current_month:
            logger.info(f"🔄 بداية شهر جديد ({now.strftime('%B')})! تصفير أرباح الشهر.")
            self.current_month = now.month
            self.monthly_realized_pnl = 0.0

        # تصفير يومي لقاطع الدائرة (Circuit Breaker)
        if now.day != self.current_day:
            self.current_day = now.day
            self.daily_realized_pnl = 0.0
            if self.circuit_breaker_triggered:
                self.circuit_breaker_triggered = False
                logger.info("🌅 بداية يوم تداول جديد: إعادة تفعيل التداول بعد إيقاف قاطع الدائرة.")
                asyncio.create_task(
                    self.send_telegram_alert(f"🌅 *[{self.system_name}] يوم جديد*: استئناف التداول وإعادة ضبط قاطع الدائرة اليومي.")
                )

    async def check_btc_shield(self) -> bool:
        """درع البيتكوين: التحقق من إيجابية الاتجاه العام على فريم 15m"""
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
        """تحليل الشموع على فريم 5 دقائق (5m) مع فلتر ATR لمنع الصفقات في الأسواق الراكدة"""
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe=self.timeframe, limit=45)
            df = pd.DataFrame(ohlcv, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df['c'] = df['c'].astype(float)
            df['h'] = df['h'].astype(float)
            df['l'] = df['l'].astype(float)
            df['v'] = df['v'].astype(float)

            # 1. حساب ATR (14) لقياس التقلب والسيولة
            tr1 = df['h'] - df['l']
            tr2 = (df['h'] - df['c'].shift()).abs()
            tr3 = (df['l'] - df['c'].shift()).abs()
            df['tr'] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
            df['atr'] = df['tr'].rolling(window=14).mean()
            current_atr_pct = df['atr'].iloc[-1] / df['c'].iloc[-1]

            # فلتر الركود: إذا كان تقلب الشموع أقل من 0.25% نرفض الدخول تماماً
            if current_atr_pct < self.min_atr_pct:
                return None, 0.0

            # 2. المتوسطات والمؤشرات على فريم 5 دقائق
            df['ema_fast'] = df['c'].ewm(span=5, adjust=False).mean()
            df['ema_slow'] = df['c'].ewm(span=13, adjust=False).mean()

            delta = df['c'].diff()
            gain = (delta.where(delta > 0, 0.0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(window=14).mean()
            rs = gain / (loss.replace(0, float('nan')))
            df['rsi'] = 100 - (100 / (1 + rs))

            df['vol_ma20'] = df['v'].rolling(window=20).mean()

            curr = df.iloc[-1]
            prev = df.iloc[-2]

            signal = None
            if (prev['ema_fast'] <= prev['ema_slow']) and (curr['ema_fast'] > curr['ema_slow']):
                if 45 <= curr['rsi'] <= 62:
                    if curr['v'] >= curr['vol_ma20']:
                        signal = 'BUY'

            return signal, float(curr['c'])
        except Exception as e:
            logger.error(f"خطأ مؤشرات {symbol}: {e}")
            return None, 0.0

    async def open_position(self, symbol: str, current_price: float):
        """فتح مركز جديد مع تهيئة الوقف المؤسسي (-0.60%)"""
        # التحقق من قاطع الدائرة اليومي (3%)
        max_allowed_daily_loss = self.initial_capital * self.daily_max_loss_pct
        if self.daily_realized_pnl <= -max_allowed_daily_loss:
            if not self.circuit_breaker_triggered:
                self.circuit_breaker_triggered = True
                msg = f"🛑 *[{self.system_name}] تفعيل قاطع الدائرة اليومي!*\nبلغت الخسارة اليومية `-${abs(self.daily_realized_pnl):.2f}` (تجاوزت 3%). تم تعليق التداول الجديد لليوم لحماية المحفظة."
                logger.warning(msg)
                await self.send_telegram_alert(msg)
            return

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

        # الوقف الابتدائي الصارم -0.60%
        initial_sl = current_price * (1 - self.base_stop_loss_pct)

        self.cash_usdt -= cost
        self.open_positions[symbol] = {
            'trade_id': trade_id,
            'entry_price': current_price,
            'qty': crypto_qty,
            'cost': cost,
            'stop_loss': initial_sl,
            'entry_time': now,
            'entry_fee': entry_fee,
            'peak_price': current_price,
            'trailing_stage': 0
        }

        if self.db_pool:
            try:
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO trades (
                            trade_id, system_name, symbol, status, entry_price, 
                            cost_usd, crypto_qty, entry_fee, take_profit, stop_loss, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    """, trade_id, self.system_name, symbol, 'OPEN', entry_price, 
                         cost, crypto_qty, entry_fee, 0.0, initial_sl, now)
            except Exception as e:
                logger.error(f"فشل إدراج الصفقة في DB: {e}")

        alert_msg = (
            f"🟢 *[{self.system_name}] صفقة جديدة ({self.timeframe})*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🆔 *رقم الصفقة:* `{trade_id}`\n"
            f"🪙 *الزوج:* `{symbol}`\n"
            f"💵 *سعر الدخول:* `${current_price:,.4f}`\n"
            f"📦 *حجم المركز:* `${cost:.2f}`\n"
            f"🛑 *وقف الخسارة الابتدائي:* `${initial_sl:,.4f}` (-{self.base_stop_loss_pct*100:.2f}%)\n"
            f"🛡️ *تأمين الدخول (BE):* عند `+{self.be_trigger_pct*100:.2f}%`\n"
            f"🎯 *نظام الأرباح:* الوقف المتحرك المتدرج + صعود مفتوح بدون سقف\n"
            f"💼 *الكاش المتبقي:* `${self.cash_usdt:,.2f}`\n"
            f"📊 *المراكز النشطة:* `{len(self.open_positions)}/{self.max_open_positions}`"
        )
        logger.info(f"🟢 [فتح مركز] {trade_id} على {symbol}")
        await self.send_telegram_alert(alert_msg)

    async def close_position(self, symbol: str, current_price: float, reason: str, is_maker: bool = False):
        """إغلاق المركز وتحديث الأرباح اليومية والشهرية وإرسال تقرير تيليجرام"""
        pos = self.open_positions.pop(symbol)
        gross_value = pos['qty'] * current_price
        exit_fee = gross_value * (self.maker_fee if is_maker else self.taker_fee)
        net_return = gross_value - exit_fee
        
        net_pnl = net_return - pos['cost']
        net_pnl_pct = (net_pnl / pos['cost']) * 100

        self.cash_usdt += net_return
        self.monthly_realized_pnl += net_pnl
        self.daily_realized_pnl += net_pnl
        now = datetime.now(timezone.utc)
        duration_sec = int((now - pos['entry_time']).total_seconds())
        duration_min = duration_sec / 60

        if net_pnl < 0:
            self.cooldown_tracker[symbol] = now + timedelta(minutes=15)

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
            f"📈 *أرباح اليوم:* `{'+$' if self.daily_realized_pnl >= 0 else '-$'}{abs(self.daily_realized_pnl):.2f}`\n"
            f"📊 *صافي شهر {month_name}:* `{'+$' if self.monthly_realized_pnl >= 0 else '-$'}{abs(self.monthly_realized_pnl):.2f}`\n"
            f"💼 *رصيد الكاش:* `${self.cash_usdt:,.2f}`"
        )
        logger.info(f"🔒 [إغلاق مركز] {pos['trade_id']} | صافي: ${net_pnl:+.2f}")
        await self.send_telegram_alert(alert_msg)

    async def monitor_open_positions(self):
        """مراقبة المراكز وتطبيق الوقف المتحرك المتدرج وقفل الأرباح وملاحقة القمم المفتوحة"""
        now = datetime.now(timezone.utc)
        total_open_value = 0.0

        for symbol in list(self.open_positions.keys()):
            pos = self.open_positions[symbol]
            ticker = await self.exchange.fetch_ticker(symbol)
            current_price = float(ticker['last'])
            
            total_open_value += (pos['qty'] * current_price)
            duration_minutes = (now - pos['entry_time']).total_seconds() / 60
            pnl_pct = (current_price - pos['entry_price']) / pos['entry_price']

            # تحديث أعلى قمة سجلها السعر
            if current_price > pos['peak_price']:
                pos['peak_price'] = current_price

            # -------------------------------------------------------------
            # خطة الوقف المتحرك المتدرج لحجز المكاسب وملاحقة الصعود الصاروخي
            # -------------------------------------------------------------
            
            # المرحلة 1: تأمين الدخول (Break-Even) فور بلوغ +0.40% ربح
            if pos['trailing_stage'] < 1 and pnl_pct >= self.be_trigger_pct:
                pos['trailing_stage'] = 1
                be_price = pos['entry_price'] * (1 + self.taker_fee)  # تغطية عمولة الدخول
                if be_price > pos['stop_loss']:
                    pos['stop_loss'] = be_price
                logger.info(f"🛡️ [Break-Even] تم تأمين صفقة {symbol} عند سعر الدخول!")

            # المرحلة 2: قفل ربح مضمون (+0.45%) فور وصول السعر إلى +0.80%
            if pos['trailing_stage'] < 2 and pnl_pct >= self.lock_step1_trigger_pct:
                pos['trailing_stage'] = 2
                lock_price = pos['entry_price'] * (1 + self.lock_step1_profit_pct)
                if lock_price > pos['stop_loss']:
                    pos['stop_loss'] = lock_price
                logger.info(f"🔒 [Lock Profit] تم قفل ربح مضمون (+{self.lock_step1_profit_pct*100:.2f}%) على {symbol}!")

            # المرحلة 3: تفعيل التتبع الصاروخي المفتوح عند تجاوز +1.20%
            if pnl_pct >= self.trail_activation_pct:
                if pos['trailing_stage'] < 3:
                    pos['trailing_stage'] = 3
                    logger.info(f"🚀 [Uncapped Ride] تم تفعيل التتبع الصاروخي على {symbol}! لا سقف للأرباح.")
                
                # الوقف يتبع القمة بمسافة 0.30% فقط لحجز أعلى ربح ممكن
                dynamic_trail_sl = pos['peak_price'] * (1 - self.trail_callback_pct)
                if dynamic_trail_sl > pos['stop_loss']:
                    pos['stop_loss'] = dynamic_trail_sl

            # -------------------------------------------------------------
            # شروط الخروج والتنفيذ
            # -------------------------------------------------------------

            # 1. ضرب خط الوقف المتحرك أو وقف الخسارة
            if current_price <= pos['stop_loss']:
                if pos['trailing_stage'] == 3:
                    peak_pct = (pos['peak_price'] - pos['entry_price']) / pos['entry_price'] * 100
                    reason = f"Trailing Peak Captured (قمة: +{peak_pct:.2f}% | خروج: +{pnl_pct*100:.2f}%)"
                    await self.close_position(symbol, current_price, reason, is_maker=True)
                elif pos['trailing_stage'] == 2:
                    reason = f"Guaranteed Profit Locked (+{pnl_pct*100:.2f}%)"
                    await self.close_position(symbol, current_price, reason, is_maker=True)
                elif pos['trailing_stage'] == 1:
                    reason = "Break-Even Protected (0% Loss)"
                    await self.close_position(symbol, current_price, reason, is_maker=False)
                else:
                    reason = f"Initial Stop Loss (-{self.base_stop_loss_pct*100:.2f}%)"
                    await self.close_position(symbol, current_price, reason, is_maker=False)
                continue

            # 2. السقف الزمني الحتمي (60 دقيقة لفريم 5m) لتحرير رأس المال إذا تجمد السعر
            if duration_minutes >= self.hard_time_cap_minutes:
                await self.close_position(
                    symbol, 
                    current_price, 
                    f"Hard Time Cap ({self.hard_time_cap_minutes}m Timeout)", 
                    is_maker=False
                )
                continue

        return total_open_value

    def display_dashboard(self, total_mtm_equity: float):
        """لوحة المتابعة الشفافة في السجلات"""
        allocated_cap = sum(p['cost'] for p in self.open_positions.values())
        headers = ["المعيار", "القيمة"]
        rows = [
            ["النظام", self.system_name],
            ["الإطار الزمني", self.timeframe],
            ["الكاش المتاح", f"${self.cash_usdt:,.2f}"],
            ["رأس المال المحجوز بالصفقات", f"${allocated_cap:,.2f} / ${self.max_total_capital:,.2f}"],
            ["إجمالي حقوق الملكية (MTM)", f"${total_mtm_equity:,.2f}"],
            ["أرباح اليوم", f"${self.daily_realized_pnl:,.2f}"],
            ["أرباح الشهر التراكمية (DB)", f"${self.monthly_realized_pnl:,.2f}"],
            ["المراكز المفتوحة النشطة", f"{len(self.open_positions)} / {self.max_open_positions}"],
            ["إجمالي الصفقات المنفذة", f"{self.trade_counter}"],
            ["حالة قاطع الدائرة اليومي (3%)", "🚨 متوقف مؤقتاً" if self.circuit_breaker_triggered else "✅ نشط وآمن"]
        ]
        print("\n" + tabulate(rows, headers=headers, tablefmt="fancy_grid"))

    async def run(self):
        await self.start_dummy_server()
        await self.init_database()
        await self.auto_heal()
        logger.info(f"🚀 تم تشغيل {self.system_name} بالنموذج المؤسسي المطور بنجاح.")
        
        mode_str = "تجريبي (Paper Trading)" if self.paper_trading else "🔴 حقيقي (Live Money)"
        await self.send_telegram_alert(
            f"🚀 *[{self.system_name}] تشغيل المحرك المؤسسي المطور*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📊 *الفريم الزمني:* `{self.timeframe}`\n"
            f"🛑 *وقف الخسارة الابتدائي:* `-{self.base_stop_loss_pct*100:.2f}%`\n"
            f"🛡️ *تأمين الدخول (BE):* عند `+{self.be_trigger_pct*100:.2f}%`\n"
            f"🔒 *قفل الأرباح الأول:* عند `+{self.lock_step1_profit_pct*100:.2f}%`\n"
            f"🚀 *صعود مفتوح (No Cap):* تتبع فوق `+{self.trail_activation_pct*100:.2f}%`\n"
            f"⚡ *قاطع الدائرة اليومي:* حماية عند خسارة 3%\n"
            f"💼 *الكاش الحالي:* `${self.cash_usdt:,.2f}`"
        )
        
        cycle_count = 0
        while True:
            try:
                self.check_period_resets()
                btc_safe = await self.check_btc_shield()
                open_mtm_value = await self.monitor_open_positions()
                total_equity = self.cash_usdt + open_mtm_value

                # الدخول فقط إذا كان درع BTC إيجابي ولم يتفعل قاطع الدائرة
                if btc_safe and not self.circuit_breaker_triggered and len(self.open_positions) < self.max_open_positions:
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
