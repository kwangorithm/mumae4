# ==========================================================
# [main.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================

import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import glob
import random
import json
import pandas_market_calendars as mcal
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from dotenv import load_dotenv

from config import ConfigManager
from broker import KoreaInvestmentBroker
from strategy import InfiniteStrategy
from telegram_bot import TelegramController

if not os.path.exists('data'):
    os.makedirs('data')
if not os.path.exists('logs'):
    os.makedirs('logs')

load_dotenv() 

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
try:
    ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID")) if os.getenv("ADMIN_CHAT_ID") else None
except ValueError:
    ADMIN_CHAT_ID = None

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
CANO = os.getenv("CANO")
ACNT_PRDT_CD = os.getenv("ACNT_PRDT_CD", "01")

if not all([TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO]):
    print("❌ [치명적 오류] .env 파일에 봇 구동 필수 키(TELEGRAM_TOKEN, APP_KEY, APP_SECRET, CANO)가 누락되었습니다. 봇을 종료합니다.")
    exit(1)

log_filename = f"logs/bot_app_{datetime.datetime.now().strftime('%Y%m%d')}.log"
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def is_dst_active():
    est = pytz.timezone('US/Eastern')
    return datetime.datetime.now(est).dst() != datetime.timedelta(0)

def get_target_hour():
    return (17, "🌞 서머타임 적용(여름)") if is_dst_active() else (18, "❄️ 서머타임 해제(겨울)")

def is_market_open():
    try:
        est = pytz.timezone('US/Eastern')
        today = datetime.datetime.now(est)
        if today.weekday() >= 5: 
            return False
            
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=today.date(), end_date=today.date())
        
        if not schedule.empty:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"⚠️ 달력 라이브러 에러 발생. 평일이므로 강제 개장 처리합니다: {e}")
        return True

# 💡 [핵심 수술] 가속모드 삭제에 따른 예산할당 엔진 파라미터 간소화
def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    rem_cash = cash
    
    for tx in sorted_tickers:
        rev_state = cfg.get_reverse_state(tx)
        is_rev = rev_state.get("is_active", False)
        
        if is_rev:
            portion = 0.0
        else:
            split = cfg.get_split_count(tx)
            portion = cfg.get_seed(tx) / split if split > 0 else 0
            
        if rem_cash >= portion:
            allocated[tx] = rem_cash
            rem_cash -= portion
        else: 
            allocated[tx] = 0
                
    return sorted_tickers, allocated

def get_actual_execution_price(execs, target_qty, side_cd):
    if not execs: return 0.0
    
    execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
    matched_qty = 0
    total_amt = 0.0
    for ex in execs:
        if ex.get('sll_buy_dvsn_cd') == side_cd: 
            eqty = int(float(ex.get('ft_ccld_qty', '0')))
            eprice = float(ex.get('ft_ccld_unpr3', '0'))
            if matched_qty + eqty <= target_qty:
                total_amt += eqty * eprice
                matched_qty += eqty
            elif matched_qty < target_qty:
                rem = target_qty - matched_qty
                total_amt += rem * eprice
                matched_qty += rem
            
            if matched_qty >= target_qty:
                break
    
    if matched_qty > 0:
        return math.floor((total_amt / matched_qty) * 100) / 100.0
    return 0.0

def perform_self_cleaning():
    try:
        now = time.time()
        seven_days = 7 * 24 * 3600
        one_day = 24 * 3600
        
        for f in glob.glob("logs/*.log"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
                
        for f in glob.glob("data/*.bak_*"):
            if os.path.isfile(f) and os.stat(f).st_mtime < now - seven_days:
                try: os.remove(f)
                except: pass
                
        for directory in ["data", "logs"]:
            for f in glob.glob(f"{directory}/tmp*"):
                if os.path.isfile(f) and os.stat(f).st_mtime < now - one_day:
                    try: os.remove(f)
                    except: pass
    except Exception as e:
        logging.error(f"🧹 자정(Self-Cleaning) 작업 중 오류 발생: {e}")

async def scheduled_self_cleaning(context):
    await asyncio.to_thread(perform_self_cleaning)
    logging.info("🧹 [시스템 자정 작업 완료] 7일 초과 로그/백업 및 24시간 초과 임시 파일 소각 완료")

async def scheduled_token_check(context):
    jitter_seconds = random.randint(0, 180)
    logging.info(f"🔑 [API 토큰 갱신] 서버 동시 접속 부하 방지를 위해 {jitter_seconds}초 대기 후 발급을 시작합니다.")
    await asyncio.sleep(jitter_seconds)
    
    await asyncio.to_thread(context.job.data['broker']._get_access_token, force=True)
    logging.info("🔑 [API 토큰 갱신] 토큰 갱신이 안전하게 완료되었습니다.")

async def scheduled_force_reset(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    
    now_minutes = now.hour * 60 + now.minute
    target_minutes = target_hour * 60
    
    if abs(now_minutes - target_minutes) > 2 and abs(now_minutes - target_minutes) < (24*60 - 2):
        return
        
    if not is_market_open():
        await context.bot.send_message(chat_id=context.job.chat_id, text="⛔ <b>오늘은 미국 증시 휴장일입니다. 금일 시스템 매매 잠금 해제 및 정규장 주문 스케줄을 모두 건너뜁니다.</b>", parse_mode='HTML')
        return
    
    try:
        app_data = context.job.data
        cfg = app_data['cfg']
        broker = app_data['broker']
        tx_lock = app_data['tx_lock']
        chat_id = context.job.chat_id
        
        cfg.reset_locks()
        
        async with tx_lock:
            _, holdings = broker.get_account_balance()
            
        if holdings is None:
            holdings = {}
            
        msg_addons = ""
        
        for t in cfg.get_active_tickers():
            rev_state = cfg.get_reverse_state(t)
            
            if rev_state.get("is_active"):
                actual_avg = float(holdings.get(t, {'avg': 0})['avg'])
                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                
                if curr_p > 0 and actual_avg > 0:
                    curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                    exit_target = rev_state.get("exit_target", 0.0)
                    
                    if curr_ret >= exit_target:
                        cfg.set_reverse_state(t, False, 0, 0.0)
                        cfg.clear_escrow_cash(t)
                        
                        ledger_data = cfg.get_ledger()
                        changed = False
                        for lr in ledger_data:
                            if lr.get('ticker') == t and lr.get('is_reverse', False):
                                lr['is_reverse'] = False
                                changed = True
                        if changed:
                            cfg._save_json(cfg.FILES["LEDGER"], ledger_data)
                            
                        msg_addons += f"\n🌤️ <b>[{t}] 리버스 목표 달성({curr_ret:.2f}%)!</b> 격리 병동 졸업 및 Escrow 해제 완료!"
                    else:
                        cfg.increment_reverse_day(t)
                else:
                    cfg.increment_reverse_day(t)
            else:
                cfg.increment_reverse_day(t)
                
        final_msg = f"🔓 <b>[{target_hour}:00] 시스템 초기화 완료 (매매 잠금 해제 & 변동성 락온)</b>" + msg_addons
        await context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML')
        
    except Exception as e:
        await context.bot.send_message(chat_id=context.job.chat_id, text=f"🚨 <b>시스템 초기화 중 에러 발생:</b> {e}", parse_mode='HTML')

async def scheduled_sniper_monitor(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    try:
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=now_est.date(), end_date=now_est.date())
        if schedule.empty: return
        
        market_open = schedule.iloc[0]['market_open'].astimezone(est)
        market_close = schedule.iloc[0]['market_close'].astimezone(est)
    except Exception:
        if now_est.weekday() < 5:
            market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
            market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
        else: return
    
    pre_start = market_open.replace(hour=4, minute=0)
    start_monitor = pre_start + datetime.timedelta(minutes=1)
    end_monitor = market_close - datetime.timedelta(minutes=15)
    
    if not (start_monitor <= now_est <= end_monitor):
        return

    is_regular_session = market_open <= now_est <= market_close

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    target_cache = app_data.setdefault('dynamic_targets', {})
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    
    today_est_str = now_est.strftime('%Y%m%d')
    saved_date = target_cache.get('date')
    
    if saved_date != today_est_str:
        target_cache.clear()
        target_cache['date'] = today_est_str
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        
        if saved_date is not None:
            try:
                for _f in glob.glob("data/sniper_cache_*.json"):
                    os.remove(_f)
            except: pass
    
    async def _do_sniper():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None: return
            
            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            for t in cfg.get_active_tickers():
                version = cfg.get_version(t)
                is_upward_sniper_on = cfg.get_upward_sniper_mode()
                
                # 💡 [핵심 수술] V17 시크릿 모드이거나 일반 사용자가 상방 스나이퍼를 ON으로 켰을 때만 작동!
                if version != "V17" and not is_upward_sniper_on:
                    continue
                
                lock_buy = cfg.check_lock(t, "SNIPER_BUY")
                lock_sell = cfg.check_lock(t, "SNIPER_SELL")
                
                if lock_buy and lock_sell:
                    continue
                
                h = holdings.get(t, {'qty': 0, 'avg': 0})
                qty = int(h['qty'])
                avg_price = float(h['avg'])
                if qty == 0: continue
                
                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                prev_c = await asyncio.to_thread(broker.get_previous_close, t)
                if curr_p <= 0: continue
                
                # 💡 [V22.12 패치] 당일 고가 기억 상실(Amnesia) 완벽 방어 (Self-Healing)
                actual_day_high, _ = await asyncio.to_thread(broker.get_day_high_low, t)
                
                tracking_info = tracking_cache.setdefault(t, {
                    'is_tracking': False, 'lowest_price': float('inf'), 'day_high': 0.0, 'armed_price': 0.0, 'alerted': False,
                    'is_trailing': False, 'peak_price': 0.0, 'trailing_armed': False, 'trigger_price': 0.0
                })
                
                # 봇 재시작 등으로 캐시가 증발했을 때, 실제 고가로 뇌 메모리 강제 복구
                if actual_day_high > tracking_info['day_high']:
                    tracking_info['day_high'] = actual_day_high
                
                # 💡 [하방 스나이퍼 구역] V17 시크릿 모드 전용
                if version == "V17":
                    idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
                    current_weight = cfg.get_sniper_multiplier(t)
                    cached_data = target_cache.get(t)
                    
                    if cached_data is None or cached_data.get('weight') != current_weight:
                        tgt = await asyncio.to_thread(broker.get_dynamic_sniper_target, idx_ticker)
                        if tgt is not None:
                            target_cache[t] = {'value': float(tgt), 'weight': current_weight}
                        else:
                            target_cache[t] = {'value': (7.59 if t=="SOXL" else 6.18), 'weight': current_weight}

                    sniper_pct = target_cache[t]['value']
                    
                    if not lock_buy:
                        candle = await asyncio.to_thread(broker.get_current_5min_candle, t)
                        
                        if candle:
                            c_open, c_high, c_low, c_close = candle['open'], candle['high'], candle['low'], candle['close']
                            c_vol, c_vol_ma20 = candle['volume'], candle['vol_ma20']
                            
                            if c_high > tracking_info['day_high']:
                                tracking_info['day_high'] = c_high
                                
                            current_drop = (tracking_info['day_high'] - c_low) / tracking_info['day_high'] if tracking_info['day_high'] > 0 else 0
                            
                            if not tracking_info['is_tracking'] and current_drop >= (abs(sniper_pct) / 100.0):
                                tracking_info['is_tracking'] = True
                                tracking_info['armed_price'] = tracking_info['day_high'] * (1 - (abs(sniper_pct) / 100.0))
                                tracking_info['lowest_price'] = c_low
                                
                                if not tracking_info['alerted']:
                                    msg = f"🎯 <b>[{t}] 하방 매수 스나이퍼 안전장치 해제 (Armed)!</b>\n"
                                    msg += f"📈 <b>장중 고점: ${tracking_info['day_high']:.2f}</b>\n"
                                    msg += f"📉 <b>에너지 소진: -{abs(sniper_pct):.2f}% (방어선 ${tracking_info['armed_price']:.2f} 돌파)</b>\n"
                                    msg += f"👀 진짜 바닥을 다질 때까지 발목을 노리며 추적을 시작합니다."
                                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                    tracking_info['alerted'] = True
                                    
                            if tracking_info['is_tracking']:
                                if c_low < tracking_info['lowest_price']:
                                    tracking_info['lowest_price'] = c_low
                                    
                                trigger_pct = 1.5 if t == "SOXL" else 1.0
                                is_yangbong = c_close > c_open
                                rebound_pct = (c_close - tracking_info['lowest_price']) / tracking_info['lowest_price'] * 100 if tracking_info['lowest_price'] > 0 else 0
                                is_rebounded = rebound_pct >= trigger_pct
                                
                                if is_regular_session:
                                    is_volume_spike = c_vol > c_vol_ma20
                                else:
                                    is_volume_spike = True
                                
                                if is_yangbong and is_rebounded and is_volume_spike:
                                    if cfg.get_secret_mode():
                                        
                                        ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                                        temp_plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
                                        sniper_budget = temp_plan.get('one_portion', 0.0)
                                        
                                        exec_price = min(c_close, tracking_info['armed_price'])
                                        
                                        if sniper_budget >= exec_price and exec_price > 0:
                                            await asyncio.to_thread(broker.cancel_targeted_orders, t, "BUY", "34")
                                            await asyncio.sleep(1.0)
                                            
                                            is_rev = temp_plan.get('is_reverse', False)
                                            if not is_rev and exec_price > avg_price:
                                                sniper_budget = sniper_budget * 0.5
                                                
                                            rem_qty = math.floor(sniper_budget / exec_price)
                                            hunt_success = False
                                            actual_buy_price = exec_price
                                            
                                            for attempt in range(3):
                                                if rem_qty <= 0:
                                                    hunt_success = True
                                                    break

                                                ask_price = await asyncio.to_thread(broker.get_ask_price, t)
                                                safe_ask_price = ask_price if ask_price > 0 else c_close
                                                
                                                final_exec_price = min(safe_ask_price, tracking_info['armed_price'])
                                                
                                                res = broker.send_order(t, "BUY", rem_qty, final_exec_price, "LIMIT")
                                                odno = res.get('odno', '')
                                                
                                                if res.get('rt_cd') == '0' and odno:
                                                    order_found_in_unfilled = False
                                                    ccld_qty = 0
                                                    
                                                    for _ in range(4): # 2.0초 x 4 = 8초 대기 다중 교차 검증
                                                        await asyncio.sleep(2.0)
                                                        unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                                        my_order = next((o for o in unfilled if o.get('odno') == odno), None)
                                                        
                                                        if my_order:
                                                            order_found_in_unfilled = True
                                                            ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                                            break
                                                            
                                                        execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                                        my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                                        if my_execs:
                                                            ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                                            if ccld_qty >= rem_qty:
                                                                break
                                                                
                                                    if ccld_qty >= rem_qty:
                                                        rem_qty = 0
                                                        hunt_success = True
                                                        actual_buy_price = final_exec_price
                                                        break
                                                    else:
                                                        if order_found_in_unfilled and my_order:
                                                            ord_qty = int(float(my_order.get('ord_qty', 0)))
                                                            rem_qty = ord_qty - ccld_qty
                                                        else:
                                                            rem_qty -= ccld_qty
                                                            
                                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                                        await asyncio.sleep(1.0)
                                                
                                                await asyncio.sleep(0.5)
                                            
                                            if hunt_success:
                                                cfg.set_lock(t, "SNIPER_BUY") 
                                                tracking_info['hit_price'] = actual_buy_price
                                                tracking_info['is_tracking'] = False
                                                
                                                try:
                                                    with open(f"data/sniper_cache_{t}.json", "w") as f:
                                                        json.dump({"hit_price": actual_buy_price, "lowest_price": tracking_info['lowest_price']}, f)
                                                except: pass
                                                
                                                session_str = "🔥 정규장" if is_regular_session else "🌅 프리/애프터마켓"
                                                vol_str = f"기관 매수세 감지 (현재 {c_vol:,.0f} > MA20 {c_vol_ma20:,.0f})" if is_regular_session else "거래량 필터 면제 (정규장 외 세션)"
                                                
                                                msg = f"💥 <b>[{t}] 하방(매수) 스나이퍼 명중! ({session_str})</b>\n"
                                                msg += f"📉 <b>최저점: ${tracking_info['lowest_price']:.2f}</b>\n"
                                                msg += f"📈 <b>반등 양봉 포착 (+{trigger_pct}% 돌파)</b>\n"
                                                msg += f"🔥 <b>{vol_str}</b>\n"
                                                msg += f"🎯 <b>상한선(${tracking_info['armed_price']:.2f}) 방어막 내에서 최적 단가 ${actual_buy_price:.2f}에 즉시 체결을 완료</b>했습니다!\n"
                                                msg += "🔫 당일 하방 스나이퍼 활동만을 종료하며, 상방(익절) 감시는 계속됩니다."
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                continue

                                            now_ts = time.time()
                                            fail_history = app_data.setdefault('sniper_fail_ts', {})
                                            if now_ts - fail_history.get(t, 0) > 60:
                                                msg = f"🛡️ <b>[{t}] 능동형 스나이퍼 기습 실패 (1분 쿨타임 진입)</b>\n"
                                                msg += f"📉 반등 타점(${exec_price:.2f})에 3회 지정가 덫을 던졌으나 잔량이 남았습니다.\n"
                                                msg += f"🦇 매수 스나이퍼는 1분 동안 숨을 죽이며, 취소했던 방어 매수(LOC) 주문만 호가창에 정밀 복구합니다."
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                fail_history[t] = now_ts
                                            
                                            for o in temp_plan.get('core_orders', []) + temp_plan.get('bonus_orders', []):
                                                if o['side'] == 'BUY':
                                                    broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                                    await asyncio.sleep(0.2)
                                                    
                                            continue

                # 💡 [상방 스나이퍼 구역] V17 시크릿 모드 및 일반 사용자 상방 스나이퍼 모드 공용 적용
                if target_pct_val := cfg.get_target_profit(t):
                    target_price = math.ceil(avg_price * (1 + target_pct_val / 100.0) * 100) / 100.0
                    
                    split = cfg.get_split_count(t)
                    t_val, _ = cfg.get_absolute_t_val(t, qty, avg_price)
                    
                    depreciation_factor = 2.0 / split if split > 0 else 0.1
                    star_ratio = (target_pct_val / 100.0) - ((target_pct_val / 100.0) * depreciation_factor * t_val)
                    star_price = math.ceil(avg_price * (1 + star_ratio) * 100) / 100.0
                    
                    if not lock_sell and curr_p >= target_price:
                        await asyncio.to_thread(broker.cancel_all_orders_safe, t, side="SELL")
                        await asyncio.sleep(1.0)
                        
                        rem_qty = qty
                        hunt_success = False
                        actual_sell_price = target_price
                        
                        for attempt in range(3):
                            if rem_qty <= 0:
                                hunt_success = True
                                break
                                
                            bid_price = await asyncio.to_thread(broker.get_bid_price, t)
                            
                            if bid_price > 0 and bid_price >= target_price:
                                res = broker.send_order(t, "SELL", rem_qty, bid_price, "LIMIT")
                                odno = res.get('odno', '')
                                
                                if res.get('rt_cd') == '0' and odno:
                                    order_found_in_unfilled = False
                                    ccld_qty = 0
                                    
                                    for _ in range(4): # 2.0초 x 4 = 8초 대기 다중 교차 검증
                                        await asyncio.sleep(2.0)
                                        unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                        my_order = next((o for o in unfilled_check if o.get('odno') == odno), None)
                                        
                                        if my_order:
                                            order_found_in_unfilled = True
                                            ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                            break
                                            
                                        execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                        my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                        if my_execs:
                                            ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                            if ccld_qty >= rem_qty:
                                                break
                                                
                                    if ccld_qty >= rem_qty:
                                        rem_qty = 0
                                        hunt_success = True
                                        actual_sell_price = bid_price
                                        break
                                    else:
                                        if order_found_in_unfilled and my_order:
                                            ord_qty = int(float(my_order.get('ord_qty', 0)))
                                            rem_qty = ord_qty - ccld_qty
                                        else:
                                            rem_qty -= ccld_qty
                                            
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(1.0)
                                        
                            await asyncio.sleep(0.5)
                            
                        if hunt_success:
                            cfg.set_lock(t, "SNIPER_SELL")
                            msg = f"🎉 <b>[{t}] 12% 잭팟 대원칙 달성! (전량 강제 익절)</b>\n\n"
                            msg += f"▫️ 12% 목표가(${target_price:.2f}) 돌파를 확인했습니다.\n"
                            msg += f"▫️ 대기 중인 모든 쿼터/LOC 방어선을 즉시 해제합니다.\n"
                            msg += f"▫️ 잔여 보유 수량 전량({qty}주)에 대해 지정가 익절을 집행하여 단가 ${actual_sell_price:.2f}에 100% 수익을 확정했습니다. 🚀\n"
                            msg += "\n🔫 당일 상방(매도) 스나이퍼 활동을 완전 종료합니다."
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            continue
                            
                        now_ts = time.time()
                        fail_history_j = app_data.setdefault('sniper_j_fail_ts', {})
                        if now_ts - fail_history_j.get(t, 0) > 60:
                            msg = f"🛡️ <b>[{t}] 스나이퍼 잭팟 기습 실패 (방어선 복구)</b>\n"
                            msg += f"🎯 3회에 걸쳐 전량 익절을 시도했으나 체결되지 않았습니다.\n"
                            msg += f"🦇 취소했던 원래의 매도(SELL) 주문을 다시 호가창에 정밀 장전합니다. (1분 쿨타임 적용)"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            fail_history_j[t] = now_ts
                        
                        ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                        plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
                        
                        for o in plan.get('core_orders', []) + plan.get('bonus_orders', []):
                            if o['side'] == 'SELL':
                                broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                await asyncio.sleep(0.2)
                                
                        continue
                
                is_rev = cfg.get_reverse_state(t).get("is_active", False)
                sell_divisor = 10 if split <= 20 else 20
                
                safe_floor_price = math.ceil(avg_price * 1.005 * 100) / 100.0
                
                if is_rev:
                    q_qty = max(4, math.floor(qty / sell_divisor)) if qty >= 4 else qty
                    base_trigger = safe_floor_price
                else:
                    is_first_half = t_val < (split / 2)
                    q_qty = math.ceil(qty / 4)
                    base_trigger = max(star_price, safe_floor_price)
                
                if not lock_sell and curr_p >= base_trigger:
                    if not tracking_info['trailing_armed']:
                        tracking_info['trailing_armed'] = True
                        tracking_info['is_trailing'] = True
                        tracking_info['peak_price'] = curr_p
                        tracking_info['trigger_price'] = base_trigger
                        
                        msg = f"🦇 <b>[{t}] 상방 트레일링 스나이퍼 락온 (Armed)!</b>\n\n"
                        msg += f"▫️ 현재가(${curr_p:.2f})가 쿼터 기준선(${base_trigger:.2f})을 돌파했습니다.\n"
                        msg += f"▫️ 기존 LOC 덫은 유지한 채, 당일 최고가(Day High)를 실시간 갱신합니다.\n"
                        msg += f"📈 이익 극대화를 위한 고점 스캔을 시작합니다!"
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                
                if not lock_sell and tracking_info['trailing_armed']:
                    if curr_p > tracking_info['peak_price']:
                        tracking_info['peak_price'] = curr_p
                        
                    trailing_drop = 1.5 if t == "SOXL" else 1.0
                    drop_trigger = tracking_info['peak_price'] * (1 - (trailing_drop / 100.0))
                    
                    if curr_p <= drop_trigger:
                        intercept_price = max(drop_trigger, base_trigger)
                        intercept_price = math.floor(intercept_price * 100) / 100.0
                        
                        unfilled = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                        target_odno = None
                        
                        for o in unfilled:
                            if o.get('sll_buy_dvsn_cd') == '01':
                                dvsn = o.get('ord_dvsn_cd') or o.get('ord_dvsn') or ''
                                o_price = float(o.get('ft_ord_unpr3', 0))
                                
                                if dvsn == '34' or o_price == star_price: 
                                    target_odno = o.get('odno')
                                    break
                                
                        if target_odno:
                            await asyncio.to_thread(broker.cancel_order, t, target_odno)
                            await asyncio.sleep(1.0) 
                        else:
                            now_ts = time.time()
                            fail_history_sync = app_data.setdefault('sniper_sync_fail_ts', {})
                            if now_ts - fail_history_sync.get(t, 0) > 300:
                                msg = f"⚠️ <b>[{t}] 상방 스나이퍼 가로채기 대기 (이중 체결 방지)</b>\n"
                                msg += f"가격(${curr_p:.2f})이 고점 대비 {trailing_drop}% 꺾였으나, 한투 서버에서 기존 방어선(LOC)을 찾지 못했습니다.\n"
                                msg += "중복 매도를 막기 위해 5분 뒤 다시 식별을 시도합니다."
                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                fail_history_sync[t] = now_ts
                            continue
                            
                        rem_qty = q_qty
                        hunt_success = False
                        actual_sell_price = intercept_price
                        
                        for attempt in range(3):
                            if rem_qty <= 0:
                                hunt_success = True
                                break
                                
                            bid_price = await asyncio.to_thread(broker.get_bid_price, t)
                            
                            if bid_price > 0 and bid_price >= intercept_price:
                                res = broker.send_order(t, "SELL", rem_qty, bid_price, "LIMIT")
                                odno = res.get('odno', '')
                                
                                if res.get('rt_cd') == '0' and odno:
                                    order_found_in_unfilled = False
                                    ccld_qty = 0
                                    
                                    for _ in range(4): # 2.0초 x 4 = 8초 대기 다중 교차 검증
                                        await asyncio.sleep(2.0)
                                        unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                        my_order = next((o for o in unfilled_check if o.get('odno') == odno), None)
                                        
                                        if my_order:
                                            order_found_in_unfilled = True
                                            ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                            break
                                            
                                        execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                        my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                        if my_execs:
                                            ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                            if ccld_qty >= rem_qty:
                                                break
                                                
                                    if ccld_qty >= rem_qty:
                                        rem_qty = 0
                                        hunt_success = True
                                        actual_sell_price = bid_price
                                        break
                                    else:
                                        if order_found_in_unfilled and my_order:
                                            ord_qty = int(float(my_order.get('ord_qty', 0)))
                                            rem_qty = ord_qty - ccld_qty
                                        else:
                                            rem_qty -= ccld_qty
                                            
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(1.0)
                                        
                            await asyncio.sleep(0.5)
                                
                        if hunt_success:
                            cfg.set_lock(t, "SNIPER_SELL")
                            tracking_info['is_trailing'] = False
                            
                            msg = f"💥 <b>[{t}] 상방 쿼터 스나이퍼 명중! (기습 익절 완료)</b>\n\n"
                            msg += f"▫️ 최고점 갱신: ${tracking_info['peak_price']:.2f}\n"
                            msg += f"▫️ 하락폭 감지: 최고점 대비 -{trailing_drop}% 꺾임\n\n"
                            msg += f"🛡️ 익절은 무조건 정답이다!\n"
                            msg += f"기본 방어선(${base_trigger:.2f})이 뚫리기 전, 기존 LOC 주문을 취소하고 지정가 ${actual_sell_price:.2f}에 {q_qty}주를 가로채어 수익을 강제 확정했습니다. 🎯\n"
                            
                            if not is_rev:
                                if t_val < (split / 2):
                                    msg += "\n🛡️ <b>[공수 완벽 분리]</b> 전반전 기존 0.5회분 강제 매수(LOC) 덫은 유지하여 눈덩이를 계속 키웁니다."
                                else:
                                    msg += "\n🛡️ <b>[공수 완벽 분리]</b> 후반전 하방 줍줍 덫은 그대로 유지합니다."
                            
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            continue
                            
                        now_ts = time.time()
                        fail_history_q = app_data.setdefault('sniper_q_fail_ts', {})
                        if now_ts - fail_history_q.get(t, 0) > 60:
                            msg = f"🛡️ <b>[{t}] 스나이퍼 쿼터 가로채기 실패 (방어선 복구)</b>\n"
                            msg += f"🎯 지정가 기습 매도를 시도했으나 잔량이 남았습니다.\n"
                            msg += f"🦇 취소했던 쿼터 방어선(LOC 매도)을 호가창에 정밀 복구합니다. (1분 쿨타임 적용)"
                            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                            fail_history_q[t] = now_ts
                        
                        ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                        plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
                        
                        for o in plan.get('core_orders', []):
                            if o['side'] == 'SELL' and o['type'] == 'LOC':
                                broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                                await asyncio.sleep(0.2)
                                
                        continue
    
    try:
        await asyncio.wait_for(_do_sniper(), timeout=45.0)
    except asyncio.TimeoutError:
        app_data = context.job.data
        fail_history_t = app_data.setdefault('sniper_timeout_ts', 0)
        now_ts = time.time()
        if now_ts - fail_history_t > 1800: 
            await context.bot.send_message(chat_id=context.job.chat_id, text="🚨 <b>[긴급] 야후 파이낸스 등 외부 API 응답 지연으로 스나이퍼 감시가 무한 대기 상태에 빠졌습니다. \n\n1회 건너뛰고 통제권(Lock)을 강제 반환하여 정규장 로직을 보호합니다!</b>", parse_mode='HTML')
            app_data['sniper_timeout_ts'] = now_ts
    except Exception as e:
        logging.error(f"🚨 스나이퍼 모니터 에러: {e}")

async def scheduled_regular_trade(context):
    kst = pytz.timezone('Asia/Seoul')
    now = datetime.datetime.now(kst)
    target_hour, _ = get_target_hour()
    chat_id = context.job.chat_id
    
    now_minutes = now.hour * 60 + now.minute
    target_minutes = target_hour * 60 + 5
    
    if abs(now_minutes - target_minutes) > 2 and abs(now_minutes - target_minutes) < (24*60 - 2):
        return
        
    if not is_market_open():
        return
    
    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    
    latest_version = cfg.get_latest_version()

    jitter_seconds = random.randint(0, 180)

    await context.bot.send_message(
        chat_id=chat_id, 
        text=f"🌃 <b>[{target_hour}:05] 앱솔루트 퀀트 시스템 {latest_version} 통합 주문 장전!</b>\n"
             f"🛡️ 서버 접속 부하(동시접속) 방지를 위해 <b>{jitter_seconds}초</b> 대기 후 안전하게 주문 전송을 시작합니다.", 
        parse_mode='HTML'
    )

    await asyncio.sleep(jitter_seconds)

    MAX_RETRIES = 15
    RETRY_DELAY = 60

    async def _do_regular_trade():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None:
                return False, "❌ 계좌 정보를 불러오지 못했습니다."

            sorted_tickers, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)
            
            plans = {}
            msgs = {t: "" for t in sorted_tickers}
            all_success = {t: True for t in sorted_tickers}

            for t in sorted_tickers:
                if cfg.check_lock(t, "REG"): continue
                h = holdings.get(t, {'qty': 0, 'avg': 0})
                
                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                prev_c = await asyncio.to_thread(broker.get_previous_close, t)
                ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                
                plan = strategy.get_plan(t, curr_p, float(h['avg']), int(h['qty']), prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t])
                plans[t] = plan
                
                if plan['orders']:
                    is_rev = plan.get('is_reverse', False)
                    title = f"🔄 <b>[{t}] 리버스 주문 실행 (교차 전송)</b>\n" if is_rev else f"💎 <b>[{t}] 정규장 주문 실행 (교차 전송)</b>\n"
                    msgs[t] += title

            for t in sorted_tickers:
                if t not in plans or not plans[t]['orders']: continue
                for o in plans[t].get('core_orders', []):
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    if not is_success: all_success[t] = False
                    err_msg = res.get('msg1')
                    msgs[t] += f"└ 1차 필수: {o['desc']} {o['qty']}주: {'✅' if is_success else f'❌({err_msg})'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans or not plans[t]['orders']: continue
                for o in plans[t].get('bonus_orders', []):
                    res = broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                    is_success = res.get('rt_cd') == '0'
                    msgs[t] += f"└ 2차 보너스: {o['desc']} {o['qty']}주: {'✅' if is_success else '❌(잔금패스)'}\n"
                    await asyncio.sleep(0.2) 

            for t in sorted_tickers:
                if t not in plans or not plans[t]['orders']: continue
                
                if all_success[t] and len(plans[t].get('core_orders', [])) > 0:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>필수 주문 정상 전송 완료 (잠금 설정됨)</b>"
                elif not all_success[t]:
                    msgs[t] += "\n⚠️ <b>일부 필수 주문 실패 (매매 잠금 보류 - 다음 스케줄 재시도)</b>"
                else:
                    cfg.set_lock(t, "REG")
                    msgs[t] += "\n🔒 <b>보너스 줍줍 주문만 전송 완료 (잠금 설정됨)</b>"
                    
                await context.bot.send_message(chat_id=chat_id, text=msgs[t], parse_mode='HTML')

            return True, "SUCCESS"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            success, fail_reason = await asyncio.wait_for(_do_regular_trade(), timeout=300.0)
            if success:
                if attempt > 1:
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ <b>[통신 복구] {attempt}번째 재시도 끝에 정규장 주문 처리를 완수했습니다!</b>", parse_mode='HTML')
                return 
            else:
                logging.warning(f"정규장 주문 실패 ({attempt}/{MAX_RETRIES}): {fail_reason}")
        except asyncio.TimeoutError:
            logging.warning(f"정규장 주문 타임아웃 ({attempt}/{MAX_RETRIES})")
        except Exception as e:
            logging.error(f"정규장 주문 에러 ({attempt}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES:
            if attempt == 1 or attempt % 5 == 0:
                await context.bot.send_message(chat_id=chat_id, text=f"⚠️ <b>[API 통신 지연 감지 - {attempt}/{MAX_RETRIES}회]</b>\n한투 서버 불안정으로 계좌 조회 실패. 1분 뒤 끈질기게 다시 진입합니다! 🛡️", parse_mode='HTML')
            await asyncio.sleep(RETRY_DELAY)

    await context.bot.send_message(chat_id=chat_id, text="🚨 <b>[긴급 에러] 최대 15회(15분) 재시도에도 불구하고 KIS API 통신 복구에 실패하여, 금일 정규장 주문을 최종 취소합니다. 수동 점검이 매뉴얼합니다!</b>", parse_mode='HTML')

async def scheduled_auto_sync_summer(context):
    if not is_dst_active(): return 
    await run_auto_sync(context, "08:30")

async def scheduled_auto_sync_winter(context):
    if is_dst_active(): return 
    await run_auto_sync(context, "09:30")

async def run_auto_sync(context, time_str):
    chat_id = context.job.chat_id
    bot = context.job.data['bot']
    status_msg = await context.bot.send_message(chat_id=chat_id, text=f"📝 <b>[{time_str}] 장부 자동 동기화(무결성 검증)를 시작합니다.</b>", parse_mode='HTML')
    
    success_tickers = []
    for t in context.job.data['cfg'].get_active_tickers():
        res = await bot.process_auto_sync(t, chat_id, context, silent_ledger=True)
        if res == "SUCCESS":
            success_tickers.append(t)
            
    if success_tickers:
        async with context.job.data['tx_lock']:
            _, holdings = context.job.data['broker'].get_account_balance()
        await bot._display_ledger(success_tickers[0], chat_id, context, message_obj=status_msg, pre_fetched_holdings=holdings)
    else:
        await status_msg.edit_text(f"📝 <b>[{time_str}] 장부 동기화 완료</b> (표시할 진행 중인 장부가 없습니다)", parse_mode='HTML')

def main():
    TARGET_HOUR, season_msg = get_target_hour()
    
    cfg = ConfigManager()
    latest_version = cfg.get_latest_version() 
    
    print("=" * 50)
    print(f"🚀 앱솔루트 스노우볼 퀀트 엔진 {latest_version} (V22.03 아키텍처 탑재)")
    print(f"📅 날짜 정보: {season_msg}")
    print(f"⏰ 자동 동기화: 08:30(여름) / 09:30(겨울) 자동 변경")
    print("=" * 50)
    
    perform_self_cleaning()
    
    if ADMIN_CHAT_ID: cfg.set_chat_id(ADMIN_CHAT_ID)
    broker = KoreaInvestmentBroker(APP_KEY, APP_SECRET, CANO, ACNT_PRDT_CD)
    strategy = InfiniteStrategy(cfg)
    tx_lock = asyncio.Lock()
    bot = TelegramController(cfg, broker, strategy, tx_lock)
    
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    
    for cmd, handler in [
        ("start", bot.cmd_start), 
        ("record", bot.cmd_record), 
        ("history", bot.cmd_history), 
        ("sync", bot.cmd_sync), 
        ("settlement", bot.cmd_settlement), 
        ("seed", bot.cmd_seed), 
        ("ticker", bot.cmd_ticker), 
        ("mode", bot.cmd_mode), 
        ("reset", bot.cmd_reset), 
        ("version", bot.cmd_version),
        ("v17", bot.cmd_v17),
        ("v4", bot.cmd_v4)
    ]:
        app.add_handler(CommandHandler(cmd, handler))
    app.add_handler(CallbackQueryHandler(bot.handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.handle_message))
    
    if cfg.get_chat_id():
        jq = app.job_queue
        app_data = {'cfg': cfg, 'broker': broker, 'strategy': strategy, 'bot': bot, 'tx_lock': tx_lock}
        kst = pytz.timezone('Asia/Seoul')
        
        for tt in [datetime.time(7,0,tzinfo=kst), datetime.time(11,0,tzinfo=kst), datetime.time(16,30,tzinfo=kst), datetime.time(22,0,tzinfo=kst)]:
            jq.run_daily(scheduled_token_check, time=tt, days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_auto_sync_summer, time=datetime.time(8, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        jq.run_daily(scheduled_auto_sync_winter, time=datetime.time(9, 30, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
        for hour in [17, 18]:
            jq.run_daily(scheduled_force_reset, time=datetime.time(hour, 0, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
            jq.run_daily(scheduled_regular_trade, time=datetime.time(hour, 5, tzinfo=kst), days=(0,1,2,3,4), chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_repeating(scheduled_sniper_monitor, interval=60, chat_id=cfg.get_chat_id(), data=app_data)
        
        jq.run_daily(scheduled_self_cleaning, time=datetime.time(6, 0, tzinfo=kst), days=tuple(range(7)), chat_id=cfg.get_chat_id(), data=app_data)
        
    app.run_polling()

if __name__ == "__main__":
    main()
