# ==========================================================
# [scheduler_core.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.09 패치] API 결측치(None) 방어용 Safe Casting 전면 이식 완료
# 💡 [V24.10 수술] V_REV 동적 에스크로 차감 방어 (이중 차감 방지)
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
import pandas_market_calendars as mcal

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
        logging.error(f"⚠️ 달력 라이브러리 에러 발생. 평일이므로 강제 개장 처리합니다: {e}")
        return True

def get_budget_allocation(cash, tickers, cfg):
    sorted_tickers = sorted(tickers, key=lambda x: 0 if x == "SOXL" else (1 if x == "TQQQ" else 2))
    allocated = {}
    
    # 💡 [핵심 수술] API 결측치(None) 방어 및 순수 가용 예산(Free Cash) 도출
    safe_cash = float(cash) if cash is not None else 0.0
    
    # 💡 [V24.10 수술] 동적 에스크로 락다운 (예산 이중 차감 방어)
    dynamic_total_locked = 0.0
    for tx in tickers:
        rev_state = cfg.get_reverse_state(tx)
        if rev_state.get("is_active", False):
            # KIS 계좌에 LOC 지정가 등으로 묶였는지(Flag) 확인. getattr 방어.
            is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
            if not is_locked:
                # 주문이 안 들어간 경우에만 방어를 위해 봇 내부 차감 실행
                dynamic_total_locked += float(cfg.get_escrow_cash(tx) or 0.0)

    free_cash = max(0.0, safe_cash - dynamic_total_locked)
    
    for tx in sorted_tickers:
        rev_state = cfg.get_reverse_state(tx)
        is_rev = rev_state.get("is_active", False)
        
        # 본인 종목을 제외한 타 종목의 동적 잠금 예산 역산 앵커링
        other_locked = dynamic_total_locked
        if is_rev:
            is_locked = getattr(cfg, 'get_order_locked', lambda x: False)(tx)
            if not is_locked:
                other_locked -= float(cfg.get_escrow_cash(tx) or 0.0)
        
        if is_rev:
            # 💡 [핵심 수술] 리버스 모드 종목은 공유 예산(rem_cash) 탈취를 금지하고 오직 자신의 에스크로만 락온
            my_escrow = float(cfg.get_escrow_cash(tx) or 0.0)
            allocated[tx] = my_escrow + other_locked
        else:
            split = int(cfg.get_split_count(tx) or 0)
            seed = float(cfg.get_seed(tx) or 0.0)
            portion = seed / split if split > 0 else 0.0
            
            if free_cash >= portion:
                allocated[tx] = free_cash + other_locked
                free_cash -= portion
            else: 
                allocated[tx] = other_locked
                
    return sorted_tickers, allocated

def get_actual_execution_price(execs, target_qty, side_cd):
    if not execs: return 0.0
    
    execs.sort(key=lambda x: str(x.get('ord_tmd') or '000000'), reverse=True)
    matched_qty = 0
    total_amt = 0.0
    for ex in execs:
        if ex.get('sll_buy_dvsn_cd') == side_cd: 
            eqty = int(float(ex.get('ft_ccld_qty') or 0))
            eprice = float(ex.get('ft_ccld_unpr3') or 0.0)
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
    
    # 💡 [수술 완료] 오타(tothread) 교정
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
        
        # 💡 [V24.10 수술] 17:00 매매 스케줄러 초기화 시 주문 상태 플래그 전면 해제
        for t in cfg.get_active_tickers():
            if hasattr(cfg, 'set_order_locked'):
                cfg.set_order_locked(t, False)
        
        async with tx_lock:
            _, holdings = broker.get_account_balance()
            
        if holdings is None:
            holdings = {}
            
        msg_addons = ""
        
        for t in cfg.get_active_tickers():
            rev_state = cfg.get_reverse_state(t)
            
            if rev_state.get("is_active"):
                # 💡 [핵심 수술] holdings 객체 내부 키 누락 및 None 캐스팅 방어
                h_data = holdings.get(t) or {}
                actual_avg = float(h_data.get('avg') or 0.0)
                
                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                curr_p = float(curr_p or 0.0)
                
                if curr_p > 0 and actual_avg > 0:
                    curr_ret = (curr_p - actual_avg) / actual_avg * 100.0
                    exit_target = float(rev_state.get("exit_target") or 0.0)
                    
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
                
        final_msg = f"🔓 <b>[{target_hour}:00] 시스템 초기화 완료 (매매 잠금 해제 & 고점 관측 센서 가동)</b>" + msg_addons
        await context.bot.send_message(chat_id=chat_id, text=final_msg, parse_mode='HTML')
        
    except Exception as e:
        await context.bot.send_message(chat_id=context.job.chat_id, text=f"🚨 <b>시스템 초기화 중 에러 발생:</b> {e}", parse_mode='HTML')

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
