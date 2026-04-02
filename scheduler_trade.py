# ==========================================================
# [scheduler_trade.py]
# ⚠️ VWAP Lock-in 핑퐁 버그 픽스 패치 완료 버전
# ==========================================================
import os
import logging
import datetime
import pytz
import time
import math
import asyncio
import glob
import json
import pandas_market_calendars as mcal
import random

# 💡 1부 코어에서 공통 모듈 임포트
from scheduler_core import is_market_open, get_budget_allocation, get_target_hour

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
    
    is_sniper_active_time = False
    switch_time = market_open + datetime.timedelta(minutes=50)
    if now_est >= switch_time:
        is_sniper_active_time = True

    app_data = context.job.data
    cfg, broker, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    target_cache = app_data.setdefault('dynamic_targets', {})
    tracking_cache = app_data.setdefault('sniper_tracking', {})
    master_switch_alerted = app_data.setdefault('master_switch_alerted', {}) 
    
    today_est_str = now_est.strftime('%Y%m%d')
    saved_date = target_cache.get('date')
    
    if saved_date != today_est_str:
        target_cache.clear()
        target_cache['date'] = today_est_str
        tracking_cache.clear()
        tracking_cache['date'] = today_est_str
        master_switch_alerted.clear()
        master_switch_alerted['date'] = today_est_str
        
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
                
                # 💡 [핵심 수술] VWAP 데스존(15:29 ~ 15:45) 엇갈림 방지를 위한 스나이퍼 조기 퇴근
                if version == "V_VWAP" and now_est.hour == 15 and now_est.minute >= 29:
                    continue
                
                # 💡 [핵심 수술] 인자 누락 버그 픽스 완료
                is_upward_sniper_on = cfg.get_upward_sniper_mode(t)
                
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
                
                actual_day_high, _ = await asyncio.to_thread(broker.get_day_high_low, t)
                
                tracking_info = tracking_cache.setdefault(t, {
                    'is_tracking': False, 'lowest_price': float('inf'), 'day_high': 0.0, 'armed_price': 0.0, 'alerted': False,
                    'is_trailing': False, 'peak_price': 0.0, 'trailing_armed': False, 'trigger_price': 0.0
                })
                
                if actual_day_high > tracking_info['day_high']:
                    tracking_info['day_high'] = actual_day_high
                
                if version == "V17":
                    idx_ticker = "SOXX" if t == "SOXL" else "QQQ"
                    current_weight = cfg.get_sniper_multiplier(t)
                    cached_data = target_cache.get(t)
                    
                    if cached_data is None or cached_data.get('weight') != current_weight:
                        tgt = await asyncio.to_thread(broker.get_dynamic_sniper_target, idx_ticker)
                        if tgt is not None:
                            target_cache[t] = {'value': float(tgt), 'weight': current_weight, 'metric_weight': tgt.weight}
                        else:
                            target_cache[t] = {'value': (7.59 if t=="SOXL" else 6.18), 'weight': current_weight, 'metric_weight': 1.0}

                    sniper_pct = target_cache[t]['value']
                    market_weight = target_cache[t]['metric_weight']
                    
                    if is_sniper_active_time and not master_switch_alerted.get(t, False):
                        master_switch_alerted[t] = True
                        state_msg = "🔫 하방 매수[ON] / 🛡️ 상방 익절[OFF] (추세 이익 극대화)" if market_weight <= 1.0 else "🔫 하방 매수[OFF] / 🛡️ 상방 익절[ON] (폭락장 칼날 회피)"
                        msg = f"📡 <b>[{t}] 마스터 스위치 락온 (10:20 EST)</b>\n"
                        msg += f"▫️ 공포 가중치: {market_weight:.2f}배\n"
                        msg += f"▫️ 고정 타격선(1년 ATR): -{abs(sniper_pct):.2f}%\n"
                        msg += f"▫️ 당일 자율제어: {state_msg}\n"
                        msg += f"👀 오프닝 휩소가 진정되었습니다. 누적된 최고점(${tracking_info['day_high']:.2f})을 앵커로 실사냥을 개시합니다."
                        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

                    if not is_sniper_active_time:
                        continue

                    if market_weight <= 1.0: 
                        if not lock_buy:
                            candle = await asyncio.to_thread(broker.get_current_5min_candle, t)
                            
                            if candle:
                                c_open, c_high, c_low, c_close = candle['open'], candle['high'], candle['low'], candle['close']
                                c_vol, c_vol_ma10, c_vol_ma20 = candle['volume'], candle.get('vol_ma10', candle['volume']), candle.get('vol_ma20', candle['volume'])
                                c_vwap = candle.get('vwap', 0.0)
                                
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
                                        is_volume_spike = (c_vol > c_vol_ma20)
                                    else:
                                        is_volume_spike = True
                                    
                                    if is_yangbong and is_rebounded and is_volume_spike:
                                        is_deep_oversold = True if c_vwap <= 0 else (c_close < c_vwap)
                                        
                                        if is_deep_oversold:
                                            if cfg.get_secret_mode():
                                                
                                                ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                                                temp_plan = strategy.get_plan(t, curr_p, avg_price, qty, prev_c, ma_5day=ma_5day, market_type="REG", available_cash=allocated_cash[t], is_simulation=True)
                                                sniper_budget = temp_plan.get('one_portion', 0.0)
                                                
                                                exec_price = min(c_close, tracking_info['armed_price'])
                                                
                                                if sniper_budget >= exec_price and exec_price > 0:
                                                    cancel_buy_prices = [o['price'] for o in temp_plan.get('core_orders', []) if o['side'] == 'BUY']
                                                    if cancel_buy_prices:
                                                        await asyncio.to_thread(broker.cancel_orders_by_price, t, "BUY", list(set(cancel_buy_prices)))
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
                                                            
                                                            for _ in range(4): 
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
                                                        vol_str = f"기관 매수세 확증 (Vol > MA20 돌파)" if is_regular_session else "거래량 필터 면제 (정규장 외 세션)"
                                                        
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
                                        else:
                                            now_ts = time.time()
                                            fail_history = app_data.setdefault('sniper_fail_ts', {})
                                            if now_ts - fail_history.get(f"{t}_vwap", 0) > 300:
                                                msg = f"🛡️ <b>[{t}] 하방 스나이퍼 매수 취소 (VWAP 필터 가동)</b>\n"
                                                msg += f"반등 조건은 충족했으나 현재가(${c_close:.2f})가 당일 평균가(${c_vwap:.2f})를 넘어선 비싼 가격입니다. 가짜 반등(데드캣 바운스)으로 판별하여 사격을 취소합니다."
                                                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                                                fail_history[f"{t}_vwap"] = now_ts
                    else:
                        pass

                if not is_sniper_active_time:
                    continue

                if version == "V17":
                    cached_data = target_cache.get(t, {})
                    market_weight = cached_data.get('metric_weight', 1.0)
                    if market_weight <= 1.0:
                        continue

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
                                    
                                    for _ in range(4): 
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
                
                # 💡 [핵심 수술] 리버스 모드 안전 마진(1.005) 전면 배제 및 순수 5MA 우선 락온
                if is_rev:
                    q_qty = max(4, math.floor(qty / sell_divisor)) if qty >= 4 else qty
                    ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                    base_trigger = round(ma_5day, 2) if ma_5day > 0 else (math.ceil(avg_price * 100) / 100.0)
                else:
                    safe_floor_price = math.ceil(avg_price * 1.005 * 100) / 100.0
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
                    
                    candle = await asyncio.to_thread(broker.get_current_5min_candle, t)
                    c_vwap = candle.get('vwap', 0.0) if candle else 0.0
                    is_vwap_death_cross = (c_vwap > 0 and curr_p < c_vwap)
                    
                    if curr_p <= drop_trigger or is_vwap_death_cross:
                        intercept_price = max(drop_trigger, base_trigger)
                        
                        if is_vwap_death_cross and curr_p > drop_trigger:
                            intercept_price = max(curr_p, base_trigger)
                            
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
                                msg += f"가격(${curr_p:.2f})이 붕괴 조건을 충족했으나, 한투 서버에서 기존 방어선(LOC)을 찾지 못했습니다.\n"
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
                                    
                                    for _ in range(4): 
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
                            
                            if is_vwap_death_cross and curr_p > drop_trigger:
                                msg += f"▫️ 지지선 붕괴: 당일 VWAP(${c_vwap:.2f}) 데드크로스 포착 (조기 기습 익절 가동)\n\n"
                            else:
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

async def scheduled_vwap_trade(context):
    if not is_market_open(): return
    
    est = pytz.timezone('US/Eastern')
    now_est = datetime.datetime.now(est)
    
    # 💡 장 마감 30분 전 타임 윈도우 검증 (15:30 ~ 15:59 EST)
    if not (now_est.hour == 15 and 30 <= now_est.minute <= 59):
        return
        
    app_data = context.job.data
    cfg, broker, vwap_strategy, strategy, tx_lock = app_data['cfg'], app_data['broker'], app_data['vwap_strategy'], app_data['strategy'], app_data['tx_lock']
    chat_id = context.job.chat_id
    
    vwap_cache = app_data.setdefault('vwap_cache', {})
    today_str = now_est.strftime('%Y%m%d')
    
    if vwap_cache.get('date') != today_str:
        vwap_cache.clear()
        vwap_cache['date'] = today_str
        # 💡 [핵심 수술] 새로운 영업일 시작 시 P매매 잉여 수량 완벽 소각(자연 소멸)
        cfg.clear_p_trade_data()

    # 💡 백테스트 데이터 기반 U-Curve 30분 가중치 할당 (15:30 ~ 15:59)
    U_CURVE_WEIGHTS = [
        0.0308, 0.0220, 0.0190, 0.0228, 0.0179, 0.0191, 0.0199, 0.0190, 0.0187, 0.0213,
        0.0216, 0.0234, 0.0231, 0.0210, 0.0205, 0.0252, 0.0225, 0.0228, 0.0238, 0.0229,
        0.0259, 0.0284, 0.0331, 0.0385, 0.0400, 0.0461, 0.0553, 0.0620, 0.0750, 0.1180
    ]
    min_idx = now_est.minute - 30
    current_weight = U_CURVE_WEIGHTS[min_idx] if 0 <= min_idx < 30 else 0.0
        
    async def _do_vwap():
        async with tx_lock:
            cash, holdings = broker.get_account_balance()
            if holdings is None: return
            
            _, allocated_cash = get_budget_allocation(cash, cfg.get_active_tickers(), cfg)

            # ==========================================================
            # 💡 P-VWAP 단독 타임 슬라이싱 가동
            # ==========================================================
            p_trade_data = cfg.get_p_trade_data()
            p_trade_changed = False
            
            for p_ticker, p_orders in p_trade_data.items():
                if not p_orders: continue
                
                curr_p = await asyncio.to_thread(broker.get_current_price, p_ticker)
                if curr_p <= 0: continue
                
                for o in p_orders:
                    rem_qty = o.get('rem_qty', 0)
                    if rem_qty <= 0: continue
                    
                    target_price = o['target_price']
                    side = o['side']
                    
                    # 1분 단위 동적 할당량 계산 (KIS API 정수 체결 규격을 위한 내림 처리)
                    slice_qty = math.floor(o['qty'] * current_weight)
                    
                    if slice_qty > rem_qty:
                        slice_qty = rem_qty
                        
                    if slice_qty <= 0:
                        continue
                        
                    # 💡 상하한선 방어막 검증 (자연 마감 원칙: 조건 미달 시 다음 분으로 이월)
                    if side == "BUY" and curr_p > target_price:
                        continue
                    if side == "SELL" and curr_p < target_price:
                        continue
                        
                    # 실시간 1호가 포착
                    ask_price = await asyncio.to_thread(broker.get_ask_price, p_ticker)
                    bid_price = await asyncio.to_thread(broker.get_bid_price, p_ticker)
                    
                    exec_price = ask_price if side == "BUY" else bid_price
                    if exec_price <= 0:
                        exec_price = curr_p
                        
                    # 호가 변동 1호가 재검증
                    if (side == "BUY" and exec_price > target_price) or (side == "SELL" and exec_price < target_price):
                        continue
                        
                    res = broker.send_order(p_ticker, side, slice_qty, exec_price, "LIMIT")
                    odno = res.get('odno', '')
                    
                    # 💡 ODNO 기반 정밀 체결 검증 및 유령 잔량 철거
                    if res.get('rt_cd') == '0' and odno:
                        ccld_qty = 0
                        for _ in range(4):
                            await asyncio.sleep(2.0)
                            unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, p_ticker)
                            my_order = next((ox for ox in unfilled_check if ox.get('odno') == odno), None)
                            if my_order:
                                ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                break
                                
                            execs = await asyncio.to_thread(broker.get_execution_history, p_ticker, today_str, today_str)
                            my_execs = [ex for ex in execs if ex.get('odno') == odno]
                            if my_execs:
                                ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                if ccld_qty >= slice_qty:
                                    break
                                    
                        if ccld_qty < slice_qty:
                            await asyncio.to_thread(broker.cancel_order, p_ticker, odno)
                            await asyncio.sleep(1.0)
                            
                        if ccld_qty > 0:
                            o['rem_qty'] -= ccld_qty
                            p_trade_changed = True
                            
            if p_trade_changed:
                cfg.set_p_trade_data(p_trade_data)

            # ==========================================================
            # 기존 무한매수 VWAP 로직
            # ==========================================================
            for t in cfg.get_active_tickers():
                if cfg.get_version(t) != "V_VWAP": 
                    continue
                    
                h = holdings.get(t, {'qty': 0, 'avg': 0})
                actual_avg = float(h['avg'])
                actual_qty = int(h['qty'])
                
                curr_p = await asyncio.to_thread(broker.get_current_price, t)
                prev_c = await asyncio.to_thread(broker.get_previous_close, t)
                ma_5day = await asyncio.to_thread(broker.get_5day_ma, t)
                if curr_p <= 0: continue
                
                # 💡 오리지널 정규장 예산/수량 산출
                plan = strategy.get_plan(
                    t, curr_p, actual_avg, actual_qty, prev_c, ma_5day=ma_5day,
                    market_type="REG", available_cash=allocated_cash[t], is_simulation=True
                )
                
                # ==========================================================
                # 💡 [핵심 버그 수정] VWAP 타격 기준가 및 예산을 최초 1회 락인(Lock-in)하여 핑퐁 엇갈림 방지
                # ==========================================================
                if f"{t}_plan_locked" not in vwap_cache:
                    target_star_buy_budget = 0.0
                    target_avg_buy_budget = 0.0
                    target_sell_qty = 0
                    jackpot_orders = []
                    
                    for o in plan.get('core_orders', []):
                        if o['side'] == 'BUY':
                            if '별값' in o['desc'] or '수혈' in o['desc'] or '잔금' in o['desc']:
                                target_star_buy_budget += (o['qty'] * o['price'])
                            else:
                                target_avg_buy_budget += (o['qty'] * o['price'])
                        elif o['side'] == 'SELL':
                            if o['type'] == 'LOC': 
                                target_sell_qty += o['qty']
                            elif o['type'] == 'LIMIT':
                                jackpot_orders.append(o)
                                
                    # 15:30에 계산된 최초 결과값을 캐시에 영구 저장 (30분간 유지)
                    vwap_cache[f"{t}_star_price"] = plan.get('star_price', 0.0)
                    vwap_cache[f"{t}_actual_avg"] = actual_avg
                    vwap_cache[f"{t}_target_star_buy_budget"] = target_star_buy_budget
                    vwap_cache[f"{t}_target_avg_buy_budget"] = target_avg_buy_budget
                    vwap_cache[f"{t}_target_sell_qty"] = target_sell_qty
                    vwap_cache[f"{t}_jackpot_orders"] = jackpot_orders
                    vwap_cache[f"{t}_plan_locked"] = True
                
                # 💡 매 1분 루프마다 T값이 흔들리지 않도록 캐싱된 고정 스냅샷 값 사용
                star_price = vwap_cache[f"{t}_star_price"]
                actual_avg = vwap_cache[f"{t}_actual_avg"]
                target_star_buy_budget = vwap_cache[f"{t}_target_star_buy_budget"]
                target_avg_buy_budget = vwap_cache[f"{t}_target_avg_buy_budget"]
                target_sell_qty = vwap_cache[f"{t}_target_sell_qty"]
                
                # ==========================================================
                # 💡 [전면 초토화 로직] 15:30 EST 정각: 줍줍 포함 호가창 전면 Nuke
                # ==========================================================
                if not vwap_cache.get(f"{t}_cancelled"):
                    await context.bot.send_message(
                        chat_id=chat_id, 
                        text=f"🚨 <b>[{t}] VWAP 락온: 호가창 전면 초토화 (15:30 EST)</b>\n"
                             f"▫️ 묶여있던 예수금과 주식을 100% 해방시키기 위해 줍줍을 포함한 모든 주문을 일괄 소각(Nuke)합니다.\n"
                             f"▫️ 급등 방어용 12% 잭팟(지정가 매도)만 단독 재장전 후 VWAP 타격을 개시합니다.", 
                        parse_mode='HTML'
                    )
                    
                    # 1. 💡 모든 매수(BUY) 및 매도(SELL) 주문 예외 없이 전량 강제 취소
                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, "BUY")
                    await asyncio.to_thread(broker.cancel_all_orders_safe, t, "SELL")
                    
                    # 2. 💡 12% 잭팟(지정가 매도)만 정밀 재장전 (재계산 방지 캐시 데이터 사용)
                    for o in vwap_cache.get(f"{t}_jackpot_orders", []):
                        broker.send_order(t, o['side'], o['qty'], o['price'], o['type'])
                        await asyncio.sleep(0.2)
                            
                    vwap_cache[f"{t}_cancelled"] = True
                    await asyncio.sleep(5.0) 
                    cash, holdings = broker.get_account_balance() # 해방된 자금/주식 최종 갱신
                
                # 💡 매수(BUY) VWAP 타임 슬라이싱 (상한선 방어막 필터링)
                rem_star_budget = max(0.0, target_star_buy_budget - vwap_cache.get(f"{t}_star_buy_executed", 0.0))
                rem_avg_budget = max(0.0, target_avg_buy_budget - vwap_cache.get(f"{t}_avg_buy_executed", 0.0))
                
                buy_qty = 0
                if curr_p <= star_price and rem_star_budget > 0:
                    p1 = vwap_strategy.get_vwap_plan(t, curr_p, rem_star_budget, side="BUY")
                    if p1['orders']: buy_qty += p1['orders'][0]['qty']
                    
                if curr_p <= actual_avg and rem_avg_budget > 0:
                    p2 = vwap_strategy.get_vwap_plan(t, curr_p, rem_avg_budget, side="BUY")
                    if p2['orders']: buy_qty += p2['orders'][0]['qty']
                    
                if buy_qty > 0:
                    ask_price = await asyncio.to_thread(broker.get_ask_price, t)
                    exec_price = ask_price if ask_price > 0 else curr_p
                    
                    # 1호가로 재검증
                    valid_buy_qty = 0
                    if exec_price <= star_price and rem_star_budget > 0:
                        p1 = vwap_strategy.get_vwap_plan(t, exec_price, rem_star_budget, side="BUY")
                        if p1['orders']: valid_buy_qty += p1['orders'][0]['qty']
                    if exec_price <= actual_avg and rem_avg_budget > 0:
                        p2 = vwap_strategy.get_vwap_plan(t, exec_price, rem_avg_budget, side="BUY")
                        if p2['orders']: valid_buy_qty += p2['orders'][0]['qty']
                        
                    if valid_buy_qty > 0:
                        res = broker.send_order(t, "BUY", valid_buy_qty, exec_price, "LIMIT")
                        odno = res.get('odno', '')
                        
                        if res.get('rt_cd') == '0' and odno:
                            ccld_qty = 0
                            for _ in range(4):
                                await asyncio.sleep(2.0)
                                unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                my_order = next((ox for ox in unfilled_check if ox.get('odno') == odno), None)
                                if my_order:
                                    ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                    break
                                    
                                execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                if my_execs:
                                    ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                    if ccld_qty >= valid_buy_qty:
                                        break
                                        
                            if ccld_qty < valid_buy_qty:
                                await asyncio.to_thread(broker.cancel_order, t, odno)
                                await asyncio.sleep(1.0)
                                
                            if ccld_qty > 0:
                                spent = ccld_qty * exec_price
                                s_active = rem_star_budget if exec_price <= star_price else 0.0
                                a_active = rem_avg_budget if exec_price <= actual_avg else 0.0
                                tot_act = s_active + a_active
                                if tot_act > 0:
                                    vwap_cache[f"{t}_star_buy_executed"] = vwap_cache.get(f"{t}_star_buy_executed", 0.0) + spent * (s_active / tot_act)
                                    vwap_cache[f"{t}_avg_buy_executed"] = vwap_cache.get(f"{t}_avg_buy_executed", 0.0) + spent * (a_active / tot_act)
                        await asyncio.sleep(0.2)
                            
                # 💡 매도(SELL) VWAP 타임 슬라이싱
                executed_sell_qty = vwap_cache.get(f"{t}_sell_executed", 0)
                remaining_sell_qty = max(0, target_sell_qty - executed_sell_qty)
                
                if remaining_sell_qty > 0 and curr_p >= star_price:
                    vwap_sell_plan = vwap_strategy.get_vwap_plan(t, curr_p, remaining_sell_qty, side="SELL")
                    
                    if vwap_sell_plan['orders']:
                        for o in vwap_sell_plan['orders']:
                            bid_price = await asyncio.to_thread(broker.get_bid_price, t)
                            exec_price = bid_price if bid_price > 0 else o['price']
                            
                            if exec_price >= star_price: 
                                res = broker.send_order(t, o['side'], o['qty'], exec_price, o['type'])
                                odno = res.get('odno', '')
                                
                                if res.get('rt_cd') == '0' and odno:
                                    ccld_qty = 0
                                    for _ in range(4):
                                        await asyncio.sleep(2.0)
                                        unfilled_check = await asyncio.to_thread(broker.get_unfilled_orders_detail, t)
                                        my_order = next((ox for ox in unfilled_check if ox.get('odno') == odno), None)
                                        if my_order:
                                            ccld_qty = int(float(my_order.get('tot_ccld_qty', 0)))
                                            break
                                            
                                        execs = await asyncio.to_thread(broker.get_execution_history, t, today_est_str, today_est_str)
                                        my_execs = [ex for ex in execs if ex.get('odno') == odno]
                                        if my_execs:
                                            ccld_qty = sum(int(float(ex.get('ft_ccld_qty', '0'))) for ex in my_execs)
                                            if ccld_qty >= o['qty']:
                                                break
                                                
                                    if ccld_qty < o['qty']:
                                        await asyncio.to_thread(broker.cancel_order, t, odno)
                                        await asyncio.sleep(1.0)
                                        
                                    if ccld_qty > 0:
                                        vwap_cache[f"{t}_sell_executed"] = vwap_cache.get(f"{t}_sell_executed", 0) + ccld_qty
                                await asyncio.sleep(0.2)
                        
    try:
        await asyncio.wait_for(_do_vwap(), timeout=45.0)
    except Exception as e:
        logging.error(f"🚨 VWAP 스케줄러 에러: {e}")

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
                    ver_txt = "VWAP 장전" if cfg.get_version(t) == "V_VWAP" else "정규장 주문"
                    title = f"🔄 <b>[{t}] 리버스 주문 실행 (교차 전송)</b>\n" if is_rev else f"💎 <b>[{t}] {ver_txt} 실행 (교차 전송)</b>\n"
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
