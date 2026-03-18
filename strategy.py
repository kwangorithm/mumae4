# ==========================================================
# [strategy.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import math
from datetime import datetime, timedelta

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config

    def _ceil(self, val): return math.ceil(val * 100) / 100.0
    def _floor(self, val): return math.floor(val * 100) / 100.0

    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, force_turbo_off=False):
        orders = []
        process_status = "" 
        
        other_locked_cash = self.cfg.get_total_locked_cash(exclude_ticker=ticker)
        real_available_cash = max(0, available_cash - other_locked_cash)
        
        split = self.cfg.get_split_count(ticker)      
        target_pct_val = self.cfg.get_target_profit(ticker) 
        target_ratio = target_pct_val / 100.0
        version = self.cfg.get_version(ticker)
        
        rev_state = self.cfg.get_reverse_state(ticker)
        is_reverse = rev_state.get("is_active", False)
        rev_day = rev_state.get("day_count", 0)
        exit_target = rev_state.get("exit_target", 0.0)

        t_val, base_portion = self.cfg.get_absolute_t_val(ticker, qty, avg_price)

        if version in ["V14", "V17"]:
            _, dynamic_budget, rem_cash = self.cfg.calculate_v14_state(ticker)
            one_portion_amt = dynamic_budget
            
            is_money_short_check = False if (is_simulation or market_type == "PRE_CHECK") else (real_available_cash < one_portion_amt)
            
            if not is_reverse and (t_val > (split - 1) or (qty > 0 and is_money_short_check)):
                is_reverse = True 
                rev_day = 1 
                
                current_return = (current_price - avg_price) / avg_price * 100.0 if avg_price > 0 else 0.0
                default_exit = -15.0 if ticker == "TQQQ" else -20.0
                
                if current_return >= default_exit:
                    exit_target = 0.0
                else:
                    exit_target = default_exit

                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
            
            if is_reverse and current_price > 0 and avg_price > 0:
                current_return = (current_price - avg_price) / avg_price * 100.0
                default_exit = -15.0 if ticker == "TQQQ" else -20.0
                
                if exit_target == 0.0 and current_return < default_exit:
                    exit_target = default_exit
                
                if current_return >= exit_target:
                    is_reverse = False
                    rev_day = 0
                    if market_type == "REG":
                        self.cfg.set_reverse_state(ticker, False, 0, 0.0)
                        self.cfg.clear_escrow_cash(ticker)
        else:
            one_portion_amt = base_portion

        depreciation_factor = 2.0 / split if split > 0 else 0.1
        star_ratio = target_ratio - (target_ratio * depreciation_factor * t_val)
        
        if is_reverse:
            # 봇 블로킹 방어: 외부에서 전달받은 ma_5day 활용
            if ma_5day > 0:
                star_price = round(ma_5day, 2)
            else:
                star_price = round(avg_price, 2)

            active_tickers_count = len(self.cfg.get_active_tickers())
            if active_tickers_count == 0: active_tickers_count = 1
            raw_portion = (rem_cash / 4.0) / active_tickers_count if rem_cash > 0 else 0.0 
            one_portion_amt = min(raw_portion, real_available_cash) if not is_simulation else raw_portion
        else:
            star_price = self._ceil(avg_price * (1 + star_ratio)) if avg_price > 0 else 0
            
        target_price = self._ceil(avg_price * (1 + target_ratio)) if avg_price > 0 else 0
        is_last_lap = (split - 1) < t_val < split
        
        if is_simulation: is_money_short = False
        else: is_money_short = real_available_cash < one_portion_amt

        base_price = current_price if current_price > 0 else prev_close
        if base_price <= 0: return {"orders": [], "t_val": t_val, "one_portion": one_portion_amt, "process_status": "⛔가격오류", "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "PRE_CHECK":
            process_status = "🌅프리마켓"
            if qty > 0 and target_price > 0 and current_price >= target_price and not is_reverse:
                orders.append({"side": "SELL", "price": current_price, "qty": qty, "type": "LIMIT", "desc": "🌅프리:목표돌파익절"})
            return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

        if market_type == "REG":
            if qty == 0:
                process_status = "✨새출발"
                buy_price = round(self._ceil(base_price * 1.15) - 0.01, 2)
                buy_qty = math.floor(one_portion_amt / buy_price) if buy_price > 0 else 0
                if buy_qty > 0:
                    orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "🆕새출발"})
                return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            if is_reverse:
                sell_divisor = 10 if split <= 20 else 20
                sell_qty = math.floor(qty / sell_divisor) 

                is_emergency_cash_needed = (real_available_cash < base_price) and (rev_day > 1)

                if rev_day == 1 or is_emergency_cash_needed:
                    process_status = "🩸리버스(긴급수혈)" if is_emergency_cash_needed else "🚨리버스(1일차)"
                    if sell_qty > 0:
                        desc_str = "🩸수혈매도(MOC)" if is_emergency_cash_needed else "🛡️의무매도"
                        orders.append({"side": "SELL", "price": 0, "qty": sell_qty, "type": "MOC", "desc": desc_str})
                else:
                    process_status = f"🔄리버스({rev_day}일차)"
                    buy_qty = 0
                    buy_price = 0
                    if one_portion_amt > 0 and star_price > 0:
                        buy_price = round(star_price - 0.01, 2)
                        if buy_price > 0: 
                            buy_qty = math.floor(one_portion_amt / buy_price)
                            if buy_qty > 0:
                                orders.append({"side": "BUY", "price": buy_price, "qty": buy_qty, "type": "LOC", "desc": "⚓잔금매수"})
                    
                    if sell_qty > 0 and star_price > 0:
                        orders.append({"side": "SELL", "price": star_price, "qty": sell_qty, "type": "LOC", "desc": "🌟별값매도"})

                    if one_portion_amt > 0 and buy_price > 0:
                        for i in range(1, 6):
                            target_qty = buy_qty + i 
                            raw_jup_price = self._floor(one_portion_amt / target_qty)
                            capped_jup_price = min(raw_jup_price, buy_price - 0.01)
                            jup_price = round(capped_jup_price, 2)
                            if jup_price > 0:
                                orders.append({"side": "BUY", "price": jup_price, "qty": 1, "type": "LOC", "desc": f"🧹리버스줍줍({i})" })
                
                if market_type == "REG":
                    self.cfg.set_reverse_state(ticker, True, rev_day, exit_target)
                        
                return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}

            if is_last_lap: process_status = "🏁마지막회차"
            elif is_money_short: process_status = "🛡️방어모드(부족)"
            elif t_val < (split / 2): process_status = "🌓전반전"
            else: process_status = "🌕후반전"

            can_buy = not is_money_short and not is_last_lap
            is_turbo_active = False if force_turbo_off else self.cfg.get_turbo_mode()
                
            if is_turbo_active and not is_last_lap:
                if is_simulation or real_available_cash >= one_portion_amt:
                    ref_price = min(avg_price, prev_close)
                    turbo_price = round(self._ceil(ref_price * 0.95) - 0.01, 2)
                    turbo_qty = math.floor(one_portion_amt / turbo_price) if turbo_price > 0 else 0
                    if turbo_qty > 0:
                        orders.append({"side": "BUY", "price": turbo_price, "qty": turbo_qty, "type": "LOC", "desc": "🏎️가속매수"})

            standard_buy_qty = 0 
            N = math.floor(one_portion_amt / avg_price) if avg_price > 0 else 0
            
            if can_buy:
                p_avg = round(self._ceil(avg_price) - 0.01, 2)
                p_star = round(star_price - 0.01, 2)

                if t_val < (split / 2):
                    half_amt = one_portion_amt * 0.5
                    q_avg_init = math.floor(half_amt / p_avg) if p_avg > 0 else 0
                    q_star = math.floor(half_amt / p_star) if p_star > 0 else 0
                    total_basic = q_avg_init + q_star
                    if total_basic < N: q_avg = q_avg_init + (N - total_basic)
                    else: q_avg = q_avg_init
                    
                    if q_avg > 0:
                        orders.append({"side": "BUY", "price": p_avg, "qty": q_avg, "type": "LOC", "desc": "⚓평단매수"})
                        standard_buy_qty += q_avg
                    if q_star > 0:
                        orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                        standard_buy_qty += q_star
                else: 
                    if p_star > 0:
                        q_star = math.floor(one_portion_amt / p_star)
                        if q_star > 0:
                            orders.append({"side": "BUY", "price": p_star, "qty": q_star, "type": "LOC", "desc": "💫별값매수"})
                            standard_buy_qty += q_star

            if one_portion_amt > 0 and (is_simulation or not is_money_short):
                base_qty_for_jupjup = standard_buy_qty if standard_buy_qty > 0 else N
                if base_qty_for_jupjup == 0: base_qty_for_jupjup = 1
                for i in range(1, 6):
                    target_qty = base_qty_for_jupjup + i 
                    raw_jup_price = self._floor(one_portion_amt / target_qty)
                    capped_jup_price = min(raw_jup_price, avg_price - 0.01)
                    jup_price = round(capped_jup_price, 2)
                    if jup_price > 0:
                        orders.append({"side": "BUY", "price": jup_price, "qty": 1, "type": "LOC", "desc": f"🧹줍줍({i})" })

            q_qty = math.ceil(qty / 4)
            r_qty = qty - q_qty 

            buy_orders_exist = (len([o for o in orders if o['side']=='BUY']) > 0)
            force_moc = is_last_lap or (is_money_short and not buy_orders_exist)
            
            if force_moc:
                orders.append({"side": "SELL", "price": 0, "qty": q_qty, "type": "MOC", "desc": "🛡️쿼터MOC"})
            else:
                if star_price > 0:
                    if version == "V17" and star_price > avg_price:
                        orders.append({"side": "SELL", "price": star_price, "qty": q_qty, "type": "LIMIT", "desc": "🦇시크릿지정가"})
                    else:
                        orders.append({"side": "SELL", "price": star_price, "qty": q_qty, "type": "LOC", "desc": "🌟쿼터매도"})

            if target_price > 0:
                orders.append({"side": "SELL", "price": target_price, "qty": r_qty, "type": "LIMIT", "desc": "🎯목표익절"})

            return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": process_status, "is_reverse": False, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}
            
        return {"orders": orders, "t_val": t_val, "one_portion": one_portion_amt, "process_status": "대기", "is_reverse": is_reverse, "star_price": star_price, "star_ratio": star_ratio, "real_cash_used": real_available_cash}
