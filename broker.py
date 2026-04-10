# ==========================================================
# [broker.py] (1부 / 2부) - 🌟 100% 통합 완성본 🌟
# ⚠️ 수술 내역: 야후 파이낸스(yfinance) 좀비 스레드 누적 방지
# 모든 history() 호출에 timeout=5 파라미터 강제 주입 완료
# ==========================================================

import requests
import json
import time
import datetime
import os
import math
import yfinance as yf
import pytz
import tempfile
import pandas as pd   
import numpy as np
import volatility_engine as ve  

class KoreaInvestmentBroker:
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
        self._excg_cd_cache = {} 
        
        self._get_access_token()

    def _get_access_token(self, force=False):
        if not force and os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    saved = json.load(f)
                expire_time = datetime.datetime.strptime(saved['expire'], '%Y-%m-%d %H:%M:%S')
                if expire_time > datetime.datetime.now() + datetime.timedelta(hours=1):
                    self.token = saved['token']
                    return
            except Exception: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except Exception: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        try:
            res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body), timeout=10)
            data = res.json()
            if 'access_token' in data:
                self.token = data['access_token']
                expire_str = (datetime.datetime.now() + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
                
                dir_name = os.path.dirname(self.token_file)
                if dir_name and not os.path.exists(dir_name):
                    os.makedirs(dir_name, exist_ok=True)
                fd, temp_path = tempfile.mkstemp(dir=dir_name, text=True)
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump({'token': self.token, 'expire': expire_str}, f)
                    f.flush()
                    os.fsync(fd)
                os.replace(temp_path, self.token_file)
            else:
                print(f"❌ [Broker] 토큰 발급 실패: {data.get('error_description', '알 수 없는 오류')}")
        except Exception as e:
            print(f"❌ [Broker] 토큰 통신 에러: {e}")

    def _get_header(self, tr_id):
        return {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P"
        }

    def _api_request(self, method, url, headers, params=None, data=None):
        for attempt in range(2): 
            try:
                if method.upper() == "GET":
                    res = requests.get(url, headers=headers, params=params, timeout=10)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None, timeout=10)
                    
                resp_json = res.json()
                
                if resp_json.get('rt_cd') != '0':
                    msg1 = resp_json.get('msg1', '')
                    if any(x in msg1.lower() for x in ['토큰', '접근토큰', 'token', 'expired', 'mig', '인증', 'authorization']):
                        if attempt == 0: 
                            print(f"\n🚨 [안전장치 가동] API 토큰 만료 감지! : {msg1}")
                            self._get_access_token(force=True)
                            headers["authorization"] = f"Bearer {self.token}"
                            time.sleep(1.0)
                            continue
                return res, resp_json
            except Exception as e:
                print(f"⚠️ API 통신 중 예외 발생: {e}")
                if attempt == 1: return None, {}
                time.sleep(1.0)
        return None, {}

    def _call_api(self, tr_id, url_path, method="GET", params=None, body=None):
        headers = self._get_header(tr_id)
        url = f"{self.base_url}{url_path}"
        res, resp_json = self._api_request(method, url, headers, params=params, data=body)
        if not resp_json: return {'rt_cd': '999', 'msg1': '통신 오류 또는 최대 재시도 횟수 초과'}
        return resp_json

    def _ceil_2(self, value):
        if value is None: return 0.0
        return math.ceil(value * 100) / 100.0

    def _safe_float(self, value):
        try: return float(str(value).replace(',', ''))
        except Exception: return 0.0

    def _get_exchange_code(self, ticker, target_api="PRICE"):
        if ticker in self._excg_cd_cache:
            codes = self._excg_cd_cache[ticker]
            return codes['PRICE'] if target_api == "PRICE" else codes['ORDER']

        price_cd = "NAS"
        order_cd = "NASD"
        dynamic_success = False

        try:
            for prdt_type in ["512", "513", "529"]:
                params = {
                    "PRDT_TYPE_CD": prdt_type,
                    "PDNO": ticker
                }
                res = self._call_api("CTPF1702R", "/uapi/overseas-price/v1/quotations/search-info", "GET", params=params)
                
                if res.get('rt_cd') == '0' and res.get('output'):
                    excg_name = str(res['output'].get('ovrs_excg_cd', '')).upper()
                    if "NASD" in excg_name or "NASDAQ" in excg_name:
                        price_cd, order_cd = "NAS", "NASD"
                        dynamic_success = True
                        break
                    elif "NYSE" in excg_name or "NEW YORK" in excg_name:
                        price_cd, order_cd = "NYS", "NYSE"
                        dynamic_success = True
                        break
                    elif "AMEX" in excg_name:
                        price_cd, order_cd = "AMS", "AMEX"
                        dynamic_success = True
                        break
        except Exception as e:
            print(f"⚠️ [Broker] 거래소 코드 동적 획득 실패: {ticker} - {e}")

        if not dynamic_success:
            if ticker == "SOXL": price_cd, order_cd = "AMS", "AMEX"
            elif ticker == "TQQQ": price_cd, order_cd = "NAS", "NASD"

        self._excg_cd_cache[ticker] = {'PRICE': price_cd, 'ORDER': order_cd}
        return price_cd if target_api == "PRICE" else order_cd

    def get_account_balance(self):
        cash = 0.0
        holdings = {}
        api_success = False 
        
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
        
        if res.get('rt_cd') == '0':
            api_success = True
            o2 = res.get('output2', {})
            if isinstance(o2, list) and len(o2) > 0: o2 = o2[0]
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))       
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = math.floor((raw_bp * 0.9945) * 100) / 100.0              

        target_excgs = ["NASD", "AMEX", "NYSE"] 
        
        for excg in target_excgs:
            params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg, "TR_CRCY_CD": "USD", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
            res_hold = self._call_api("TTTS3012R", "/uapi/overseas-stock/v1/trading/inquire-balance", "GET", params_hold)
            
            if res_hold.get('rt_cd') == '0':
                api_success = True
                if cash <= 0:
                    o2 = res_hold.get('output2', {})
                    if isinstance(o2, list) and len(o2) > 0: o2 = o2[0]
                    new_cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
                    if new_cash > cash: cash = new_cash
                
                for item in res_hold.get('output1', []):
                    ticker = item.get('ovrs_pdno')
                    qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                    avg = self._safe_float(item.get('pchs_avg_pric', 0))
                    if qty > 0 and ticker not in holdings: 
                        holdings[ticker] = {'qty': qty, 'avg': avg}
        
        if api_success:
            return cash, holdings
        else:
            return cash, None

    def get_current_5min_candle(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            # 💡 [수술] timeout=5 강제 주입
            df = stock.history(period="5d", interval="1m", prepost=True, timeout=5)
            
            if df.empty:
                return None
                
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
                
            df.index = df.index.tz_convert('America/New_York')
            
            regular_market = df.between_time('09:30', '15:59')
            
            if regular_market.empty:
                return None
                
            typical_price = (regular_market['High'] + regular_market['Low'] + regular_market['Close']) / 3.0
            vol_price = typical_price * regular_market['Volume']
            
            cum_vol_price = vol_price.cumsum()
            cum_vol = regular_market['Volume'].cumsum()
            
            vwap_series = cum_vol_price / cum_vol.replace(0, 1) 
            current_vwap = float(vwap_series.iloc[-1]) if not vwap_series.empty else 0.0
            
            resampled = regular_market.resample('5min', label='left', closed='left').agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            }).dropna()
            
            if resampled.empty:
                return None
                
            resampled['Vol_MA10'] = resampled['Volume'].rolling(10, min_periods=1).mean()
            resampled['Vol_MA20'] = resampled['Volume'].rolling(20, min_periods=1).mean()
            
            last_candle = resampled.iloc[-1]
            
            vol_ma10 = float(last_candle['Vol_MA10']) if not pd.isna(last_candle['Vol_MA10']) else float(last_candle['Volume'])
            vol_ma20 = float(last_candle['Vol_MA20']) if not pd.isna(last_candle['Vol_MA20']) else float(last_candle['Volume'])
            
            latest_1m = df.iloc[-1]
            
            return {
                'open': float(last_candle['Open']),
                'high': float(latest_1m['High']),  
                'low': float(latest_1m['Low']),    
                'close': float(latest_1m['Close']),
                'volume': float(last_candle['Volume']), 
                'vol_ma10': vol_ma10,
                'vol_ma20': vol_ma20,
                'vwap': current_vwap  
            }
        except Exception as e:
            print(f"⚠️ [Broker] 실시간 5분봉 캔들 조회 실패 ({ticker}): {e}")
            return None

    def get_current_price(self, ticker, is_market_closed=False):
        try:
            stock = yf.Ticker(ticker)
            if is_market_closed: return float(stock.fast_info['last_price'])
            # 💡 [수술] timeout=5 강제 주입
            hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty: return float(hist['Close'].iloc[-1])
            else: return float(stock.fast_info['last_price'])
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 현재가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                return float(res.get('output', {}).get('last', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 현재가 우회 조회 실패: {e}")
        return 0.0
        
    def get_ask_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) > 0:
                    return float(output2[0].get('pask1', 0.0))
                elif isinstance(output2, dict):
                    return float(output2.get('pask1', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 매도 1호가 조회 실패: {e}")
        return 0.0

    def get_bid_price(self, ticker):
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200100", "/uapi/overseas-price/v1/quotations/inquire-asking-price", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) > 0:
                    return float(output2[0].get('pbid1', 0.0))
                elif isinstance(output2, dict):
                    return float(output2.get('pbid1', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 매수 1호가 조회 실패: {e}")
        return 0.0

    def get_previous_close(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            # 💡 [수술] timeout=5 강제 주입
            hist = stock.history(period="5d", timeout=5)
            if not hist.empty:
                est = pytz.timezone('US/Eastern')
                now_est = datetime.datetime.now(est)
                
                cutoff_date = now_est.date()
                if now_est.time() < datetime.time(16, 0):
                    cutoff_date -= datetime.timedelta(days=1)
                
                if hist.index.tzinfo is None:
                    hist.index = hist.index.tz_localize('UTC').tz_convert(est)
                else:
                    hist.index = hist.index.tz_convert(est)
                    
                past_hist = hist[hist.index.date <= cutoff_date]
                if not past_hist.empty:
                    return float(past_hist['Close'].iloc[-1])
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 전일 정규장 종가 파싱 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                return float(res.get('output', {}).get('base', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 전일종가 우회 조회 실패: {e}")
        return 0.0
    def get_5day_ma(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            # 💡 [수술] timeout=5 강제 주입
            hist = stock.history(period="10d", timeout=5) 
            if len(hist) >= 5: return float(hist['Close'][-5:].mean())
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] MA5 에러, 한투 API 우회 가동: {e}")
            
        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {
                "AUTH": "", "EXCD": excg_cd, "SYMB": ticker,
                "GUBN": "0", "BYMD": "", "MODP": "1"
            }
            res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) >= 5:
                    closes = [float(x['clos']) for x in output2[:5]]
                    return sum(closes) / len(closes)
        except Exception as e:
            print(f"❌ [한투 API] MA5 우회 조회 실패: {e}")
            
        return 0.0

    def get_unfilled_orders(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
        res = self._call_api("TTTS3018R", "/uapi/overseas-stock/v1/trading/inquire-nccs", "GET", params=params)
        if res.get('rt_cd') == '0':
            output = res.get('output', [])
            if isinstance(output, dict): output = [output]
            return [item.get('odno') for item in output if item.get('pdno') == ticker]
        return []

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
        res = self._call_api("TTTS3018R", "/uapi/overseas-stock/v1/trading/inquire-nccs", "GET", params=params)
        if res.get('rt_cd') == '0':
            output = res.get('output', [])
            if isinstance(output, dict): output = [output]
            return [item for item in output if item.get('pdno') == ticker]
        return []

    def cancel_all_orders_safe(self, ticker, side=None):
        for i in range(3):
            orders = self.get_unfilled_orders_detail(ticker)
            if not orders: return True
            
            target_orders = orders
            if side == "BUY":
                target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '02']
            elif side == "SELL":
                target_orders = [o for o in orders if o.get('sll_buy_dvsn_cd') == '01']
                
            if not target_orders: return True
            
            for o in target_orders: 
                self.cancel_order(ticker, o.get('odno'))
            time.sleep(5)
            
        final_orders = self.get_unfilled_orders_detail(ticker)
        failed_orders = []
        
        if side == "BUY":
            failed_orders = [o for o in final_orders if o.get('sll_buy_dvsn_cd') == '02']
        elif side == "SELL":
            failed_orders = [o for o in final_orders if o.get('sll_buy_dvsn_cd') == '01']
        else:
            failed_orders = final_orders
            
        if failed_orders:
            failed_odnos = [o.get('odno') for o in failed_orders]
            error_msg = f"[FATAL ERROR] {ticker} 미체결 주문 취소 실패! (Double Spending 방어용 하드 락 발동). 미취소 ODNO: {failed_odnos}"
            print(f"🚨 {error_msg}")
            raise Exception(error_msg)
            
        return True

    def cancel_targeted_orders(self, ticker, side, target_ord_dvsn):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        
        target_orders = []
        for o in orders:
            dvsn = o.get('ord_dvsn_cd') or o.get('ord_dvsn') or ''
            if o.get('sll_buy_dvsn_cd') == sll_buy_cd and dvsn == target_ord_dvsn:
                target_orders.append(o)
                
        for o in target_orders:
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
            
        return len(target_orders)

    def cancel_orders_by_price(self, ticker, side, target_prices):
        sll_buy_cd = '02' if side == "BUY" else '01'
        orders = self.get_unfilled_orders_detail(ticker)
        if not orders: return 0
        
        target_orders = []
        for o in orders:
            if o.get('sll_buy_dvsn_cd') == sll_buy_cd:
                raw_p1 = o.get('ft_ord_unpr3', 0)
                raw_p2 = o.get('ord_unpr', 0)
                raw_p3 = o.get('ovrs_ord_unpr', 0)
                
                o_price = 0.0
                for rp in [raw_p1, raw_p2, raw_p3]:
                    try:
                        val = float(rp)
                        if val > 0:
                            o_price = val
                            break
                    except: pass

                for tp in target_prices:
                    if o_price > 0 and abs(o_price - tp) < 0.005: 
                        target_orders.append(o)
                        break
                        
        for o in target_orders:
            self.cancel_order(ticker, o.get('odno'))
            time.sleep(0.3)
            
        return len(target_orders)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT"):
        tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")

        if order_type == "LOC": ord_dvsn = "34"
        elif order_type == "MOC": ord_dvsn = "33"
        elif order_type == "LOO": ord_dvsn = "02"
        elif order_type == "MOO": ord_dvsn = "31"
        elif order_type == "AFTER_LIMIT": ord_dvsn = "34"  # 💡 [안전장치] 애프터마켓 지정가 지원
        else: ord_dvsn = "00"

        final_price = self._ceil_2(price)
        if order_type in ["MOC", "MOO"]: final_price = 0
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORD_QTY": str(int(qty)), "OVRS_ORD_UNPR": str(final_price),
            "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn 
        }
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
        
        rt_cd = res.get('rt_cd', '999')
        msg1 = res.get('msg1', '오류')
        output = res.get('output', {})
        odno = output.get('ODNO', '') if isinstance(output, dict) else ''
        
        return {'rt_cd': rt_cd, 'msg1': msg1, 'odno': odno}

    def cancel_order(self, ticker, order_id):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"
        }
        self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = self._get_exchange_code(ticker, target_api="ORDER")
        valid_execs = []
        seen_keys = set()
        fk200 = ""
        nk200 = ""
        
        for attempt in range(10): 
            params = {
                "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "PDNO": ticker,
                "ORD_STRT_DT": start_date, "ORD_END_DT": end_date, "SLL_BUY_DVSN": "00",      
                "CCLD_NCCS_DVSN": "00", "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS",
                "ORD_DT": "", "ORD_GNO_BRNO": "", "ODNO": "", "CTX_AREA_FK200": fk200, "CTX_AREA_NK200": nk200
            }
            
            headers = self._get_header("TTTS3035R")
            url = f"{self.base_url}/uapi/overseas-stock/v1/trading/inquire-ccnl"
            res, resp_json = self._api_request("GET", url, headers, params=params)
            
            if res and resp_json.get('rt_cd') == '0':
                output = resp_json.get('output', [])
                if isinstance(output, dict): output = [output] 
                for item in output:
                    if float(item.get('ft_ccld_qty', '0')) > 0:
                        unique_key = f"{item.get('odno')}_{item.get('ord_tmd')}_{item.get('ft_ccld_qty')}_{item.get('ft_ccld_unpr3')}"
                        if unique_key not in seen_keys:
                            seen_keys.add(unique_key)
                            valid_execs.append(item)
                        
                tr_cont = res.headers.get('tr_cont', '')
                fk200 = resp_json.get('ctx_area_fk200', '').strip()
                nk200 = resp_json.get('ctx_area_nk200', '').strip()
                
                if tr_cont in ['M', 'F'] and nk200:
                    time.sleep(0.3) 
                    continue
                else: break 
            else:
                error_msg = resp_json.get('msg1') if resp_json else "응답 없음"
                print(f"❌ [{ticker} 체결내역 오류] {error_msg}")
                break
        return valid_execs

    def get_genesis_ledger(self, ticker, limit_date_str=None):
        _, holdings = self.get_account_balance()
        if holdings is None: return None, 0, 0.0
            
        ticker_info = holdings.get(ticker, {'qty': 0, 'avg': 0.0})
        curr_qty = int(ticker_info.get('qty', 0))
        final_qty = curr_qty
        final_avg = float(ticker_info.get('avg', 0.0))
        
        if curr_qty == 0: return [], 0, 0.0
            
        ledger_records = []
        est = pytz.timezone('US/Eastern')
        target_date = datetime.datetime.now(est)
        genesis_reached = False
        loop_counter = 0 
        
        while curr_qty > 0 and not genesis_reached and loop_counter < 365:
            loop_counter += 1
            date_str = target_date.strftime('%Y%m%d')
            
            if limit_date_str and date_str < limit_date_str:
                break 
                
            execs = self.get_execution_history(ticker, date_str, date_str)
            
            if execs:
                execs.sort(key=lambda x: x.get('ord_tmd', '000000'), reverse=True)
                for ex in execs:
                    side_cd = ex.get('sll_buy_dvsn_cd')
                    exec_qty = int(float(ex.get('ft_ccld_qty', '0')))
                    exec_price = float(ex.get('ft_ccld_unpr3', '0'))
                    
                    record_qty = exec_qty
                    
                    if side_cd == "02": 
                        if curr_qty <= exec_qty: 
                            record_qty = curr_qty 
                            curr_qty = 0
                            genesis_reached = True
                        else:
                            curr_qty -= exec_qty
                    else: 
                        curr_qty += exec_qty
                    
                    ledger_records.append({
                        'date': f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}",
                        'side': "BUY" if side_cd == "02" else "SELL",
                        'qty': record_qty,
                        'price': exec_price
                    })
                    
                    if genesis_reached:
                        break
                        
            target_date -= datetime.timedelta(days=1)
            time.sleep(0.1) 
                
        ledger_records.reverse()
        return ledger_records, final_qty, final_avg

    def get_recent_stock_split(self, ticker, last_date_str):
        try:
            stock = yf.Ticker(ticker)
            splits = stock.splits
            if splits is not None and not splits.empty:
                
                if last_date_str == "":
                    est = pytz.timezone('US/Eastern')
                    seven_days_ago = datetime.datetime.now(est) - datetime.timedelta(days=7)
                    safe_last_date = seven_days_ago.strftime('%Y-%m-%d')
                else:
                    safe_last_date = last_date_str
                    
                for split_date_dt, ratio in splits.items():
                    split_date = split_date_dt.strftime('%Y-%m-%d')
                    if split_date > safe_last_date:
                        return float(ratio), split_date
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 액면분할 조회 에러: {e}")
        return 0.0, ""

    def get_dynamic_sniper_target(self, index_ticker):
        try:
            class TargetFloat(float):
                pass
            
            if index_ticker == "SOXX":
                hv_val, weight, target_drop, base_amp = ve.get_soxl_target_drop_full()
                ret = TargetFloat(target_drop)
                ret.metric_val = hv_val
                ret.weight = weight
                ret.base_amp = base_amp
                ret.metric_name = "SOXX HV"
                ret.metric_base = round(hv_val / weight, 2) if weight > 0 else 25.0
            else:
                vxn_val, weight, target_drop, base_amp = ve.get_tqqq_target_drop_full()
                ret = TargetFloat(target_drop)
                ret.metric_val = vxn_val
                ret.weight = weight
                ret.base_amp = base_amp
                ret.metric_name = "실시간 VXN"
                ret.metric_base = round(vxn_val / weight, 2) if weight > 0 else 20.0
            
            ret.is_panic = False
            ret.gap_pct = 0.0 
            
            return ret
            
        except Exception as e:
            print(f"⚠️ [Broker] V3.1 스나이퍼 타점 반환 실패 ({index_ticker}): {e}")
            fallback_val = -8.79 if index_ticker == "SOXX" else -4.95
            ret = TargetFloat(fallback_val)
            ret.metric_val = 0.0
            ret.weight = 1.0
            ret.base_amp = fallback_val
            ret.metric_name = "통신오류(기본값)"
            ret.metric_base = 25.0 if index_ticker == "SOXX" else 20.0
            ret.is_panic = False
            ret.gap_pct = 0.0
            return ret

    def get_day_high_low(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            # 💡 [수술] timeout=5 강제 주입
            hist = stock.history(period="1d", interval="1m", prepost=True, timeout=5)
            if not hist.empty:
                day_high = float(hist['High'].max())
                day_low = float(hist['Low'].min())
                return day_high, day_low
            else:
                return float(stock.fast_info.get('dayHigh', 0.0)), float(stock.fast_info.get('dayLow', 0.0))
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 고가/저가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = self._get_exchange_code(ticker, target_api="PRICE")
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker} 
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                out = res.get('output', {})
                return float(out.get('high', 0.0)), float(out.get('low', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 고가/저가 우회 조회 실패: {e}")
            
        return 0.0, 0.0

    def get_atr_data(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            # 💡 [수술] timeout=5 강제 주입
            hist = stock.history(period="30d", timeout=5)
            
            if hist.empty or len(hist) < 15:
                return 0.0, 0.0
                
            hist['Prev_Close'] = hist['Close'].shift(1)
            hist['TR'] = hist.apply(lambda row: max(
                row['High'] - row['Low'],
                abs(row['High'] - row['Prev_Close']) if not pd.isna(row['Prev_Close']) else 0,
                abs(row['Low'] - row['Prev_Close']) if not pd.isna(row['Prev_Close']) else 0
            ), axis=1)
            
            hist['ATR5'] = hist['TR'].rolling(window=5).mean()
            hist['ATR14'] = hist['TR'].rolling(window=14).mean()
            
            last_row = hist.iloc[-1]
            last_close = float(last_row['Close'])
            
            if last_close > 0:
                atr5_pct = (float(last_row['ATR5']) / last_close) * 100
                atr14_pct = (float(last_row['ATR14']) / last_close) * 100
                return round(atr5_pct, 1), round(atr14_pct, 1)
                
            return 0.0, 0.0
            
        except Exception as e:
            print(f"⚠️ [Broker] 실시간 ATR 연산 실패 ({ticker}): {e}")
            return 0.0, 0.0
