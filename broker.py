# ==========================================================
# [broker.py]
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# ==========================================================
import requests
import json
import time
import datetime
import os
import math
import yfinance as yf

class KoreaInvestmentBroker:
    def __init__(self, app_key, app_secret, cano, acnt_prdt_cd="01"):
        self.app_key = app_key
        self.app_secret = app_secret
        self.cano = cano
        self.acnt_prdt_cd = acnt_prdt_cd
        self.base_url = "https://openapi.koreainvestment.com:9443"
        self.token_file = f"data/token_{cano}.dat" 
        self.token = None
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
            except: pass

        if force and os.path.exists(self.token_file):
            try: os.remove(self.token_file)
            except: pass

        url = f"{self.base_url}/oauth2/tokenP"
        body = {"grant_type": "client_credentials", "appkey": self.app_key, "appsecret": self.app_secret}
        
        try:
            res = requests.post(url, headers={"content-type": "application/json"}, data=json.dumps(body))
            data = res.json()
            if 'access_token' in data:
                self.token = data['access_token']
                expire_str = (datetime.datetime.now() + datetime.timedelta(seconds=int(data['expires_in']))).strftime('%Y-%m-%d %H:%M:%S')
                
                with open(self.token_file, 'w') as f:
                    json.dump({'token': self.token, 'expire': expire_str}, f)
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
                    res = requests.get(url, headers=headers, params=params)
                else:
                    res = requests.post(url, headers=headers, data=json.dumps(data) if data else None)
                    
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
        except: return 0.0

    def get_account_balance(self):
        cash = 0.0
        holdings = None 
        
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "WCRC_FRCR_DVSN_CD": "02", "NATN_CD": "840", "TR_MKET_CD": "00", "INQR_DVSN_CD": "00"}
        res = self._call_api("CTRP6504R", "/uapi/overseas-stock/v1/trading/inquire-present-balance", "GET", params=params)
        
        if res.get('rt_cd') == '0':
            o2 = res.get('output2', {})
            if isinstance(o2, list) and len(o2) > 0: o2 = o2[0]
            
            dncl_amt = self._safe_float(o2.get('frcr_dncl_amt_2', 0))       
            sll_amt = self._safe_float(o2.get('frcr_sll_amt_smtl', 0))      
            buy_amt = self._safe_float(o2.get('frcr_buy_amt_smtl', 0))      
            
            raw_bp = dncl_amt + sll_amt - buy_amt
            cash = math.floor((raw_bp * 0.9945) * 100) / 100.0              

        params_hold = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": "NASD", "TR_CRCY_CD": "USD", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
        res = self._call_api("TTTS3012R", "/uapi/overseas-stock/v1/trading/inquire-balance", "GET", params_hold)
        
        if res.get('rt_cd') == '0':
            holdings = {} 
            if cash <= 0:
                o2 = res.get('output2', {})
                if isinstance(o2, list) and len(o2) > 0: o2 = o2[0]
                cash = self._safe_float(o2.get('ovrs_ord_psbl_amt', 0))
            
            for item in res.get('output1', []):
                ticker = item.get('ovrs_pdno')
                qty = int(self._safe_float(item.get('ovrs_cblc_qty', 0)))
                avg = self._safe_float(item.get('pchs_avg_pric', 0))
                if qty > 0: holdings[ticker] = {'qty': qty, 'avg': avg}
        
        return cash, holdings

    def get_current_price(self, ticker, is_market_closed=False):
        try:
            stock = yf.Ticker(ticker)
            if is_market_closed: return float(stock.fast_info['last_price'])
            hist = stock.history(period="1d", interval="1m", prepost=True)
            if not hist.empty: return float(hist['Close'].iloc[-1])
            else: return float(stock.fast_info['last_price'])
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 현재가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = "AMS" if ticker == "SOXL" else "NAS"
            params = {"AUTH": "", "EXCD": excg_cd, "SYMB": ticker}
            res = self._call_api("HHDFS76200200", "/uapi/overseas-price/v1/quotations/price", "GET", params=params)
            if res.get('rt_cd') == '0':
                return float(res.get('output', {}).get('last', 0.0))
        except Exception as e:
            print(f"❌ [한투 API] 현재가 우회 조회 실패: {e}")
        return 0.0

    def get_previous_close(self, ticker):
        try: return float(yf.Ticker(ticker).fast_info['previous_close'])
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] 전일종가 에러, 한투 API 우회 가동: {e}")

        try:
            excg_cd = "AMS" if ticker == "SOXL" else "NAS"
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
            hist = stock.history(period="10d") 
            if len(hist) >= 5: return float(hist['Close'][-5:].mean())
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] MA5 에러, 한투 API 우회 가동: {e}")
            
        try:
            excg_cd = "AMS" if ticker == "SOXL" else "NAS"
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

    def get_bb_lower(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="20d") 
            if len(hist) >= 20: 
                ma20 = float(hist['Close'].mean())
                std20 = float(hist['Close'].std())
                return float(ma20 - (std20 * 2))
        except Exception as e:
            print(f"⚠️ [야후 파이낸스] BB 하한선 에러, 한투 API 우회 가동: {e}")
            
        try:
            excg_cd = "AMS" if ticker == "SOXL" else "NAS"
            params = {
                "AUTH": "", "EXCD": excg_cd, "SYMB": ticker,
                "GUBN": "0", "BYMD": "", "MODP": "1"
            }
            res = self._call_api("HHDFS76240000", "/uapi/overseas-price/v1/quotations/dailyprice", "GET", params=params)
            if res.get('rt_cd') == '0':
                output2 = res.get('output2', [])
                if isinstance(output2, list) and len(output2) >= 20:
                    closes = [float(x['clos']) for x in output2[:20]]
                    ma20 = sum(closes) / 20.0
                    variance = sum([((x - ma20) ** 2) for x in closes]) / 19.0
                    std20 = math.sqrt(variance)
                    return float(ma20 - (std20 * 2))
        except Exception as e:
            print(f"❌ [한투 API] BB 하한선 우회 조회 실패: {e}")
            
        return 0.0

    def get_unfilled_orders(self, ticker):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
        params = {"CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd, "SORT_SQN": "DS", "CTX_AREA_FK200": "", "CTX_AREA_NK200": ""}
        res = self._call_api("TTTS3018R", "/uapi/overseas-stock/v1/trading/inquire-nccs", "GET", params=params)
        if res.get('rt_cd') == '0':
            output = res.get('output', [])
            if isinstance(output, dict): output = [output]
            return [item.get('odno') for item in output if item.get('pdno') == ticker]
        return []

    def get_unfilled_orders_detail(self, ticker):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
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
        if side == "BUY":
            return not any(o.get('sll_buy_dvsn_cd') == '02' for o in final_orders)
        elif side == "SELL":
            return not any(o.get('sll_buy_dvsn_cd') == '01' for o in final_orders)
        return not bool(final_orders)

    def send_order(self, ticker, side, qty, price, order_type="LIMIT"):
        tr_id = "TTTT1002U" if side == "BUY" else "TTTT1006U"
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"

        if order_type == "LOC": ord_dvsn = "34"
        elif order_type == "MOC": ord_dvsn = "33"
        elif order_type == "LOO": ord_dvsn = "02"
        elif order_type == "MOO": ord_dvsn = "31"
        else: ord_dvsn = "00"

        final_price = self._ceil_2(price)
        if order_type in ["MOC", "MOO"]: final_price = 0
        
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORD_QTY": str(int(qty)), "OVRS_ORD_UNPR": str(final_price),
            "ORD_SVR_DVSN_CD": "0", "ORD_DVSN": ord_dvsn 
        }
        res = self._call_api(tr_id, "/uapi/overseas-stock/v1/trading/order", "POST", body=body)
        return {'rt_cd': res.get('rt_cd'), 'msg1': res.get('msg1')}

    def cancel_order(self, ticker, order_id):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
        body = {
            "CANO": self.cano, "ACNT_PRDT_CD": self.acnt_prdt_cd, "OVRS_EXCG_CD": excg_cd,
            "PDNO": ticker, "ORGN_ODNO": order_id, "RVSE_CNCL_DVSN_CD": "02",
            "ORD_QTY": "0", "OVRS_ORD_UNPR": "0", "ORD_SVR_DVSN_CD": "0"
        }
        self._call_api("TTTT1004U", "/uapi/overseas-stock/v1/trading/order-rvsecncl", "POST", body=body)

    def get_execution_history(self, ticker, start_date, end_date):
        excg_cd = "AMEX" if ticker == "SOXL" else "NASD"
        valid_execs = []
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
        target_date = datetime.datetime.now()
        genesis_reached = False
        
        while curr_qty > 0 and not genesis_reached:
            date_str = target_date.strftime('%Y%m%d')
            
            if limit_date_str and date_str < limit_date_str:
                return "CIRCUIT_BREAKER", final_qty, final_avg
                
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
            if (datetime.datetime.now() - target_date).days > 365: break 
                
        ledger_records.reverse()
        return ledger_records, final_qty, final_avg
