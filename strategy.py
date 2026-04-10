# ==========================================================
# [strategy.py] - 🌟 2대 코어 통합 완성본 🌟
# ⚠️ 이 주석 및 파일명 표기는 절대 지우지 마세요.
# 💡 [V24.15 대수술] V_VWAP 영구 소각 및 2대 코어(V14, V-REV) 체제 확립
# 💡 [핵심 수술] 폐기된 V_VWAP 감지 시 V14로 강제 오버라이딩(Auto-Upgrade)
# ==========================================================
import logging
import pandas as pd
from strategy_v14 import V14Strategy

class InfiniteStrategy:
    def __init__(self, config):
        self.cfg = config
        # 💡 오리지널 무매(V14) 및 클래식 리버스 로직이 캡슐화된 플러그인 인스턴스화
        self.v14_plugin = V14Strategy(config)

    # ==========================================================
    # 🛡️ VWAP 시장 미시구조 거래량 지배력 코어 엔진 (60% 상향)
    # (V-REV 전투 스케줄러와의 의존성 보존을 위해 라우터에 유지)
    # ==========================================================
    def analyze_vwap_dominance(self, df):
        if df is None or len(df) < 10:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
            
        try:
            if 'High' in df.columns and 'Low' in df.columns:
                typical_price = (df['High'] + df['Low'] + df['Close']) / 3.0
            else:
                typical_price = df['Close']
                
            vol_x_price = typical_price * df['Volume']
            total_vol = df['Volume'].sum()
            
            if total_vol == 0:
                return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}
                
            vwap_price = vol_x_price.sum() / total_vol
            
            df_temp = pd.DataFrame()
            df_temp['Volume'] = df['Volume']
            df_temp['Vol_x_Price'] = vol_x_price
            df_temp['Cum_Vol'] = df_temp['Volume'].cumsum()
            df_temp['Cum_Vol_Price'] = df_temp['Vol_x_Price'].cumsum()
            df_temp['Running_VWAP'] = df_temp['Cum_Vol_Price'] / df_temp['Cum_Vol']
            
            idx_10pct = int(len(df_temp) * 0.1)
            vwap_start = df_temp['Running_VWAP'].iloc[idx_10pct]
            vwap_end = df_temp['Running_VWAP'].iloc[-1]
            vwap_slope = vwap_end - vwap_start
            
            vol_above = df[df['Close'] > vwap_price]['Volume'].sum()
            vol_below = df[df['Close'] <= vwap_price]['Volume'].sum()
            
            vol_above_pct = vol_above / total_vol if total_vol > 0 else 0
            vol_below_pct = vol_below / total_vol if total_vol > 0 else 0
            
            daily_open = df['Open'].iloc[0] if 'Open' in df.columns else df['Close'].iloc[0]
            daily_close = df['Close'].iloc[-1]
            
            is_up_day = daily_close > daily_open
            is_down_day = daily_close < daily_open
            
            is_strong_up = is_up_day and (vwap_slope > 0) and (vol_above_pct > 0.60)
            is_strong_down = is_down_day and (vwap_slope < 0) and (vol_below_pct > 0.60)
            
            return {
                "vwap_price": round(vwap_price, 2),
                "is_strong_up": bool(is_strong_up),
                "is_strong_down": bool(is_strong_down),
                "vol_above_pct": round(vol_above_pct, 4),
                "vwap_slope": round(vwap_slope, 4)
            }
        except Exception as e:
            return {"vwap_price": 0.0, "is_strong_up": False, "is_strong_down": False}

    # ==========================================================
    # 🎯 중앙 라우팅 엔진 (Dynamic Routing)
    # ==========================================================
    def get_plan(self, ticker, current_price, avg_price, qty, prev_close, ma_5day=0.0, market_type="REG", available_cash=0, is_simulation=False, vwap_status=None):
        """
        [중앙 라우터]
        모든 종목의 통합 지시서(/sync) 및 정규장 17:05 선제적 주문서(Plan) 생성을 
        strategy_v14.py 플러그인으로 위임합니다.
        """
        version = self.cfg.get_version(ticker)
        
        # 🚨 영구 소각된 레거시 모드 감지 시 오리지널 V14 엔진으로 강제 오버라이딩(Auto-Upgrade)
        if version in ["V13", "V17", "V_VWAP"]:
            logging.warning(f"[{ticker}] 폐기된 레거시 모드({version}) 감지. V14 엔진으로 강제 라우팅합니다.")
            self.cfg.set_version(ticker, "V14")
            version = "V14"

        # 💡 V14, V_REV 모두 17:05 기본 지시서(Fail-Safe LOC 등)는 
        # 오리지널 V14 엔진의 뼈대를 공유하므로 v14_plugin 으로 통합 라우팅합니다.
        return self.v14_plugin.get_plan(
            ticker=ticker,
            current_price=current_price,
            avg_price=avg_price,
            qty=qty,
            prev_close=prev_close,
            ma_5day=ma_5day,
            market_type=market_type,
            available_cash=available_cash,
            is_simulation=is_simulation,
            vwap_status=vwap_status
        )
