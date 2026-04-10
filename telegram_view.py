# ==========================================================
# [telegram_view.py] - Part 1/2 부 (상반부)
# ⚠️ V-REV 장부 강제 초기화 3중 경고 방어막 유지
# 💡 [V24.15 대수술] 2대 코어(V14, V-REV) 체제 UI 최적화 및 V_VWAP 적출
# ==========================================================
import os
import math
import json
import logging
from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramView:
    def __init__(self):
        # 💡 리눅스 서버(Ubuntu 등) 폰트 파일 시스템 전방위 추적 경로 확장
        self.bold_font_paths = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
            "arialbd.ttf"
        ]
        self.reg_font_paths = [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
            "/usr/share/fonts/dejavu/DejaVuSans.ttf",
            "arial.ttf"
        ]

    def _load_best_font(self, font_list, size):
        for path in font_list:
            try:
                if os.path.exists(path):
                    return ImageFont.truetype(path, size)
            except:
                continue
        try:
            return ImageFont.truetype("sans-serif", size)
        except:
            return ImageFont.load_default()

    def get_start_message(self, target_hour, season_icon, latest_version):
        init_time = f"{target_hour:02d}:00"
        order_time = f"{target_hour:02d}:05"
        season_short = "🌞서머타임 ON" if "Summer" in season_icon else "❄️서머타임 OFF"
        sync_time = "08:30" if target_hour == 17 else "09:30"

        return (
            f"🌌 <b>[ 인피니트 스노우볼 {latest_version} ]</b>\n" 
            f"💠 <b> V-REV 역추세 & V14 무매 퀀트 코어 </b>\n\n" 
            f"🕒 <b>[ 운영 스케줄 ({season_short}) ]</b>\n"
            f"🔹 6시간 간격 : 🔑 API 토큰 자동 갱신\n"
            f"🔹 {sync_time} : 📝 잔고 동기화 & 자동 복리\n"
            f"🔹 {init_time} : 🔐 매매 초기화 및 변동성 락온\n"
            f"🔹 {order_time} : 🌃 통합 주문 자동 실행\n\n"
            "🛠 <b>[ 주요 명령어 ]</b>\n"
            "▶️ <b>/sync</b> : 📜 통합 지시서 조회\n"
            "▶️ <b>/record</b> : 📊 장부 동기화 및 조회\n"
            "▶️ <b>/history</b> : 🏆 졸업 명예의 전당\n"
            "▶️ <b>/settlement</b> : ⚙️ 분할/복리/액면 설정\n"
            "▶️ <b>/seed</b> : 💵 개별 시드머니 관리\n"
            "▶️ <b>/ticker</b> : 🔄 운용 종목 선택\n"
            "▶️ <b>/mode</b> : 🎯 상방 스나이퍼 ON/OFF\n"
            "▶️ <b>/version</b> : 🛠️ 버전 및 업데이트 내역\n\n" 
            "⚠️ <b>/reset</b> : 🔓 비상 해제 메뉴 (락/리버스)\n" 
            "<i>┗ 🚨 수동 닻 올리기: 예산 부족으로 리버스 진입 후 외화RP매도 등 예수금을 추가 입금하셨다면, 이 메뉴에서 반드시 '리버스 강제 해제'를 눌러 닻을 올려주세요!</i>"
        )

    def get_reset_menu(self, active_tickers):
        msg = (
            "🛠️ <b>[ 시스템 안전 통제실 ]</b>\n"
            "⚠️ 주의: 강제 초기화할 항목을 선택하세요."
        )
        keyboard = []
        for t in active_tickers:
            keyboard.append([InlineKeyboardButton(f"🔓 [{t}] 매매 잠금 해제", callback_data=f"RESET:LOCK:{t}")])
        for t in active_tickers:
            keyboard.append([InlineKeyboardButton(f"🚨 [{t}] 리버스/장부 초기화", callback_data=f"RESET:REV:{t}")])
        keyboard.append([InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")])
        return msg, InlineKeyboardMarkup(keyboard)

    def get_reset_confirm_menu(self, ticker):
        msg = (
            f"⚠️ <b>[ 경고: {ticker} 리버스 및 가상장부 강제 초기화 ]</b>\n\n"
            f"정말로 {ticker}의 리버스 모드를 종료하고 <b>가상장부(Escrow) 격리금액을 0원으로 완전 소각</b>하시겠습니까?\n"
            "<i>(충분한 시드가 추가되었거나 로직 꼬임 시에만 권장하며, 해제 시 다음부터 일반 모드로 돌아갑니다.)</i>"
        )
        keyboard = [
            [InlineKeyboardButton("✅ 네, 모두 초기화합니다", callback_data=f"RESET:CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 유지합니다", callback_data="RESET:MENU")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_init_v_rev_confirm_menu(self, ticker):
        msg = (
            f"🛑 <b>[ 초긴급: {ticker} V-REV 장부 강제 초기화 ]</b>\n\n"
            "이 버튼은 현재 쌓여있는 모든 정밀 LIFO 지층을 파괴하고 하나로 합칩니다. "
            "실행 전 다음 <b>3가지 위험 요소</b>를 반드시 확인하세요:\n\n"
            "1️⃣ <b>지층 붕괴:</b> 분할 매수된 모든 개별 로트가 하나의 거대 블록으로 강제 압축됩니다.\n"
            "2️⃣ <b>전술 상실:</b> 최근 물량의 '전일 종가 익절' 타점이 소각되며, 전체 평단가 돌파 전까지 매도가 잠깁니다.\n"
            "3️⃣ <b>용도 제한:</b> 수동 매매로 수량이 틀어졌거나, 최초 이관 시에만 사용하는 최후의 수단입니다.\n\n"
            "⚠️ <b>정말로 모든 전략 지층을 삭제하고 초기화하시겠습니까?</b>"
        )
        keyboard = [
            [InlineKeyboardButton("🔥 예, 위험을 인지하고 초기화합니다", callback_data=f"SET_INIT:EXEC_CONFIRM:{ticker}")],
            [InlineKeyboardButton("❌ 아니오, 기존 지층을 유지합니다", callback_data="SETTLEMENT:BACK")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)

    def get_version_message(self, history_data, page_index=None):
        if not history_data:
            return "📭 기록된 버전 히스토리가 없습니다.", None

        items_per_page = 5
        total_items = len(history_data)
        total_pages = math.ceil(total_items / items_per_page)

        if page_index is None:
            page_index = 0
        else:
            page_index = max(0, min(page_index, total_pages - 1))

        end_idx = total_items - (page_index * items_per_page)
        start_idx = max(0, end_idx - items_per_page)
        items = history_data[start_idx:end_idx]

        if page_index == 0:
            title = "🛠️ <b>[ 최신 업데이트 내역 ]</b>\n\n"
        else:
            title = f"📚 <b>[ 과거 업데이트 내역 (Page {page_index + 1}/{total_pages}) ]</b>\n\n"

        msg = title
        for h in items:
            if isinstance(h, str):
                parts = h.split(' ', 2)
                if len(parts) >= 3:
                    ver = parts[0]
                    date = parts[1]
                    summary = parts[2]
                    msg += f"📌 <b>{ver}</b> {date}\n▫️ {summary}\n\n"
                else:
                    msg += f"📌 {h}\n\n"
            elif isinstance(h, dict): 
                msg += f"📌 <b>{h.get('version', '')}</b> ({h.get('date', '')})\n▫️ {h.get('summary', '')}\n\n"
        
        msg = msg.strip()
        keyboard = []
        
        nav_row = []
        if page_index < total_pages - 1:
            nav_row.append(InlineKeyboardButton("◀️ 과거 기록", callback_data=f"VERSION:PAGE:{page_index + 1}"))
        if page_index > 0:
            nav_row.append(InlineKeyboardButton("최신 기록 ▶️", callback_data=f"VERSION:PAGE:{page_index - 1}"))
            
        if nav_row:
            keyboard.append(nav_row)
            
        if page_index > 0:
            keyboard.append([InlineKeyboardButton("⬆️ 접기 (최신 버전만 보기)", callback_data="VERSION:LATEST")])
            
        return msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_queue_management_menu(self, ticker, q_data):
        msg = f"🗄️ <b>[ {ticker} V-REV 큐(Queue) 정밀 타격 통제소 ]</b>\n\n"
        msg += "▫️ 현재 장부에 적재된 날짜별 지층입니다.\n"
        msg += "▫️ 🛡️ <b>표기된 지층만 수정/삭제가 가능합니다.</b>\n"
        msg += "▫️ 수동 추가: <code>/add_q 종목명 YYYY-MM-DD 수량 평단가</code>\n\n"

        keyboard = []
        if not q_data:
            msg += "텅 비어 있습니다. (0주)\n"
        else:
            sorted_q = sorted(q_data, key=lambda x: x.get('date', ''), reverse=True)
            for i, item in enumerate(sorted_q):
                floor = i + 1
                date_str = item.get('date', 'Unknown')
                short_date = date_str[2:10] if len(date_str) >= 10 else date_str 
                qty = item.get('qty', 0)
                price = item.get('price', 0.0)
                lot_type = item.get('type', '')

                if lot_type == "INIT_TRANSFERRED":
                    btn_text = f"[{floor}] {short_date} | {qty}주 | ${price:.2f} 🔒(보호)"
                    callback_data = "IGNORE"
                else:
                    btn_text = f"[{floor}] {short_date} | {qty}주 | ${price:.2f} ❌"
                    callback_data = f"DEL_REQ:{ticker}:{date_str}"
                
                keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("🔙 대시보드로 돌아가기", callback_data=f"REC:SYNC:{ticker}")])
        return msg, InlineKeyboardMarkup(keyboard)

    def get_queue_action_confirm_menu(self, ticker, target_date, qty, price):
        short_date = target_date[:10] if len(target_date) >= 10 else target_date
        msg = f"⚠️ <b>[ {ticker} 지층 안전 통제망 ]</b>\n\n"
        msg += f"선택하신 <b>[{short_date}]</b> 지층을 제어합니다:\n"
        msg += f"▫️ 현재 수량: <b>{qty}주</b>\n"
        msg += f"▫️ 현재 평단: <b>${price:.2f}</b>\n\n"
        msg += "원하시는 작업을 선택해 주십시오.\n"
        msg += "<i>(✏️ 수정을 누르시면 봇이 새 값을 입력받기 위해 대기합니다.)</i>"

        keyboard = [
            [InlineKeyboardButton("✏️ 지층 정보 수정하기", callback_data=f"EDIT_Q:{ticker}:{target_date}")],
            [InlineKeyboardButton("🗑️ 영구 삭제 (복구 불가)", callback_data=f"DEL_Q:{ticker}:{target_date}")],
            [InlineKeyboardButton("🔙 취소 및 목록으로", callback_data=f"QUEUE:VIEW:{ticker}")]
        ]
        return msg, InlineKeyboardMarkup(keyboard)
# ==========================================================
# [telegram_view.py] - Part 2/2 부 (하반부)
# ⚠️ V-REV 설정줄 숨김 및 종목 간 띄어쓰기(엔터) 간격 완벽 교정
# 💡 [V24.15 대수술] 2대 코어(V14, V-REV) 체제 UI 최적화 및 V_VWAP 적출
# ==========================================================

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active, p_trade_data=None):
        # 💡 [핵심 수술] LOC 주문이 아직 들어가지 않은(is_locked가 False인) 종목의 필수 예산만 합산
        total_required_budget = sum(
            t_info.get('one_portion', 0.0) 
            for t_info in ticker_data 
            if not t_info.get('is_locked', False)
        )
        
        # 💡 [핵심 수술] KIS 주문가능금액(cash)에서 미주문 필수 예산만 선제적 차감 (이중 차감 방어)
        dynamic_rp_amount = max(0.0, cash - total_required_budget)
        
        total_locked = sum(t_info.get('escrow', 0.0) for t_info in ticker_data)
        
        header_msg = f"📜 <b>[ 통합 지시서 ({status_text}) ]</b>\n📅 <b>{dst_text}</b>\n"
        
        if total_locked > 0:
            real_cash = max(0, cash - total_locked)
            header_msg += f"💵 한투 전체 잔고: ${cash:,.2f}\n"
            header_msg += f"🔒 에스크로 격리금: -${total_locked:,.2f}\n"
            header_msg += f"✅ 실질 가용 예산: ${real_cash:,.2f}\n"
        else:
            header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
            
        header_msg += f"🏛️ RP 투자권장: ${dynamic_rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        body_msg = ""
        keyboard = []

        for t_info in ticker_data:
            t = t_info['ticker']
            v_mode = t_info['version']
            
            if t_info.get('t_val', 0.0) > (t_info.get('split', 40.0) * 1.1):
                body_msg += f"⚠️ <b>[🚨 시스템 긴급 경고: 비정상 T값 폭주 감지!]</b>\n"
                body_msg += f"🔎 현재 T값(<b>{t_info['t_val']:.4f}T</b>)이 설정된 분할수(<b>{int(t_info['split'])}분할</b>) 초과했습니다!\n"
                body_msg += f"💡 <b>원인 역산 추정:</b> 수동 매수로 수량이 급증했거나, '/seed' 시드머니 설정이 대폭 축소되었습니다.\n"
                body_msg += f"🛡️ <b>가동 조치:</b> 마이너스 호가 차단용 절대 하한선($0.01) 방어막 가동 중!\n\n"

            # 💡 [다이어트 완료] 오직 V-REV와 V14(디폴트) 2대 코어만 판별
            if v_mode == "V_REV":
                v_mode_display = "V_REV 역추세"
                main_icon = "⚖️"
            else:
                v_mode_display = "무매4"
                main_icon = "💎"
                
            is_rev = t_info.get('is_reverse', False)
            proc_status = t_info.get('plan', {}).get('process_status', '')
            tracking_info = t_info.get('tracking_info', {})
            
            if proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"⚠️ <b>[🚨 비상 상황: {t} 긴급 수혈 중]</b>\n"
                body_msg += f"❗ <i>에스크로 금고가 바닥나 강제 매도를 통해 현금을 생성합니다.</i>\n\n"
            
            if is_rev:
                bdg_txt = f"리버스 잔금쿼터: ${t_info['one_portion']:,.0f}"
                icon = "🩸" if proc_status == "🩸리버스(긴급수혈)" else "🔄"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            elif v_mode == "V_REV":
                bdg_txt = f"1회(1배수) 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 큐(Queue): <b>{t_info.get('v_rev_q_lots', 0)}개 로트 대기 중 (총 {t_info.get('v_rev_q_qty', 0)}주)</b>\n"
            else:
                bdg_txt = f"당일 예산: ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] {v_mode_display}</b>\n"
                body_msg += f"📈 진행: <b>{t_info['t_val']:.4f}T / {int(t_info['split'])}분할</b>\n"
            
            body_msg += f"💵 총 시드: ${t_info['seed']:,.0f}\n"
            body_msg += f"🛒 <b>{bdg_txt}</b>\n"
            
            escrow = t_info.get('escrow', 0.0)
            if escrow > 0:
                body_msg += f"🔐 내 금고 보호액: ${escrow:,.2f}\n"
            elif is_rev and proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"🔐 내 금고 보호액: $0.00 (Empty 🚨)\n"
                
            body_msg += f"💰 현재 ${t_info['curr']:,.2f} / 평단 ${t_info['avg']:,.2f} ({t_info['qty']}주)\n"
            
            day_high = t_info.get('day_high', 0.0)
            day_low = t_info.get('day_low', 0.0)
            prev_close = t_info.get('prev_close', 0.0)
            
            if prev_close > 0 and day_high > 0 and day_low > 0:
                high_pct = (day_high - prev_close) / prev_close * 100
                low_pct = (day_low - prev_close) / prev_close * 100
                high_sign = "+" if high_pct > 0 else ""
                low_sign = "+" if low_pct > 0 else ""
                body_msg += f"📈 금일 고가: ${day_high:.2f} ({high_sign}{high_pct:.2f}%)\n"
                body_msg += f"📉 금일 저가: ${day_low:.2f} ({low_sign}{low_pct:.2f}%)\n"

            sign = "+" if t_info['profit_amt'] >= 0 else "-"
            icon = "🔺" if t_info['profit_amt'] >= 0 else "🔻"
            body_msg += f"{icon} 수익: {sign}{abs(t_info['profit_pct']):.2f}% ({sign}${abs(t_info['profit_amt']):,.2f})\n"
            
            sniper_status_txt = t_info.get('upward_sniper', 'OFF')
            
            if v_mode != "V_REV":
                if is_rev:
                    body_msg += f"⚙️ 🌟 5일선 별지점: ${t_info['star_price']:.2f} | 🎯감시: {sniper_status_txt}\n"
                else:
                    body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}% | 🎯감시: {sniper_status_txt}\n"
                    
                if sniper_status_txt == "ON":
                    if not is_trade_active:
                        body_msg += f"🎯 상방 스나이퍼: 감시 종료 (장마감)\n"
                    elif tracking_info.get('is_trailing', False):
                        peak_price = tracking_info.get('peak_price', 0.0)
                        trigger_price = tracking_info.get('trigger_price', 0.0)
                        body_msg += f"🎯 상방 추적(${trigger_price:.2f}) 중 (고가: ${peak_price:.2f})\n"
                    else:
                        if is_rev:
                            sn_target = t_info['star_price']
                        else:
                            safe_floor = math.ceil(t_info['avg'] * 1.005 * 100) / 100.0
                            sn_target = max(t_info['star_price'], safe_floor)
                            
                        if sn_target > 0:
                            body_msg += f"🎯 상방 스나이퍼: ${sn_target:.2f} 이상 대기\n"
            
            # 💡 [다이어트 완료] V_VWAP 표출 텍스트 소각
            elif v_mode == "V_REV":
                body_msg += f"⚖️ <b>역추세 LIFO 큐(Queue) 엔진 스탠바이</b>\n"
                body_msg += f"⏱️ <b>VWAP 스케줄:</b> 15:30 EST 앵커 세팅 ➔ 1분 단위 교차 타격\n"
            
            if v_mode == "V_REV":
                body_msg += f"📋 <b>[주문 가이던스 - ⚖️다중 LIFO 제어]</b>\n"
                
                qty = t_info.get('qty', 0)
                alloc_cash = t_info.get('one_portion', 0.0)
                prev_c = t_info.get('prev_close', 0.0)
                
                if qty == 0 and alloc_cash > 0 and prev_c > 0:
                    p1_trigger = round(prev_c * 1.15, 2)
                    p2_trigger = round(prev_c * 0.975, 2)
                    b1_budget = alloc_cash * 0.5
                    b2_budget = alloc_cash * 0.5
                    
                    q1 = math.floor(b1_budget / p1_trigger) if p1_trigger > 0 else 0
                    q2 = math.floor(b2_budget / p2_trigger) if p2_trigger > 0 else 0
                    
                    body_msg += f" 🔵 매도(Pop): 대기 물량 없음 (관망)\n"
                    body_msg += f" 🔴 매수1(Buy1): <b>${p1_trigger:.2f}</b> 진입 시 {q1}주\n"
                    body_msg += f" 🔴 매수2(Buy2): <b>${p2_trigger:.2f}</b> 진입 시 {q2}주\n"
                else:
                    raw_guidance = t_info.get('v_rev_guidance', " (가이던스 대기 중)")
                    raw_guidance = raw_guidance.rstrip('\n')
                    body_msg += raw_guidance + "\n"
                
                if alloc_cash > 0 and prev_c > 0:
                    p2_trigger = round(prev_c * 0.975, 2)
                    if p2_trigger > 0:
                        b2_budget = alloc_cash * 0.5
                        q2 = math.floor(b2_budget / p2_trigger)
                        
                        grid_start = round(b2_budget / (q2 + 1), 2)
                        grid_end = round(b2_budget / (q2 + 5), 2)
                        
                        if grid_start >= 0.01 and grid_start < p2_trigger:
                            grid_end = max(grid_end, 0.01)
                            body_msg += f" 🧹 줍줍(5개): <b>${grid_start:.2f} ~ ${grid_end:.2f} (LOC)</b>\n"
            else:
                body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
                plan_orders = t_info.get('plan', {}).get('orders', [])
                if plan_orders:
                    jup_orders = [o for o in plan_orders if "줍줍" in o['desc']]
                    n_orders = [o for o in plan_orders if "줍줍" not in o['desc']]
    
                    for o in n_orders:
                        ico = "🔴" if o['side'] == 'BUY' else "🔵"
                        desc = o['desc']
                        
                        if "수혈" in desc: 
                            ico = "🩸"
                            desc = desc.replace("🩸", "")
                            
                        type_str = "" if o['type'] == 'LIMIT' else f"({o['type']})"
                        type_disp = f" {type_str}" if type_str else ""
                        
                        body_msg += f" {ico} {desc}: <b>${o['price']} x {o['qty']}주</b>{type_disp}\n"
    
                    if jup_orders:
                        prices = sorted([o['price'] for o in jup_orders], reverse=True)
                        body_msg += f" 🧹 줍줍({len(jup_orders)}개): <b>${prices[0]} ~ ${prices[-1]} (LOC)</b>\n"
                    
                    if is_trade_active:
                        if t_info.get('is_locked', False): body_msg += f" (✅ 금일 주문 완료/잠금)\n"
                        else: keyboard.append([InlineKeyboardButton(f"🚀 {t} 주문 실행", callback_data=f"EXEC:{t}")])
                else:
                    body_msg += f" 💤 주문 없음 (관망/예산소진)\n"
                
            body_msg += "\n"

        final_msg = header_msg + body_msg

        vol_summaries = []
        for t_info in ticker_data:
            if 'vol_weight' in t_info and 'vol_status' in t_info:
                vol_summaries.append(f"{t_info['ticker']}: {t_info['vol_weight']} ({t_info['vol_status']})")
        
        if vol_summaries:
            final_msg += "📊 <b>[자율지표]</b> " + " | ".join(vol_summaries) + "\n<i>(상세: /mode)</i>\n\n"

        if not is_trade_active: final_msg += "⛔ 장마감/애프터마켓: 주문 불가"
        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

    def get_settlement_message(self, active_tickers, config, atr_data, dynamic_target_data=None):
        msg = "⚙️ <b>[ 현재 설정 및 복리 상태 ]</b>\n\n"
        keyboard = []
        
        for t in active_tickers:
            ver = config.get_version(t)
            
            # 💡 [다이어트 완료] V14, V-REV 2대 코어만 판별
            if ver == "V_REV":
                icon = "⚖️"
                ver_display = "V_REV 역추세"
            else:
                icon = "💎"
                ver_display = "무매4"
                
            split_cnt = int(config.get_split_count(t))
            target_pct = config.get_target_profit(t)
            comp_rate = config.get_compound_rate(t)
            
            msg += f"{icon} <b>{t} ({ver_display} 모드)</b>\n"
            msg += f"▫️ 분할: {split_cnt}회\n▫️ 목표: {target_pct}%\n▫️ 자동복리: {comp_rate}%\n"
            
            if ver == "V_REV":
                msg += f"⚖️ <b>역추세(Reversion) 하이브리드 엔진 스탠바이:</b>\n"
                msg += f"▫️ 전일 종가 앵커 기준 LIFO 큐 교차 매매 대기 중\n\n"
                row_init = [InlineKeyboardButton(f"🔌 {t} V-REV 큐 장부 초기화 (물량이관)", callback_data=f"SET_INIT:V_REV:{t}")]
                keyboard.append(row_init)
            else:
                msg += "\n"
                
            # 💡 [다이어트 완료] V14 무매4 전환 버튼 단일 유지
            row1 = [
                InlineKeyboardButton("💎 V14 (무매4)", callback_data=f"SET_VER:V14:{t}")
            ]
            keyboard.append(row1)
            
            row2 = [
                InlineKeyboardButton(f"⚙️ {t} 분할", callback_data=f"INPUT:SPLIT:{t}"), 
                InlineKeyboardButton(f"🎯 {t} 목표", callback_data=f"INPUT:TARGET:{t}"),
                InlineKeyboardButton(f"💸 {t} 복리", callback_data=f"INPUT:COMPOUND:{t}")
            ]
            keyboard.append(row2)
            
            row3 = [
                InlineKeyboardButton(f"✂️ {t} 액면보정", callback_data=f"INPUT:STOCK_SPLIT:{t}")
            ]
            keyboard.append(row3)
            
        return msg, InlineKeyboardMarkup(keyboard)

    def create_ledger_dashboard(self, ticker, qty, avg, invested, sold, records, t_val, split, is_history=False, is_reverse=False):
        groups = {}
        for r in records:
            key = (r['date'], r['side'])
            if key not in groups: groups[key] = {'sum_qty': 0, 'sum_cost': 0}
            groups[key]['sum_qty'] += r['qty']
            groups[key]['sum_cost'] += (r['qty'] * r['price'])

        agg_list = []
        for (date, side), data in groups.items():
            if data['sum_qty'] > 0:
                avg_p = data['sum_cost'] / data['sum_qty']
                agg_list.append({'date': date, 'side': side, 'qty': data['sum_qty'], 'avg': avg_p})

        agg_list.sort(key=lambda x: x['date'])
        for i, item in enumerate(agg_list): item['no'] = i + 1
        agg_list.reverse()

        title = "과거 졸업 기록" if is_history else "일자별 매매 (통합 변동분)"
        msg = f"📜 <b>[ {ticker} {title} (총 {len(agg_list)}일) ]</b>\n\n"
        
        msg += "<code>No. 일자   구분  평균단가  수량\n"
        msg += "-"*30 + "\n"
        
        for item in agg_list[:50]: 
            d_str = item['date'][5:].replace('-', '.')
            s_str = "🔴매수" if item['side'] == 'BUY' else "🔵매도"
            msg += f"{item['no']:<3} {d_str} {s_str} ${item['avg']:<6.2f} {item['qty']}주\n"
            
        if len(agg_list) > 50: msg += "... (이전 기록 생략)\n"
        msg += "-"*30 + "</code>\n"

        msg += f"📊 <b>[ 현재 진행 상황 요약 ]</b>\n"
        if not is_history:
            if is_reverse:
                msg += f"▪️ 운용 상태 : 🚨 <b>시드 소진 (리버스모드 가동 중)</b>\n"
                msg += f"▪️ 리버스 T값 : <b>{t_val} T</b> (특수연산 적용됨)\n"
            else:
                msg += f"▪️ <b>현재 T값 : {t_val} T</b> ({int(split)}분할)\n"
            msg += f"▪️ 보유 수량 : {qty} 주 (평단 ${avg:.2f})\n"
        else:
            profit = sold - invested
            pct = (profit/invested*100) if invested > 0 else 0
            sign = "+" if profit >= 0 else "-"
            msg += f"▪️ <b>최종수익: {sign}${abs(profit):,.2f} ({pct:.2f}%)</b>\n"

        msg += f"▪️ 총 매수액 : ${invested:,.2f}\n▪️ 총 매도액 : ${sold:,.2f}\n"

        keyboard = []
        if not is_history:
            other = "TQQQ" if ticker == "SOXL" else "SOXL"
            keyboard.append([InlineKeyboardButton(f"🔄 {other} 장부 조회", callback_data=f"REC:VIEW:{other}")])
            keyboard.append([InlineKeyboardButton(f"🗄️ {ticker} V-REV 큐(Queue) 정밀 관리", callback_data=f"QUEUE:VIEW:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 장부 대시보드 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🖼️ 프리미엄 졸업 카드 발급", callback_data=f"HIST:IMG:{ticker}")])
            keyboard.append([InlineKeyboardButton("🔙 역사 목록으로 돌아가기", callback_data="HIST:LIST")])

        return msg, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H = 600, 920 
        IMG_H = 430 
        
        img = Image.new('RGB', (W, H), color='#1E222D')
        draw = ImageDraw.Draw(img)
        
        try:
            if os.path.exists("background.png"):
                bg = Image.open("background.png").convert("RGB")
                bg_ratio = bg.width / bg.height
                if bg_ratio > (W / IMG_H):
                    new_w = int(IMG_H * bg_ratio)
                    bg = bg.resize((new_w, IMG_H), Image.Resampling.LANCZOS).crop(((new_w - W) // 2, 0, (new_w + W) // 2, IMG_H))
                else:
                    new_h = int(W / bg_ratio)
                    bg = bg.resize((W, new_h), Image.Resampling.LANCZOS).crop((0, (new_h - IMG_H) // 2, W, (new_h + IMG_H) // 2))
                img.paste(bg, (0, 0))
            else:
                draw.rectangle([0, 0, W, IMG_H], fill="#111217")
        except: 
            draw.rectangle([0, 0, W, IMG_H], fill="#111217")

        f_title = self._load_best_font(self.bold_font_paths, 65)
        f_p = self._load_best_font(self.bold_font_paths, 85)
        f_y = self._load_best_font(self.reg_font_paths, 40)
        f_b_val = self._load_best_font(self.bold_font_paths, 32)
        f_b_lbl = self._load_best_font(self.reg_font_paths, 22)

        y_title = IMG_H + 60
        draw.rectangle([W/2 - 140, y_title - 45, W/2 + 140, y_title + 45], fill="#2A2F3D")
        draw.text((W/2, y_title), f"{ticker}", font=f_title, fill="white", anchor="mm")
        
        color = "#007AFF" if profit < 0 else "#FF3B30"
        sign = "-" if profit < 0 else "+"
        
        y_profit = y_title + 105
        draw.text((W/2, y_profit), f"{sign}${abs(profit):,.2f}", font=f_p, fill=color, anchor="mm")
        
        y_yield = y_profit + 75
        draw.text((W/2, y_yield), f"YIELD {sign}{abs(yield_pct):,.2f}%", font=f_y, fill=color, anchor="mm")
        
        y_box = y_yield + 60
        
        draw.rectangle([40, y_box, 290, y_box + 100], fill="#2A2F3D")
        draw.text((165, y_box + 35), f"${invested:,.2f}", font=f_b_val, fill="white", anchor="mm")
        draw.text((165, y_box + 75), "TOTAL INVESTED", font=f_b_lbl, fill="#8E8E93", anchor="mm")
        
        draw.rectangle([310, y_box, 560, y_box + 100], fill="#2A2F3D")
        draw.text((435, y_box + 35), f"${revenue:,.2f}", font=f_b_val, fill="white", anchor="mm")
        draw.text((435, y_box + 75), "TOTAL REVENUE", font=f_b_lbl, fill="#8E8E93", anchor="mm")
        
        draw.text((W/2, H - 35), f"{end_date}", font=f_b_lbl, fill="#636366", anchor="mm")
        
        fname = f"data/profit_{ticker}.png"
        img.save(fname)
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🔥 SOXL 전용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("🚀 TQQQ 전용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("💎 SOXL + TQQQ 통합", callback_data="TICKER:ALL")]
        ]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재: <b>{', '.join(current_tickers)}</b>", InlineKeyboardMarkup(keyboard)
