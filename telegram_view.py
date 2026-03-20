import os
from PIL import Image, ImageDraw, ImageFont
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

class TelegramView:
    def __init__(self):
        pass

    def get_start_message(self, target_hour, season_icon, latest_version):
        init_time = f"{target_hour}:00"
        order_time = f"{target_hour}:30"
        season_short = "🌞서머타임 ON" if "Summer" in season_icon else "❄️서머타임 OFF"
        sync_time = "08:30" if target_hour == 17 else "09:30"

        return (
            f"🌌 <b>[ 다이내믹 스노우볼 TrueSync {latest_version} ]</b>\n" 
            f"⚡ <b>API 팩트 기반 무결성 동기화 엔진 가동</b> \n\n"
            f"🕒 <b>[ 운영 스케줄 ({season_short}) ]</b>\n"
            f"🔹 6시간 간격 : 🔑 API 토큰 자동 갱신\n"
            f"🔹 {sync_time} : 📝 잔고 동기화 & 자동 복리\n"
            f"🔹 {init_time} : 🔐 매매 잠금(Lock) 초기화\n"
            f"🔹 {init_time}~{order_time} : 프리장 감시 (🎯익절)\n"
            f"🔹 {order_time} : 🌃 정규장 통합 주문 실행\n\n"
            "🛠 <b>[ 주요 명령어 ]</b>\n"
            "▶️ <b>/sync</b> : 📜 통합 지시서 조회\n"
            "▶️ <b>/record</b> : 📊 장부 동기화 및 조회\n"
            "▶️ <b>/history</b> : 🏆 졸업 명예의 전당\n"
            "▶️ <b>/settlement</b> : ⚙️ 분할/복리/액면 설정\n"
            "▶️ <b>/seed</b> : 💵 개별 시드머니 관리\n"
            "▶️ <b>/ticker</b> : 🔄 운용 종목 선택\n"
            "▶️ <b>/mode</b> : 🏎️ 일반/가속 모드 변경\n"
            "▶️ <b>/version</b> : 🛠️ 버전 및 업데이트 내역\n\n" 
            "⚠️ <b>/reset</b> : 🔓 비상 해제 메뉴 (락/리버스)\n" 
            "<i>┗ 🚨 수동 닻 올리기: 예산 부족으로 리버스 진입 후 외화RP매도 등 예수금을 추가 입금하셨다면, 이 메뉴에서 반드시 '리버스 강제 해제'를 눌러 닻을 올려주세요!</i>"
        )

    def get_reset_menu(self):
        msg = (
            "🛠️ <b>[ 시스템 안전 통제실 ]</b>\n"
            "⚠️ 주의: 강제 초기화할 항목을 선택하세요."
        )
        keyboard = [
            [InlineKeyboardButton("🔓 [SOXL] 매매 잠금 해제", callback_data="RESET:LOCK:SOXL")],
            [InlineKeyboardButton("🔓 [TQQQ] 매매 잠금 해제", callback_data="RESET:LOCK:TQQQ")],
            [InlineKeyboardButton("🚨 [SOXL] 리버스/장부 초기화", callback_data="RESET:REV:SOXL")],
            [InlineKeyboardButton("🚨 [TQQQ] 리버스/장부 초기화", callback_data="RESET:REV:TQQQ")],
            [InlineKeyboardButton("❌ 취소 및 닫기", callback_data="RESET:CANCEL")]
        ]
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

    def get_version_message(self, history_data, page_index=None):
        groups = {}
        for h in history_data:
            ver_str = h if isinstance(h, str) else h.get('version', '')
            major_ver = ver_str.split('.')[0] if '.' in ver_str else ver_str.split(' ')[0]
            if major_ver not in groups:
                groups[major_ver] = []
            groups[major_ver].append(h)
            
        major_versions = list(groups.keys())
        major_versions.reverse() 

        if not major_versions:
            return "📭 기록된 버전 히스토리가 없습니다.", None

        if page_index is None:
            current_major = major_versions[0]
            items = groups.get(current_major, [])
            title = f"🛠️ <b>[ 최신 버전({current_major}) 업데이트 내역 ]</b>\n\n"
        else:
            page_index = max(0, min(page_index, len(major_versions) - 1))
            current_major = major_versions[page_index]
            items = groups.get(current_major, [])
            title = f"📚 <b>[ 과거 버전({current_major}) 명예의 전당 ]</b>\n\n"

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
        
        if page_index is None:
            if len(major_versions) > 1:
                keyboard.append([InlineKeyboardButton(f"📜 과거 버전 명예의 전당 보기 ({major_versions[1]})", callback_data="VERSION:PAGE:1")])
        else:
            nav_row = []
            if page_index < len(major_versions) - 1:
                prev_major = major_versions[page_index + 1]
                nav_row.append(InlineKeyboardButton(f"◀️ 이전 ({prev_major})", callback_data=f"VERSION:PAGE:{page_index + 1}"))
            if page_index > 0:
                next_major = major_versions[page_index - 1]
                nav_row.append(InlineKeyboardButton(f"다음 ({next_major}) ▶️", callback_data=f"VERSION:PAGE:{page_index - 1}"))
            
            if nav_row:
                keyboard.append(nav_row)
            
            keyboard.append([InlineKeyboardButton("⬆️ 접기 (최신 버전만 보기)", callback_data="VERSION:LATEST")])
            
        return msg, InlineKeyboardMarkup(keyboard)

    def create_sync_report(self, status_text, dst_text, cash, rp_amount, ticker_data, is_trade_active):
        total_locked = sum(t_info.get('escrow', 0.0) for t_info in ticker_data)
        
        header_msg = f"📜 <b>[ 통합 지시서 ({status_text}) ]</b>\n📅 <b>{dst_text}</b>\n"
        
        if total_locked > 0:
            real_cash = max(0, cash - total_locked)
            header_msg += f"💵 한투 전체 잔고: ${cash:,.2f}\n"
            header_msg += f"🔒 에스크로 격리금: -${total_locked:,.2f}\n"
            header_msg += f"✅ 실질 가용 예산: ${real_cash:,.2f}\n"
        else:
            header_msg += f"💵 주문가능금액: ${cash:,.2f}\n"
            
        header_msg += f"🏛️ RP 투자권장: ${rp_amount:,.2f}\n"
        header_msg += "----------------------------\n\n"
        
        body_msg = ""
        keyboard = []

        for t_info in ticker_data:
            t = t_info['ticker']
            v_mode = t_info['version']
            
            if v_mode == "V17":
                v_mode_display = "V17 시크릿"
                main_icon = "🦇"
            elif v_mode == "V14":
                v_mode_display = "무매4"
                main_icon = "💎"
            else:
                v_mode_display = "무매3"
                main_icon = "💎"
                
            is_rev = t_info.get('is_reverse', False)
            proc_status = t_info['plan'].get('process_status', '')
            
            if proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"⚠️ <b>[🚨 비상 상황: {t} 긴급 수혈 중]</b>\n"
                body_msg += f"❗ <i>에스크로 금고가 바닥나 강제 매도를 통해 현금을 생성합니다.</i>\n\n"
            
            if is_rev:
                bdg_txt = f"리버스 잔금쿼터 ${t_info['one_portion']:,.0f}"
                icon = "🩸" if proc_status == "🩸리버스(긴급수혈)" else "🔄"
                body_msg += f"{icon} <b>[{t}] {v_mode_display} 리버스 ({t_info['t_val']}T / {int(t_info['split'])}분)</b>\n"
            else:
                bdg_txt = f"오늘 예산 ${t_info['one_portion']:,.0f}" if v_mode in ["V14", "V17"] else f"1회 ${t_info['one_portion']:,.0f}"
                body_msg += f"{main_icon} <b>[{t}] ({v_mode_display} / {t_info['t_val']}T / {int(t_info['split'])}분할)</b>\n"
            
            body_msg += f"💵 총 시드: ${t_info['seed']:,.0f} ({bdg_txt})\n"
            
            escrow = t_info.get('escrow', 0.0)
            if escrow > 0:
                body_msg += f"🔐 <b>내 금고 보호액: ${escrow:,.2f}</b>\n"
            elif is_rev and proc_status == "🩸리버스(긴급수혈)":
                body_msg += f"🔐 <b>내 금고 보호액: $0.00 (Empty 🚨)</b>\n"
                
            body_msg += f"💰 현재 ${t_info['curr']:,.2f} / 평단 ${t_info['avg']:,.2f} <b>({t_info['qty']}주)</b>\n"
            
            sign = "+" if t_info['profit_amt'] >= 0 else "-"
            icon = "🔺" if t_info['profit_amt'] >= 0 else "🔻"
            body_msg += f"{icon} <b>수익: {sign}{abs(t_info['profit_pct']):.2f}% ({sign}${abs(t_info['profit_amt']):,.2f})</b>\n"
            
            if is_rev:
                body_msg += f"⚙️ 🌟 <b>5일선 별지점</b>: ${t_info['star_price']:.2f}\n"
            else:
                body_msg += f"⚙️ 🎯 {t_info['target']}% | ⭐ {t_info['star_pct']}% | 🏎️가속 {t_info['turbo_txt']}\n"
                
            # 🦇 [V19.1 패치] 하이브리드 동적 하한선(Dynamic Floor) 시각화 UI 제안 C(초정밀 축약형) 적용
            if v_mode == "V17":
                hybrid_target = t_info.get('hybrid_target', 0.0)
                if hybrid_target > 0:
                    trigger_reason = "BB" if t_info.get('trigger_reason') == "BB" else "-9%"
                    body_msg += f"📉 <b>스나이퍼({trigger_reason}): ${hybrid_target:.2f} 이하 대기중</b>\n"
                else:
                    body_msg += f"📉 <b>스나이퍼: 장전 대기 중</b>\n"
            
            body_msg += f"📋 <b>[주문 계획 - {proc_status}]</b>\n"
            
            if t_info['plan']['orders']:
                jup_orders = [o for o in t_info['plan']['orders'] if "줍줍" in o['desc']]
                n_orders = [o for o in t_info['plan']['orders'] if "줍줍" not in o['desc']]

                for o in n_orders:
                    ico = "🔴" if o['side'] == 'BUY' else "🔵"
                    desc = o['desc']
                    
                    if "수혈" in desc: 
                        ico = "🩸"
                        desc = desc.replace("🩸", "")
                    elif "시크릿" in desc: 
                        ico = "🦇"
                        desc = desc.replace("🦇", "")
                        
                    type_str = "" if o['type'] == 'LIMIT' else f"({o['type']})"
                    type_disp = f" {type_str}" if type_str else ""
                    
                    body_msg += f" {ico} {desc}: <b>${o['price']} x {o['qty']}주</b>{type_disp}\n"

                if jup_orders:
                    prices = sorted([o['price'] for o in jup_orders], reverse=True)
                    body_msg += f" 🧹 줍줍({len(jup_orders)}개): <b>${prices[0]} ~ ${prices[-1]} (LOC)</b>\n"
                
                if is_trade_active:
                    if t_info['is_locked']: body_msg += f" (✅ 금일 주문 완료/잠금)\n"
                    else: keyboard.append([InlineKeyboardButton(f"🚀 {t} 주문 실행", callback_data=f"EXEC:{t}")])
            else:
                body_msg += f" 💤 주문 없음 (관망/예산소진)\n"
            body_msg += "\n"

        final_msg = header_msg + body_msg
        if not is_trade_active: final_msg += "⛔ 장마감/애프터마켓: 주문 불가"
        return final_msg, InlineKeyboardMarkup(keyboard) if keyboard else None

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
            keyboard.append([InlineKeyboardButton("🔙 장부 대시보드 업데이트", callback_data=f"REC:SYNC:{ticker}")])
        else:
            keyboard.append([InlineKeyboardButton("🔙 역사 목록으로 돌아가기", callback_data="HIST:LIST")])

        return msg, InlineKeyboardMarkup(keyboard)

    def create_profit_image(self, ticker, profit, yield_pct, invested, revenue, end_date):
        W, H = 600, 920 
        IMG_H = 430 
        img = Image.new('RGB', (W, H), color='#121212')
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
        except: draw.rectangle([0, 0, W, IMG_H], fill="#252525")

        try:
            f_p = ImageFont.truetype("arial.ttf", 85)
            f_y = ImageFont.truetype("arial.ttf", 45)
            f_b = ImageFont.truetype("arial.ttf", 35)
        except: f_p = f_y = f_b = ImageFont.load_default()

        y = IMG_H + 50
        draw.text((W/2, y), ticker, font=f_b, fill="white", anchor="mm")
        y += 100
        color = "#FF453A" if profit < 0 else "#30D158"
        sign = "-" if profit < 0 else "+"
        draw.text((W/2, y), f"{sign}${abs(profit):,.2f}", font=f_p, fill=color, anchor="mm")
        y += 75
        draw.text((W/2, y), f"{sign}{abs(yield_pct):,.2f}%", font=f_y, fill=color, anchor="mm")
        
        y += 110
        draw.rectangle([40, y, 290, y + 110], fill="#1C1C1E")
        draw.text((165, y + 35), f"${invested:,.2f}", font=f_b, fill="white", anchor="mm")
        draw.text((165, y + 75), "Total Invested", font=f_b, fill="#8E8E93", anchor="mm")
        
        draw.rectangle([310, y, 560, y + 110], fill="#1C1C1E")
        draw.text((435, y + 35), f"${revenue:,.2f}", font=f_b, fill="white", anchor="mm")
        draw.text((435, y + 75), "Total Revenue", font=f_b, fill="#8E8E93", anchor="mm")
        
        draw.text((W/2, H - 40), f"Graduation Date: {end_date}", font=f_b, fill="#636366", anchor="mm")
        
        fname = f"data/profit_{ticker}_{end_date}.png"
        img.save(fname)
        return fname

    def get_ticker_menu(self, current_tickers):
        keyboard = [
            [InlineKeyboardButton("🔥 SOXL 전용", callback_data="TICKER:SOXL")],
            [InlineKeyboardButton("🚀 TQQQ 전용", callback_data="TICKER:TQQQ")],
            [InlineKeyboardButton("💎 SOXL + TQQQ 통합", callback_data="TICKER:ALL")]
        ]
        return f"🔄 <b>[ 운용 종목 선택 ]</b>\n현재: <b>{', '.join(current_tickers)}</b>", InlineKeyboardMarkup(keyboard)
