# ==========================================================
# [queue_ledger.py]
# ⚠️ 신규 역추세 엔진(V_REV) 전용 LIFO 로트(Lot) 장부 관리 모듈
# 💡 [핵심 수술] 수량 동기화(CALIB) 및 Pop 차감 로직 내 Safe Casting (None 방어) 전면 이식 완료
# ==========================================================
import os
import json
import time
from datetime import datetime

class QueueLedger:
    def __init__(self, file_path="data/queue_ledger.json"):
        self.file_path = file_path
        self._ensure_file()

    def _ensure_file(self):
        dir_name = os.path.dirname(self.file_path)
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
        if not os.path.exists(self.file_path):
            self._save({})

    def _load(self):
        for _ in range(3):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                time.sleep(0.1)
        return {}

    def _save(self, data):
        for _ in range(3):
            try:
                with open(self.file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    f.flush()
                    os.fsync(f.fileno())
                return
            except Exception:
                time.sleep(0.1)

    def get_queue(self, ticker):
        """특정 종목의 전체 매수 로트(Lot) 배열을 반환합니다."""
        data = self._load()
        return data.get(ticker, [])

    def get_total_qty(self, ticker):
        """큐에 적재된 해당 종목의 총 수량을 반환합니다."""
        q = self.get_queue(ticker)
        # 💡 [수술] None 방어막 이식
        return sum(int(float(item.get("qty") or 0)) for item in q)

    def add_lot(self, ticker, qty, price, lot_type="NORMAL"):
        """새로운 매수 체결 건을 큐의 마지막(우측)에 독립 객체로 Push 합니다."""
        qty = int(float(qty or 0))
        if qty <= 0: return
        data = self._load()
        q = data.get(ticker, [])
        q.append({
            "qty": qty,
            "price": float(price or 0.0),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": lot_type
        })
        data[ticker] = q
        self._save(data)

    def pop_lots(self, ticker, target_qty):
        """매도 체결 시 큐의 우측(가장 최근 매수분)부터 LIFO 방식으로 수량을 Pop 차감합니다."""
        target_qty = int(float(target_qty or 0))
        if target_qty <= 0: return 0
        data = self._load()
        q = data.get(ticker, [])
        popped_total = 0

        while q and target_qty > 0:
            last_lot = q[-1]
            lot_qty = int(float(last_lot.get("qty") or 0))
            if lot_qty <= target_qty:
                popped = q.pop()
                popped_qty = int(float(popped.get("qty") or 0))
                popped_total += popped_qty
                target_qty -= popped_qty
            else:
                last_lot["qty"] = lot_qty - target_qty
                popped_total += target_qty
                target_qty = 0

        data[ticker] = q
        self._save(data)
        return popped_total

    def sync_with_broker(self, ticker, actual_qty):
        """
        [비파괴 보정 CALIB 로직]
        KIS 서버의 실제 잔고와 큐의 총합이 다를 경우, 장부를 절대 덮어쓰지 않고
        LIFO 방식의 가상 Pop 또는 CALIB 로트 Push를 통해 오차만 보정합니다.
        """
        data = self._load()
        q = data.get(ticker, [])
        current_q_qty = sum(int(float(item.get("qty") or 0)) for item in q)
        actual_qty = int(float(actual_qty or 0))

        if current_q_qty == actual_qty:
            return False 

        if current_q_qty < actual_qty:
            # 💡 한투 실제 수량이 더 많음 -> 오차만큼 CALIB_ADD 밀어넣기
            diff = actual_qty - current_q_qty
            q.append({
                "qty": diff,
                "price": 0.0, 
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "type": "CALIB_ADD"
            })
        else:
            # 💡 한투 실제 수량이 더 적음 -> LIFO 방식으로 오차만큼 큐에서 깎아내기
            diff = current_q_qty - actual_qty
            while q and diff > 0:
                last_lot = q[-1]
                lot_qty = int(float(last_lot.get("qty") or 0))
                if lot_qty <= diff:
                    popped = q.pop()
                    popped_qty = int(float(popped.get("qty") or 0))
                    diff -= popped_qty
                else:
                    last_lot["qty"] = lot_qty - diff
                    diff = 0

        data[ticker] = q
        self._save(data)
        return True
