from __future__ import annotations

# 何をするファイルか：Best層の板変化からCancel/Add比（C/A比）をローリング窓で計算する部品
from collections import deque
from dataclasses import dataclass
import math
from typing import Deque, Optional, Tuple


@dataclass(frozen=True)
class _CAEvent:
    # 何をするクラスか：1回の板更新で発生したAdd/Cancelカウントを時刻つきで保持する箱
    ts_ms: int
    add: int
    cancel: int


@dataclass
class _BestState:
    # 何をするクラスか：各サイドのBest（価格とサイズ）を前回値として覚えて差分判定に使う箱
    price: Optional[float] = None
    size: Optional[float] = None


class CancelAddRatio:
    # 何をするクラスか：直近win_msのBest層Cancel/Add比（C/A比）を計算してゲート判定に使う

    def __init__(self, win_ms: int) -> None:
        # 何をする関数か：窓幅（ms）を受け取り、イベント窓と合計値、前回Best状態を初期化する
        self._win_ms: int = int(win_ms)
        self._events: Deque[_CAEvent] = deque()
        self._add_sum: int = 0
        self._cancel_sum: int = 0
        self._bid: _BestState = _BestState()
        self._ask: _BestState = _BestState()

    def update(
        self,
        ts_ms: int,
        best_bid_price: float,
        best_bid_size: float,
        best_ask_price: float,
        best_ask_size: float,
    ) -> None:
        # 何をする関数か：Best（bid/ask）の変化からAdd/Cancelを数えて窓に積み上げる
        add_bid, cancel_bid = self._count_side(self._bid, best_bid_price, best_bid_size)
        add_ask, cancel_ask = self._count_side(self._ask, best_ask_price, best_ask_size)

        add_total = add_bid + add_ask
        cancel_total = cancel_bid + cancel_ask

        if add_total != 0 or cancel_total != 0:
            self._events.append(_CAEvent(ts_ms=int(ts_ms), add=add_total, cancel=cancel_total))
            self._add_sum += add_total
            self._cancel_sum += cancel_total

        self._prune(int(ts_ms))

    @property
    def ratio(self) -> float:
        # 何をする関数か：直近窓のC/A比を返す（Add=0の例外扱いもここで固定する）
        if self._add_sum == 0:
            return math.inf if self._cancel_sum > 0 else 0.0
        return self._cancel_sum / self._add_sum

    def allowed(self, threshold: float) -> bool:
        # 何をする関数か：C/A比がしきい値以下ならTrue（発注してよい）を返す
        return self.ratio <= float(threshold)

    def snapshot(self) -> Tuple[int, int, float]:
        # 何をする関数か：ログ出し用に（add_sum, cancel_sum, ratio）をまとめて返す
        return self._add_sum, self._cancel_sum, self.ratio

    def _prune(self, now_ms: int) -> None:
        # 何をする関数か：窓の外（now_ms - win_ms より古い）イベントを捨てて合計を保つ
        cutoff = now_ms - self._win_ms
        while self._events and self._events[0].ts_ms < cutoff:
            ev = self._events.popleft()
            self._add_sum -= ev.add
            self._cancel_sum -= ev.cancel

        if self._add_sum < 0:
            self._add_sum = 0
        if self._cancel_sum < 0:
            self._cancel_sum = 0

    def _count_side(self, st: _BestState, new_price: float, new_size: float) -> Tuple[int, int]:
        # 何をする関数か：片側のBest変化をAdd/Cancelイベント（回数）へ変換する
        # ルール：
        # - 価格同一なら size増=Add、size減=Cancel
        # - 価格変化なら「旧Bestが消えた=Cancel」「新Bestが現れた=Add」として各1回カウント
        add = 0
        cancel = 0

        if st.price is None or st.size is None:
            st.price = float(new_price)
            st.size = float(new_size)
            return 0, 0

        old_price = st.price
        old_size = st.size
        st.price = float(new_price)
        st.size = float(new_size)

        if float(new_price) != float(old_price):
            if old_size > 0:
                cancel += 1
            if new_size > 0:
                add += 1
            return add, cancel

        delta = float(new_size) - float(old_size)
        if delta > 0:
            add += 1
        elif delta < 0:
            cancel += 1

        return add, cancel
