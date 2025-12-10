from banking_system import BankingSystem
from collections import deque 


class BankingSystemImpl(BankingSystem):
    def __init__(self):
        self.balances = {}
        self.outgoing = {}
        self.payments = {}
        self.payment_counter = 0
        self.merged_time = {}
        self.balance_history = {}
        self.cashback = deque()

    def _process_cashbacks(self, timestamp: int):
        while self.cashback and self.cashback[0][0] <= timestamp:
            refund_ts, acc, name = self.cashback.popleft() # pop from the front of the queue 
            acc_payments = self.payments.get(acc)
            if acc_payments:
                info = acc_payments.get(name)

                if info["status"] == "IN_PROGRESS":
                    if acc in self.balances:
                        self.balances[acc] += info["cashback"]
                        self.balance_history[acc].append((refund_ts, self.balances[acc]))
                    info["status"] = "CASHBACK_RECEIVED"

    def create_account(self, timestamp: int, account_id: str) -> bool:
        self._process_cashbacks(timestamp)
        if account_id in self.balances:
            return False

        if account_id in self.merged_time:
            del self.merged_time[account_id]

        self.balances[account_id] = 0
        self.outgoing[account_id] = 0
        self.payments[account_id] = {}
        self.balance_history[account_id] = [(timestamp, 0)]
        return True


    def deposit(self, timestamp: int, account_id: str, amount: int) -> int | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.balances:
            return None
        self.balances[account_id] += amount
        self.balance_history[account_id].append((timestamp, self.balances[account_id]))
        return self.balances[account_id]

    def transfer(self, timestamp: int, source: str, target: str, amount: int) -> int | None:
        self._process_cashbacks(timestamp)
        if (source not in self.balances or target not in self.balances or
                source == target or self.balances[source] < amount):
            return None
        self.balances[source] -= amount
        self.balances[target] += amount
        self.outgoing[source] += amount
        self.balance_history[source].append((timestamp, self.balances[source]))
        self.balance_history[target].append((timestamp, self.balances[target]))
        return self.balances[source]

    def pay(self, timestamp: int, account_id: str, amount: int) -> str | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.balances or self.balances[account_id] < amount:
            return None

        self.balances[account_id] -= amount
        self.outgoing[account_id] += amount
        self.balance_history[account_id].append((timestamp, self.balances[account_id]))

        self.payment_counter += 1
        name = f"payment{self.payment_counter}"
        cashback = amount * 2 // 100
        refund_ts = timestamp + 86_400_000  # 24h in ms
        self.payments[account_id][name] = {
            "refund_ts": refund_ts,
            "cashback": cashback,
            "status": "IN_PROGRESS",
        }

        # push tuple onto the deque (note that tuples are compared element-by-element from left to right)
        self.cashback.append((refund_ts, account_id, name))

        return name

    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.payments or payment not in self.payments[account_id]:
            return None
        return self.payments[account_id][payment]["status"]

    def top_spenders(self, timestamp: int, n: int) -> list[str]:
        self._process_cashbacks(timestamp)
        sorted_accounts = sorted(self.outgoing.items(), key=lambda x: (-x[1], x[0]))
        return [f"{acc}({amt})" for acc, amt in sorted_accounts[:n]]

 
    def merge_accounts(self, timestamp: int, a1: str, a2: str) -> bool:
        self._process_cashbacks(timestamp)
        
        if a1 == a2 or a1 not in self.balances or a2 not in self.balances:
            return False

        self.balances[a1] += self.balances[a2]
        self.outgoing[a1] += self.outgoing[a2]

        for name, info in self.payments.get(a2, {}).items():
            self.payments[a1][name] = info

        self.balance_history[a1].append((timestamp, self.balances[a1]))

        self.merged_time[a2] = timestamp
        del self.balances[a2]
        del self.outgoing[a2]
        del self.payments[a2]

        # update the deque such that cashback entries from a2 now go to a1 
        new_deque = deque()
        for refund_ts, acc, name in self.cashback:
            if acc == a2:
                acc = a1 
            new_deque.append((refund_ts, acc, name))
        self.cashback = new_deque


        return True


    def get_balance(self, timestamp: int, account_id: str, time_at: int) -> int | None:
        self._process_cashbacks(timestamp)

        if account_id in self.merged_time:
            if time_at >= self.merged_time[account_id]:
                return None

        if account_id not in self.balance_history:
            return None

        history = self.balance_history[account_id]
        if not history or time_at < history[0][0]:
            return None

        # binary search 
        left = 0
        right = len(history) - 1
        bal = None

        while left <= right:
            mid = (left+right) // 2
            ts, b = history[mid]

            if ts == time_at:
                return b 
            elif ts < time_at:
                bal = b
                left = mid+1
            else:
                right = mid-1
        return bal
    
  
