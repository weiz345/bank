from banking_system import BankingSystem
import heapq # As we were told in pset 3, question 2.1, Python provides a min heap implementation in the `heapq` class 


class BankingSystemImpl(BankingSystem):
    def __init__(self):
        self.balances = {}
        self.outgoing = {}
        self.payments = {}
        self.payment_counter = 0
        self.merged_time = {}
        self.balance_history = {}
        self.cashback = [] # priority queue (min heap)

    def _process_cashbacks(self, timestamp: int):
        while self.cashback:
            closest_refund_ts = self.cashback[0][0] # the next cashback due 
            if closest_refund_ts > timestamp:
                break # not yet time for a refund 

            refund_ts, acc, name = heapq.heappop(self.cashback) # pop and return the smallest item from the heap
            acc_payments = self.payments.get(acc)
            if acc_payments:
                info = acc_payments.get(name)

                if info["status"] == "IN_PROGRESS" and refund_ts <= timestamp:
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

        # push tuple onto the heap. Note that tuples are compared element-by-element from left to right,
        # i.e., our min heap will by sorted by `refund_ts` values 
        heapq.heappush(self.cashback, (refund_ts, account_id, name))

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

        # update the min heap such that cashback entries from a2 now go to a1 
        merged_heap = [] 
        for refund_ts, acc, name in self.cashback:
            if acc == a2:
                acc = a1 
            merged_heap.append((refund_ts, acc, name))
        heapq.heapify(merged_heap) # transform the list into a min-heap (linear time) 
        self.cashback = merged_heap 


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

        bal = None
        for ts, b in history:
            if ts <= time_at:
                bal = b
            else:
                break
        return bal

