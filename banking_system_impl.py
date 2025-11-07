from banking_system import BankingSystem


class BankingSystemImpl(BankingSystem):
    def __init__(self):
        self.balances = {}
        self.outgoing = {}
        self.payments = {}        
        self.payment_counter = 0

    def _process_cashbacks(self, timestamp: int):
        for acc, acc_payments in self.payments.items():
            for name, info in acc_payments.items():
                if info["status"] == "IN_PROGRESS" and info["refund_ts"] <= timestamp:
                    self.balances[acc] += info["cashback"]
                    info["status"] = "CASHBACK_RECEIVED"

    def create_account(self, timestamp: int, account_id: str) -> bool:
        self._process_cashbacks(timestamp)
        if account_id in self.balances:
            return False
        self.balances[account_id] = 0
        self.outgoing[account_id] = 0
        self.payments[account_id] = {}
        return True

    def deposit(self, timestamp: int, account_id: str, amount: int) -> int | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.balances:
            return None
        self.balances[account_id] += amount
        return self.balances[account_id]

    def transfer(self, timestamp: int, source: str, target: str, amount: int) -> int | None:
        self._process_cashbacks(timestamp)
        if (source not in self.balances or target not in self.balances or
            source == target or self.balances[source] < amount):
            return None
        self.balances[source] -= amount
        self.balances[target] += amount
        self.outgoing[source] += amount
        return self.balances[source]

    def pay(self, timestamp: int, account_id: str, amount: int) -> str | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.balances or self.balances[account_id] < amount:
            return None

        self.balances[account_id] -= amount
        self.outgoing[account_id] += amount

        self.payment_counter += 1
        name = f"payment{self.payment_counter}"

        cashback = amount * 2 // 100
        refund_ts = timestamp + 86_400_000 
        self.payments[account_id][name] = {
            "refund_ts": refund_ts,
            "cashback": cashback,
            "status": "IN_PROGRESS",
        }

        return name

    def get_payment_status(self, timestamp: int, account_id: str, payment: str) -> str | None:
        self._process_cashbacks(timestamp)
        if account_id not in self.balances or payment not in self.payments.get(account_id, {}):
            return None
        return self.payments[account_id][payment]["status"]

    def top_spenders(self, timestamp: int, n: int) -> list[str]:
        self._process_cashbacks(timestamp)
        sorted_accounts = sorted(self.outgoing.items(), key=lambda x: (-x[1], x[0]))
        return [f"{acc}({amt})" for acc, amt in sorted_accounts[:n]]
