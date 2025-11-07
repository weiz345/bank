from banking_system import BankingSystem


class BankingSystemImpl(BankingSystem):
    def __init__(self):
        self.balances = {}
        self.outgoing = {}

    def create_account(self, timestamp: int, account_id: str) -> bool:
        if account_id in self.balances:
            return False
        self.balances[account_id] = 0
        self.outgoing[account_id] = 0
        return True

    def deposit(self, timestamp: int, account_id: str, amount: int) -> int | None:
        if account_id not in self.balances:
            return None
        self.balances[account_id] += amount
        return self.balances[account_id]

    def transfer(self, timestamp: int, source_account_id: str, target_account_id: str, amount: int) -> int | None:
        if (source_account_id not in self.balances or
            target_account_id not in self.balances or
            source_account_id == target_account_id or
            self.balances[source_account_id] < amount):
            return None
        self.balances[source_account_id] -= amount
        self.balances[target_account_id] += amount
        self.outgoing[source_account_id] += amount
        return self.balances[source_account_id]

    def top_spenders(self, timestamp: int, n: int) -> list[str]:
        accounts = self.outgoing.items()
        sorted_accounts = sorted(accounts, key=lambda x: (-x[1], x[0]))
        result = [f"{acc}({amt})" for acc, amt in sorted_accounts[:n]]
        return result

