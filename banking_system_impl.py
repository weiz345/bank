from banking_system import BankingSystem


class BankingSystemImpl(BankingSystem):
    def __init__(self):
        self.balances = {}

    def create_account(self, timestamp: int, account_id: str) -> bool:
        if account_id in self.balances:
            return False
        self.balances[account_id] = 0
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
        return self.balances[source_account_id]
