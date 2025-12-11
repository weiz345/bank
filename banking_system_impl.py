from banking_system import BankingSystem
from collections import deque 


class BankingSystemImpl(BankingSystem):
    '''
    Implementation of the BankingSystem interface supporting deposits, transfers,
    payments with delayed cashback, account merging, top-spender queries, and
    historical balance lookup.
    '''
    def __init__(self):
        self.balances = {}
        self.outgoing = {}
        self.payments = {}
        self.payment_counter = 0
        self.merged_time = {}
        self.balance_history = {}
        self.cashback = deque()
        self.sorted_outgoing = []
        self.outgoing_key_map = {}
    def _process_cashbacks(self, timestamp: int):
        '''
        Docstring for _process_cashbacks

        :param timestamp: The current timestamp. All payments with refund_ts <= timestamp are refunded
        :type timestamp: int
        '''
        while self.cashback and self.cashback[0][0] <= timestamp:
            refund_ts, acc, name = self.cashback.popleft() # pop from the front of the queue 
            acc_payments = self.payments.get(acc)
            if acc_payments:
                info = acc_payments.get(name)

                if info["status"] == "IN_PROGRESS":
                    if acc in self.balances:
                        self.balances[acc] += info["cashback"]
                        self.balance_history[acc].append((info["refund_ts"], self.balances[acc]))
                    info["status"] = "CASHBACK_RECEIVED"
    def _remove_from_sorted(self, acc: str):
        key = self.outgoing_key_map.pop(acc, None)
        if key is None:
            return
        idx = bisect.bisect_left(self.sorted_outgoing, key)
        if idx < len(self.sorted_outgoing) and self.sorted_outgoing[idx] == key:
            self.sorted_outgoing.pop(idx)

    def _insert_into_sorted(self, acc: str):
        key = (-self.outgoing[acc], acc)
        bisect.insort(self.sorted_outgoing, key)
        self.outgoing_key_map[acc] = key

    def _update_sorted_outgoing(self, acc: str):
        if acc not in self.outgoing:
            self._remove_from_sorted(acc)
            return
        self._remove_from_sorted(acc)
        self._insert_into_sorted(acc)
    def create_account(self, timestamp: int, account_id: str) -> bool:
        '''
        Create a new account with zero initial balance

        :param timestamp: Current timestamp
        :type timestamp: int
        :param account_id: Unique account identifier
        :type account_id: str
        :return: Return True if the account was successfully created, else return False
        :rtype: bool
        '''
        self._process_cashbacks(timestamp)
        if account_id in self.balances:
            return False

        if account_id in self.merged_time:
            del self.merged_time[account_id]

        self.balances[account_id] = 0
        self.outgoing[account_id] = 0
        self.payments[account_id] = {}
        self.balance_history[account_id] = [(timestamp, 0)]
        self._insert_into_sorted(account_id)
        return True


    def deposit(self, timestamp: int, account_id: str, amount: int) -> int | None:
        '''
        Deposit an amount into an account

        :param timestamp: Current timestamp
        :type timestamp: int
        :param account_id: Target account
        :type account_id: str
        :param amount: Amoujnt to deposit
        :type amount: int
        :return: Return updated balance or None for account not found
        :rtype: int | None
        '''
        self._process_cashbacks(timestamp)
        if account_id not in self.balances:
            return None
        self.balances[account_id] += amount
        self.balance_history[account_id].append((timestamp, self.balances[account_id]))
        return self.balances[account_id]

    def transfer(self, timestamp: int, source: str, target: str, amount: int) -> int | None:
        '''
        Transfer funds from one account to another

        :param timestamp: Current timestamp
        :type timestamp: int
        :param source: Account to transfer from
        :type source: str
        :param target: Account to transfer to
        :type target: str
        :param amount: Amount to transfer
        :type amount: int
        :return: Return updated balance or None for transfer failed
        :rtype: int | None
        '''
        self._process_cashbacks(timestamp)
        if (source not in self.balances or target not in self.balances or
                source == target or self.balances[source] < amount):
            return None
        self.balances[source] -= amount
        self.balances[target] += amount
        self.outgoing[source] += amount
        self._update_sorted_outgoing(source)
        self.balance_history[source].append((timestamp, self.balances[source]))
        self.balance_history[target].append((timestamp, self.balances[target]))
        return self.balances[source]

    def pay(self, timestamp: int, account_id: str, amount: int) -> str | None:
        '''
        Docstring for pay
-
        :param timestamp: Current timestamp
        :type timestamp: int
        :param account_id: Account making the payment
        :type account_id: str
        :param amount: Amount to deduct
        :type amount: int
        :return: Return payment ID or None if the account does not exist or balance is insufficient
        :rtype: str | None
        '''

        self._process_cashbacks(timestamp)
        if account_id not in self.balances or self.balances[account_id] < amount:
            return None

        self.balances[account_id] -= amount
        self.outgoing[account_id] += amount
        self._update_sorted_outgoing(account_id)
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
        '''
        Retrieve the status of a previously created payment
        
        :param timestamp: Current timestamp
        :type timestamp: int
        :param account_id: Account associated with the payment
        :type account_id: str
        :param payment: Payment identifier
        :type payment: str
        :return: Return the payment status or None if not found
        :rtype: str | None
        '''
        self._process_cashbacks(timestamp)
        if account_id not in self.payments or payment not in self.payments[account_id]:
            return None
        return self.payments[account_id][payment]["status"]

    def top_spenders(self, timestamp: int, n: int) -> list[str]:
        '''
        Return the top-N accounts ranked by outgoing payment totals

        :param timestamp: Current timestamp
        :type timestamp: int
        :param n: Number of accounts to return
        :type n: int
        :return: A list of formatted strings that sorted by spending
        :rtype: list[str]
        '''
        self._process_cashbacks(timestamp)
        result = []
        for _, acc in self.sorted_outgoing[:n]:
            if acc in self.outgoing:
                result.append(f"{acc}({self.outgoing[acc]})")
        return result

 
    def merge_accounts(self, timestamp: int, a1: str, a2: str) -> bool:
        '''
        Merge account a2 into account a1
  
        :param timestamp: Current timestamp
        :type timestamp: int
        :param a1: Destination account
        :type a1: str
        :param a2: Account to be merged and deleted.
        :type a2: str
        :return: True if succeeded else False
        :rtype: bool
        '''
        self._process_cashbacks(timestamp)
        
        if a1 == a2 or a1 not in self.balances or a2 not in self.balances:
            return False
        self._remove_from_sorted(a1)
        self._remove_from_sorted(a2)

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
        '''
        Query the balance of an account at a specific historical timestamp
        
        :param timestamp: Current timestamp
        :type timestamp: int
        :param account_id: Account being queried
        :type account_id: str
        :param time_at: Historical timestamp to check
        :type time_at: int
        :return: The balance at the given time or None
        :rtype: int | None
        '''
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
    
  
