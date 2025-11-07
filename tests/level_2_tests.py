import unittest
from banking_system_impl import BankingSystemImpl


class Level2Tests(unittest.TestCase):
    """
    The test suit below includes 10 tests for Level 2.

    All have the same score.
    You are not allowed to modify this file,
    but feel free to read the source code
    to better understand what is happening in every specific case.
    """

    failureException = Exception


    @classmethod
    def setUp(cls):
        cls.system = BankingSystemImpl()

    def test_level_2_case_01_basic_top_spenders_1(self):
        self.assertTrue(self.system.create_account(1, 'account1'))
        self.assertTrue(self.system.create_account(2, 'account2'))
        self.assertTrue(self.system.create_account(3, 'account3'))
        self.assertEqual(self.system.deposit(4, 'account1', 1000), 1000)
        self.assertEqual(self.system.deposit(5, 'account2', 1000), 1000)
        self.assertEqual(self.system.deposit(6, 'account3', 1000), 1000)
        self.assertEqual(self.system.transfer(7, 'account2', 'account3', 100), 900)
        self.assertEqual(self.system.transfer(8, 'account2', 'account1', 100), 800)
        self.assertEqual(self.system.transfer(9, 'account3', 'account1', 100), 1000)
        expected = ['account2(200)', 'account3(100)', 'account1(0)']
        self.assertEqual(self.system.top_spenders(10, 3), expected)

    def test_level_2_case_02_basic_top_spenders_2(self):
        self.assertTrue(self.system.create_account(1, 'account1'))
        self.assertTrue(self.system.create_account(2, 'account2'))
        self.assertTrue(self.system.create_account(3, 'account3'))
        self.assertEqual(self.system.deposit(4, 'account1', 500), 500)
        self.assertEqual(self.system.deposit(5, 'account2', 500), 500)
        self.assertEqual(self.system.deposit(6, 'account3', 750), 750)
        self.assertEqual(self.system.transfer(7, 'account2', 'account3', 239), 261)
        self.assertEqual(self.system.transfer(8, 'account3', 'account1', 350), 639)
        self.assertEqual(self.system.transfer(9, 'account2', 'account1', 61), 200)
        expected = ['account3(350)', 'account2(300)']
        self.assertEqual(self.system.top_spenders(10, 2), expected)
        expected = ['account3(350)', 'account2(300)', 'account1(0)']
        self.assertEqual(self.system.top_spenders(11, 4), expected)

    def test_level_2_case_03_basic_top_spenders_3(self):
        self.assertTrue(self.system.create_account(1, 'account1'))
        self.assertTrue(self.system.create_account(2, 'account2'))
        self.assertTrue(self.system.create_account(3, 'account3'))
        self.assertEqual(self.system.deposit(4, 'account1', 1000), 1000)
        self.assertEqual(self.system.deposit(5, 'account2', 1000), 1000)
        self.assertEqual(self.system.deposit(6, 'account3', 1000), 1000)
        self.assertEqual(self.system.transfer(7, 'account2', 'account3', 99), 901)
        self.assertEqual(self.system.transfer(8, 'account3', 'account1', 200), 899)
        self.assertEqual(self.system.transfer(9, 'account3', 'account1', 100), 799)
        self.assertEqual(self.system.transfer(10, 'account2', 'account3', 199), 702)
        self.assertEqual(self.system.transfer(11, 'account2', 'account3', 99), 603)
        self.assertEqual(self.system.transfer(12, 'account2', 'account1', 4), 599)
        self.assertEqual(self.system.transfer(13, 'account3', 'account1', 100), 997)
        expected = ['account2(401)', 'account3(400)']
        self.assertEqual(self.system.top_spenders(14, 2), expected)
        expected = ['account2(401)', 'account3(400)', 'account1(0)']
        self.assertEqual(self.system.top_spenders(15, 3), expected)
        expected = ['account2(401)', 'account3(400)', 'account1(0)']
        self.assertEqual(self.system.top_spenders(16, 4), expected)

    def test_level_2_case_04_top_spenders_with_failed_transfers(self):
        self.assertTrue(self.system.create_account(1, 'account1'))
        self.assertTrue(self.system.create_account(2, 'account2'))
        self.assertTrue(self.system.create_account(3, 'account3'))
        self.assertEqual(self.system.deposit(4, 'account1', 1000), 1000)
        self.assertEqual(self.system.deposit(5, 'account2', 1000), 1000)
        self.assertEqual(self.system.deposit(6, 'account3', 1000), 1000)
        self.assertEqual(self.system.transfer(7, 'account2', 'account3', 100), 900)
        self.assertIsNone(self.system.transfer(8, 'account2', 'account2', 500))
        self.assertIsNone(self.system.transfer(9, 'account2', 'account1', 2000))
        self.assertIsNone(self.system.transfer(10, 'account2', 'account4', 500))
        self.assertEqual(self.system.transfer(11, 'account3', 'account1', 200), 900)
        self.assertEqual(self.system.transfer(12, 'account1', 'account2', 300), 900)
        expected = ['account1(300)', 'account3(200)']
        self.assertEqual(self.system.top_spenders(13, 2), expected)
        expected = ['account1(300)', 'account3(200)', 'account2(100)']
        self.assertEqual(self.system.top_spenders(14, 3), expected)
        expected = ['account1(300)', 'account3(200)', 'account2(100)']
        self.assertEqual(self.system.top_spenders(15, 4), expected)

    def test_level_2_case_05_top_spenders_alphabetical_order_1(self):
        self.assertTrue(self.system.create_account(1, 'accountA'))
        self.assertTrue(self.system.create_account(2, 'accountC'))
        self.assertTrue(self.system.create_account(3, 'accountB'))
        self.assertEqual(self.system.deposit(4, 'accountA', 1000), 1000)
        self.assertEqual(self.system.deposit(5, 'accountB', 1000), 1000)
        self.assertEqual(self.system.deposit(6, 'accountC', 1000), 1000)
        self.assertEqual(self.system.transfer(7, 'accountB', 'accountC', 101), 899)
        self.assertEqual(self.system.transfer(8, 'accountB', 'accountA', 99), 800)
        self.assertEqual(self.system.transfer(9, 'accountA', 'accountC', 100), 999)
        self.assertEqual(self.system.transfer(10, 'accountA', 'accountB', 100), 899)
        self.assertEqual(self.system.transfer(11, 'accountC', 'accountA', 199), 1002)
        self.assertEqual(self.system.transfer(12, 'accountC', 'accountB', 1), 1001)
        expected = ['accountA(200)', 'accountB(200)', 'accountC(200)']
        self.assertEqual(self.system.top_spenders(13, 3), expected)

    def test_level_2_case_06_top_spenders_alphabetical_order_2(self):
        self.assertTrue(self.system.create_account(1, 'acc1'))
        self.assertTrue(self.system.create_account(2, 'acc3'))
        self.assertTrue(self.system.create_account(3, 'acc2'))
        self.assertEqual(self.system.deposit(4, 'acc1', 100), 100)
        self.assertEqual(self.system.deposit(5, 'acc2', 200), 200)
        self.assertEqual(self.system.deposit(6, 'acc3', 300), 300)
        expected = ['acc1(0)', 'acc2(0)', 'acc3(0)']
        self.assertEqual(self.system.top_spenders(7, 3), expected)

    def test_level_2_case_07_all_commands_1(self):
        self.assertTrue(self.system.create_account(1, 'acc1'))
        self.assertTrue(self.system.create_account(2, 'acc2'))
        self.assertTrue(self.system.create_account(3, 'acc3'))
        self.assertTrue(self.system.create_account(4, 'acc4'))
        self.assertTrue(self.system.create_account(5, 'acc5'))
        self.assertIsNone(self.system.deposit(11, 'acc0', 900))
        self.assertEqual(self.system.deposit(12, 'acc1', 300), 300)
        self.assertEqual(self.system.deposit(13, 'acc2', 350), 350)
        self.assertEqual(self.system.deposit(14, 'acc3', 150), 150)
        self.assertEqual(self.system.deposit(15, 'acc4', 400), 400)
        self.assertEqual(self.system.deposit(16, 'acc5', 600), 600)
        self.assertEqual(self.system.transfer(21, 'acc1', 'acc3', 20), 280)
        self.assertEqual(self.system.deposit(22, 'acc3', 150), 320)
        self.assertIsNone(self.system.transfer(23, 'acc2', 'acc2', 25))
        self.assertEqual(self.system.transfer(24, 'acc2', 'acc1', 30), 320)
        self.assertEqual(self.system.transfer(25, 'acc3', 'acc5', 35), 285)
        self.assertEqual(self.system.deposit(26, 'acc4', 400), 800)
        self.assertEqual(self.system.transfer(27, 'acc5', 'acc1', 40), 595)
        self.assertEqual(self.system.transfer(28, 'acc3', 'acc4', 45), 240)
        self.assertEqual(self.system.transfer(29, 'acc4', 'acc1', 50), 795)
        expected = ['acc3(80)', 'acc4(50)', 'acc5(40)', 'acc2(30)', 'acc1(20)']
        self.assertEqual(self.system.top_spenders(30, 5), expected)
        self.assertEqual(self.system.deposit(31, 'acc5', 600), 1195)
        self.assertEqual(self.system.transfer(32, 'acc3', 'acc5', 55), 185)
        self.assertEqual(self.system.transfer(33, 'acc1', 'acc2', 60), 340)
        self.assertIsNone(self.system.transfer(34, 'acc1', 'acc0', 65))
        self.assertTrue(self.system.create_account(35, 'acc6'))
        expected = ['acc3(135)', 'acc1(80)', 'acc4(50)', 'acc5(40)', 'acc2(30)']
        self.assertEqual(self.system.top_spenders(36, 5), expected)

    def test_level_2_case_08_all_commands_2(self):
        self.assertTrue(self.system.create_account(1, 'acc1'))
        self.assertTrue(self.system.create_account(2, 'acc2'))
        self.assertTrue(self.system.create_account(3, 'acc3'))
        self.assertTrue(self.system.create_account(4, 'acc4'))
        self.assertTrue(self.system.create_account(5, 'acc5'))
        self.assertTrue(self.system.create_account(6, 'acc6'))
        self.assertTrue(self.system.create_account(7, 'acc7'))
        self.assertTrue(self.system.create_account(8, 'acc8'))
        self.assertTrue(self.system.create_account(9, 'acc9'))
        self.assertTrue(self.system.create_account(10, 'acc10'))
        self.assertIsNone(self.system.deposit(11, 'acc0', 41))
        self.assertEqual(self.system.deposit(12, 'acc1', 48), 48)
        self.assertEqual(self.system.deposit(13, 'acc2', 30), 30)
        self.assertEqual(self.system.deposit(14, 'acc3', 680), 680)
        self.assertEqual(self.system.deposit(15, 'acc4', 326), 326)
        self.assertEqual(self.system.deposit(16, 'acc5', 73), 73)
        self.assertEqual(self.system.deposit(17, 'acc6', 349), 349)
        self.assertEqual(self.system.deposit(18, 'acc7', 65), 65)
        self.assertEqual(self.system.deposit(19, 'acc8', 547), 547)
        self.assertEqual(self.system.deposit(20, 'acc9', 452), 452)
        self.assertIsNone(self.system.transfer(21, 'acc10', 'acc9', 540))
        self.assertIsNone(self.system.transfer(22, 'acc9', 'acc3', 732))
        self.assertIsNone(self.system.transfer(23, 'acc3', 'acc9', 926))
        self.assertIsNone(self.system.transfer(24, 'acc4', 'acc5', 732))
        self.assertIsNone(self.system.transfer(25, 'acc7', 'acc8', 304))
        self.assertIsNone(self.system.transfer(26, 'acc7', 'acc4', 782))
        self.assertIsNone(self.system.transfer(27, 'acc1', 'acc9', 597))
        self.assertEqual(self.system.transfer(28, 'acc6', 'acc5', 236), 113)
        self.assertIsNone(self.system.transfer(29, 'acc2', 'acc2', 467))
        self.assertIsNone(self.system.transfer(30, 'acc6', 'acc8', 860))
        self.assertIsNone(self.system.deposit(31, 'acc0', 396))
        self.assertEqual(self.system.deposit(32, 'acc1', 520), 568)
        self.assertEqual(self.system.deposit(33, 'acc2', 709), 739)
        self.assertEqual(self.system.deposit(34, 'acc3', 752), 1432)
        self.assertEqual(self.system.deposit(35, 'acc4', 382), 708)
        self.assertEqual(self.system.deposit(36, 'acc5', 521), 830)
        self.assertEqual(self.system.deposit(37, 'acc6', 325), 438)
        self.assertEqual(self.system.deposit(38, 'acc7', 534), 599)
        self.assertEqual(self.system.deposit(39, 'acc8', 697), 1244)
        self.assertEqual(self.system.deposit(40, 'acc9', 370), 822)
        expected = ['acc6(236)', 'acc1(0)', 'acc10(0)', 'acc2(0)', 'acc3(0)', 'acc4(0)', 'acc5(0)', 'acc7(0)', 'acc8(0)', 'acc9(0)']
        self.assertEqual(self.system.top_spenders(41, 10), expected)
        self.assertEqual(self.system.transfer(42, 'acc2', 'acc1', 21), 718)
        self.assertIsNone(self.system.transfer(43, 'acc2', 'acc9', 922))
        self.assertEqual(self.system.transfer(44, 'acc9', 'acc8', 118), 704)
        self.assertIsNone(self.system.transfer(45, 'acc7', 'acc7', 230))
        expected = ['acc6(236)', 'acc9(118)', 'acc2(21)', 'acc1(0)', 'acc10(0)', 'acc3(0)', 'acc4(0)', 'acc5(0)', 'acc7(0)', 'acc8(0)']
        self.assertEqual(self.system.top_spenders(46, 10), expected)
        self.assertEqual(self.system.transfer(47, 'acc8', 'acc4', 197), 1165)
        self.assertIsNone(self.system.transfer(48, 'acc10', 'acc7', 914))
        self.assertEqual(self.system.transfer(49, 'acc4', 'acc10', 192), 713)
        expected = ['acc6(236)', 'acc8(197)', 'acc4(192)', 'acc9(118)', 'acc2(21)', 'acc1(0)', 'acc10(0)', 'acc3(0)', 'acc5(0)', 'acc7(0)']
        self.assertEqual(self.system.top_spenders(50, 10), expected)
        self.assertEqual(self.system.transfer(51, 'acc5', 'acc1', 829), 1)
        self.assertEqual(self.system.transfer(52, 'acc7', 'acc10', 451), 148)
        self.assertEqual(self.system.transfer(53, 'acc9', 'acc7', 581), 123)
        expected = ['acc5(829)', 'acc9(699)', 'acc7(451)', 'acc6(236)', 'acc8(197)', 'acc4(192)', 'acc2(21)', 'acc1(0)', 'acc10(0)', 'acc3(0)']
        self.assertEqual(self.system.top_spenders(54, 10), expected)

    def test_level_2_case_09_all_commands_3(self):
        self.assertTrue(self.system.create_account(1, 'acc1'))
        self.assertTrue(self.system.create_account(2, 'acc2'))
        self.assertTrue(self.system.create_account(3, 'acc3'))
        self.assertTrue(self.system.create_account(4, 'acc4'))
        self.assertTrue(self.system.create_account(5, 'acc5'))
        self.assertTrue(self.system.create_account(6, 'acc6'))
        self.assertTrue(self.system.create_account(7, 'acc7'))
        self.assertTrue(self.system.create_account(8, 'acc8'))
        self.assertTrue(self.system.create_account(9, 'acc9'))
        self.assertTrue(self.system.create_account(10, 'acc10'))
        self.assertIsNone(self.system.deposit(11, 'acc0', 261))
        self.assertEqual(self.system.deposit(12, 'acc1', 4), 4)
        self.assertEqual(self.system.deposit(13, 'acc2', 820), 820)
        self.assertEqual(self.system.deposit(14, 'acc3', 798), 798)
        self.assertEqual(self.system.deposit(15, 'acc4', 949), 949)
        self.assertEqual(self.system.deposit(16, 'acc5', 470), 470)
        self.assertEqual(self.system.deposit(17, 'acc6', 94), 94)
        self.assertEqual(self.system.deposit(18, 'acc7', 596), 596)
        self.assertEqual(self.system.deposit(19, 'acc8', 893), 893)
        self.assertEqual(self.system.deposit(20, 'acc9', 504), 504)
        self.assertIsNone(self.system.transfer(21, 'acc5', 'acc3', 794))
        self.assertEqual(self.system.transfer(22, 'acc3', 'acc4', 462), 336)
        self.assertEqual(self.system.transfer(23, 'acc5', 'acc6', 212), 258)
        self.assertIsNone(self.system.transfer(24, 'acc9', 'acc0', 357))
        self.assertEqual(self.system.transfer(25, 'acc2', 'acc1', 767), 53)
        self.assertIsNone(self.system.transfer(26, 'acc5', 'acc0', 779))
        self.assertEqual(self.system.transfer(27, 'acc1', 'acc7', 246), 525)
        self.assertEqual(self.system.transfer(28, 'acc8', 'acc2', 304), 589)
        self.assertIsNone(self.system.transfer(29, 'acc0', 'acc2', 698))
        self.assertIsNone(self.system.transfer(30, 'acc5', 'acc5', 544))
        expected = ['acc2(767)', 'acc3(462)', 'acc8(304)', 'acc1(246)', 'acc5(212)', 'acc10(0)', 'acc4(0)', 'acc6(0)', 'acc7(0)', 'acc9(0)']
        self.assertEqual(self.system.top_spenders(31, 10), expected)

    def test_level_2_case_10_all_commands_4(self):
        self.assertTrue(self.system.create_account(1, 'acc1'))
        self.assertTrue(self.system.create_account(2, 'acc2'))
        self.assertTrue(self.system.create_account(3, 'acc3'))
        self.assertTrue(self.system.create_account(4, 'acc4'))
        self.assertTrue(self.system.create_account(5, 'acc5'))
        self.assertTrue(self.system.create_account(6, 'acc6'))
        self.assertTrue(self.system.create_account(7, 'acc7'))
        self.assertTrue(self.system.create_account(8, 'acc8'))
        self.assertTrue(self.system.create_account(9, 'acc9'))
        self.assertTrue(self.system.create_account(10, 'acc10'))
        self.assertIsNone(self.system.deposit(11, 'acc0', 151))
        self.assertEqual(self.system.deposit(12, 'acc1', 20), 20)
        self.assertEqual(self.system.deposit(13, 'acc2', 236), 236)
        self.assertEqual(self.system.deposit(14, 'acc3', 444), 444)
        self.assertEqual(self.system.deposit(15, 'acc4', 540), 540)
        self.assertEqual(self.system.deposit(16, 'acc5', 460), 460)
        self.assertEqual(self.system.deposit(17, 'acc6', 254), 254)
        self.assertEqual(self.system.deposit(18, 'acc7', 946), 946)
        self.assertEqual(self.system.deposit(19, 'acc8', 401), 401)
        self.assertEqual(self.system.deposit(20, 'acc9', 398), 398)
        self.assertIsNone(self.system.transfer(21, 'acc8', 'acc4', 519))
        self.assertIsNone(self.system.transfer(22, 'acc9', 'acc10', 733))
        self.assertIsNone(self.system.transfer(23, 'acc4', 'acc9', 783))
        self.assertIsNone(self.system.transfer(24, 'acc1', 'acc10', 506))
        self.assertIsNone(self.system.transfer(25, 'acc4', 'acc5', 757))
        self.assertIsNone(self.system.transfer(26, 'acc2', 'acc7', 676))
        self.assertEqual(self.system.transfer(27, 'acc3', 'acc7', 133), 311)
        self.assertIsNone(self.system.transfer(28, 'acc5', 'acc5', 917))
        self.assertIsNone(self.system.transfer(29, 'acc10', 'acc8', 145))
        self.assertIsNone(self.system.transfer(30, 'acc10', 'acc10', 704))
        self.assertIsNone(self.system.deposit(31, 'acc0', 552))
        self.assertEqual(self.system.deposit(32, 'acc1', 939), 959)
        self.assertEqual(self.system.deposit(33, 'acc2', 771), 1007)
        self.assertEqual(self.system.deposit(34, 'acc3', 127), 438)
        self.assertEqual(self.system.deposit(35, 'acc4', 765), 1305)
        self.assertEqual(self.system.deposit(36, 'acc5', 275), 735)
        self.assertEqual(self.system.deposit(37, 'acc6', 661), 915)
        self.assertEqual(self.system.deposit(38, 'acc7', 40), 1119)
        self.assertEqual(self.system.deposit(39, 'acc8', 542), 943)
        self.assertEqual(self.system.deposit(40, 'acc9', 195), 593)
        expected = ['acc3(133)', 'acc1(0)', 'acc10(0)', 'acc2(0)', 'acc4(0)', 'acc5(0)', 'acc6(0)', 'acc7(0)', 'acc8(0)', 'acc9(0)']
        self.assertEqual(self.system.top_spenders(41, 10), expected)
        self.assertEqual(self.system.transfer(42, 'acc5', 'acc6', 220), 515)
        self.assertIsNone(self.system.transfer(43, 'acc8', 'acc8', 722))
        self.assertEqual(self.system.transfer(44, 'acc4', 'acc7', 646), 659)
        self.assertIsNone(self.system.transfer(45, 'acc2', 'acc2', 132))
        self.assertIsNone(self.system.transfer(46, 'acc4', 'acc0', 581))
        self.assertIsNone(self.system.transfer(47, 'acc10', 'acc1', 903))
        self.assertEqual(self.system.transfer(48, 'acc2', 'acc5', 282), 725)
        self.assertEqual(self.system.transfer(49, 'acc3', 'acc10', 326), 112)
        self.assertEqual(self.system.transfer(50, 'acc1', 'acc2', 271), 688)
        self.assertEqual(self.system.transfer(51, 'acc4', 'acc1', 136), 523)
        expected = ['acc4(782)', 'acc3(459)', 'acc2(282)', 'acc1(271)', 'acc5(220)', 'acc10(0)', 'acc6(0)', 'acc7(0)', 'acc8(0)', 'acc9(0)']
        self.assertEqual(self.system.top_spenders(52, 10), expected)
