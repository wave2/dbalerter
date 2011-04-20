import unittest
from dbalerter import checkmysql
from dbalerter import config

class TestCheckMySQL(unittest.TestCase):

    def setUp(self):
        #Config file required to process checks
        config.initialise('/usr/local/etc/dbalerter/dbalerter.conf')
        checkmysql.initialise()

    def tearDown(self):
        checkmysql.cleanup()

    def test_check_slow_query_log(self):
        checkmysql.update_variables()
        checkmysql.check_slow_query_log()

if __name__ == '__main__':
    unittest.main()
