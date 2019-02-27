import unittest
from sas import *

class TestUtilitiesMethods(unittest.TestCase):

    def test_simple_unanchor(self):
        self.assertEqual(unanchor('<a href="https://www.example.com">'),
                         'https://www.example.com')

    def test_messy_unanchor(self):
        self.assertEqual(unanchor("<a sntg href='url' else>link</a>"),
                         'url')

    def test_unanchor_returning_that_if_not_match(self):
        self.assertEqual(unanchor("i am not parsing"),
                          "i am not parsing")

    def test_getproxy_return_null_from_empty_list(self):
        proxy = getproxy(list())
        self.assertIsNone(proxy)

    def test_gen_urls(self):
        gen = gen_urls([str(i) for i in range(1,8)])
        resp = []
        for i in range(7):
            resp.append(next(gen))
        print(f'[1, 2, 3, 4, 5, 6, 7] in random order {resp}')
        for i in range(1, 8):
            with self.subTest(i=i):
                self.assertIn(str(i), resp)
        self.assertRaises(StopIteration, next, gen)


class TestWorkerThread(unittest.TestCase):
    
    def test_worker_isconsist(self):
        worker = WorkingThread(1, [], ['google',], [])
        self.assertTrue(worker.is_consist('https://google.com'))     
        
        

if __name__ == "__main__":
    unittest.main()
