import unittest

from asyncgen import async

class SimpleCallsTestCase(unittest.TestCase):
    """
    Test async calls with no input, only yielding output
    """
    def test_one_function(self):
        @async
        def f():
            yield 'a'
        
        self.failUnlessEqual(list(f()), ['a'])
    
    def test_two_functions(self):
        import os
        @async
        def f():
            yield os.getpid()
        
        self.failIfEqual(list(f()), list(f()))
    
    def test_arguments(self):
        @async
        def f(i, j):
            yield i+j
        
        self.failUnlessEqual(list(f('[', j=']')), ['[]'])

if __name__ == '__main__':
    unittest.main()
