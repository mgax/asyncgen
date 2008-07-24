import unittest

from asyncgen import async, async_with_input

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

class WithSingleInputTestCase(unittest.TestCase):
    """
    Test async calls that accept one input
    """
    def test_one_function(self):
        @async_with_input(i=[1,2,3])
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f()), [1, 4, 9])
    
    def test_one_function_with_generator(self):
        @async_with_input(i=(j for j in [1,2,3]))
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f()), [1, 4, 9])
    
    def test_function_chain(self):
        @async
        def g():
            return (j*j for j in [1,2,3])
        
        @async_with_input(i=g())
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f()), [1, 16, 81])
    
    def test_exception_passing(self):
        @async
        def f():
            raise ValueError('blah')
        
        try:
            f().next() # TODO: should not require .next() here!
            self.fail('did not raise exception')
        except ValueError, e:
            self.failUnless('blah' in str(e))

if __name__ == '__main__':
    unittest.main()
