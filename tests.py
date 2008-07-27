import unittest

from asyncgen import async, generator_map

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
        @async('i')
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f(i=[1,2,3])), [1, 4, 9])
    
    def test_one_function_with_generator(self):
        @async('i')
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f(i=( j for j in [1,2,3] ))), [1, 4, 9])
    
    def test_function_chain(self):
        @async
        def g():
            return (j*j for j in [1,2,3])
        
        @async('i')
        def f(i):
            return (v*v for v in i)
        
        self.failUnlessEqual(list(f(i=g())), [1, 16, 81])
    
    def test_exception_passing(self):
        @async
        def f():
            raise ValueError('blah')
        
        try:
            f().next() # TODO: should not require .next() here!
            self.fail('did not raise exception')
        except ValueError, e:
            self.failUnless('blah' in str(e))
    
    def test_missing_async_argument(self):
        @async('i')
        def f(i):
            return (v*v for v in i)
        
        try:
            f([1,2,3])
            self.fail('did not raise exception')
        except ValueError, e:
            self.failUnless('Did not find async input named "i"' in str(e))

class GeneratorMapTestCase(unittest.TestCase):
    def test_equal_lengths(self):
        g1 = [1, 2, 4].__iter__()
        g2 = [1, 3, 3].__iter__()
        g3 = [1, 4, 2].__iter__()
        
        g = generator_map(lambda a, b, c: a+b+c, g1, g2, g3)
        
        self.failUnlessEqual(list(g), [3, 9, 9])
    
    def test_different_lengths(self):
        g1 = [1].__iter__()
        g2 = [1, 2, 3].__iter__()
        
        def add(a, b):
            return (a if a else 0) + (b if b else 0)
        
        self.failUnlessEqual(list(generator_map(add, g1, g2)), [2, 2, 3])

class WithMultipleInputsTestCase(unittest.TestCase):
    def test_two_inputs(self):
        @async('i1', 'i2')
        def f(i1, i2):
            return generator_map(lambda v1, v2: v1+v2, i1, i2)
        
        self.failUnlessEqual(list(f(i1=[1,2,3], i2=[3,2,1])), [4, 4, 4])

if __name__ == '__main__':
    unittest.main()
