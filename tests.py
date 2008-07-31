import unittest

from asyncgen import async, generator_map, generator_splitter

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
    
    def test_exception_passing(self):
        @async
        def f():
            raise ValueError('blah')
        
        try:
            f()
            self.fail('did not raise exception')
        except ValueError, e:
            self.failUnless('blah' in str(e))

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
    
    def test_missing_async_argument(self):
        @async('i')
        def f(i):
            return (v*v for v in i)
        
        try:
            f([1,2,3])
            self.fail('did not raise exception')
        except ValueError, e:
            self.failUnless('Did not find async input named "i"' in str(e))
    
    def test_exception_via_input(self):
        def src():
            yield 1
            yield 2
            raise TypeError
        
        @async('i')
        def f(i):
            try:
                for v in i:
                    yield v
            except TypeError, e:
                yield 'err'
        
        self.failUnlessEqual(list(f(i=src())), [1, 2, 'err'])

class GeneratorMapTestCase(unittest.TestCase):
    def test_equal_lengths(self):
        g1 = [1, 2, 4]
        g2 = [1, 3, 3]
        g3 = [1, 4, 2]
        
        g = generator_map(lambda a, b, c: a+b+c, g1, g2, g3)
        
        self.failUnlessEqual(list(g), [3, 9, 9])
    
    def test_different_lengths(self):
        g1 = [1]
        g2 = [1, 2, 3]
        
        def add(a, b):
            # return (a if a else 0) + (b if b else 0)
            if not a: a = 0
            if not b: b = 0
            return a + b
        
        self.failUnlessEqual(list(generator_map(add, g1, g2)), [2, 2, 3])

class WithMultipleInputsTestCase(unittest.TestCase):
    def test_two_inputs(self):
        @async('i1', 'i2')
        def f(i1, i2):
            return generator_map(lambda v1, v2: v1+v2, i1, i2)
        
        self.failUnlessEqual(list(f(i1=[1,2,3], i2=[3,2,1])), [4, 4, 4])
    
    def test_three_level_cascade(self):
        @async('a', 'b')
        def s(a, b):
            return generator_map(lambda ai, bi: ai+bi, a, b)
        
        out = s(
            a=s(
                a=s(a=[1, 2, 3], b=[3, 2, 1]),
                b=s(a=[-1, -2, -3], b=[7, 8, 9])
            ),
            b=s(a=[-7, -2, -1], b=[-3, -8, -9])
        )
        
        self.failUnlessEqual(list(out), [0, 0, 0])

class MultipleWorkersTestCase(unittest.TestCase):
    def test_two_workers(self):
        import os
        @async(workers=2)
        def pid():
            yield os.getpid()
        
        result = list(pid())
        
        self.failUnlessEqual(len(result), 2)
        self.failIfEqual(result[0], result[1])
    
    def test_distributed_inputs(self):
        @async('i', workers=3)
        def echo(i):
            for value in i:
                yield value
        
        self.failUnlessEqual(sum(echo(i=range(100))), sum(range(100)))
    
    def test_uneven_lengths(self):
        @async('length', workers=2)
        def f(length):
            for n in range(length.next()):
                yield n
        
        output = list(f(length=[1, 19]))
        self.failUnlessEqual(len(output), 20)
    
    def test_stop_after_one_exception(self):
        @async('raises', workers=2)
        def f(raises):
            if raises.next():
                raise TypeError
            else:
                while(True):
                    yield 13
        
        gen = f(raises=[True, False])
        
        # wait for a TypeError
        try:
            output = list(gen)
            self.fail('did not raise TypeError')
        except TypeError:
            pass
        
        # expect the generator to raise StopIteration from now on
        self.failUnlessRaises(StopIteration, lambda: gen.next())
        self.failUnlessRaises(StopIteration, lambda: gen.next())
        self.failUnlessRaises(StopIteration, lambda: gen.next())

class ResultBufferingTestCase(unittest.TestCase):
    def test_with_sleep(self):
        log = []
        def logger(s):
            while True:
                log.append(s)
                yield None
        
        @async('l', buffer=4)
        def producer(l):
            for c in range(10):
                l.next()
                yield c
        
        @async('l', 'numbers')
        def consumer(l, numbers):
            import time
            while True:
                time.sleep(.01)
                c = numbers.next()
                l.next()
            yield None
        
        list(consumer(l=logger('c'), numbers=producer(l=logger('p'))))
        self.failUnlessEqual(''.join(log), 'ppppcpcpcpcpcpcpcccc')

class PickeTestCase(unittest.TestCase):
    def setUp(self):
        self.pickle_log = []
        
        import cPickle
        import asyncgen
        
        real_load = cPickle.load
        def _load(file):
            self.pickle_log.append('u')
            return cPickle.load(file)
        asyncgen.pickle_load = _load
    
    def test_simple_pickle(self):
        @async(tempfile_output=True)
        def f():
            yield 1
            yield 2
        
        self.failUnlessEqual(list(f()), [1, 2])
        self.failUnlessEqual(''.join(self.pickle_log), 'uu')
    
    def test_pickle_chain(self):
        @async('i', tempfile_output=True)
        def f(i):
            for val in i:
                yield val+1
        
        self.failUnlessEqual(list(f(i=f(i=[1,2,3]))), [3, 4, 5])
        self.failUnlessEqual(''.join(self.pickle_log), 'uuu')

class GeneratorSplitterTestCase(unittest.TestCase):
    def test_split(self):
        @async
        def src():
            yield (1, 'a') # tuple
            yield {0:2, 1:'b'} # dict
            yield [3, 'c'] # list
        
        @async('i')
        def dst(i):
            for v in i:
                yield v
        
        gs = generator_splitter(src(), [0, 1])
        self.failUnlessEqual(list(dst(i=gs[0])), [1, 2, 3])
        self.failUnlessEqual(list(dst(i=gs[1])), ['a', 'b', 'c'])
    
    def test_dict(self):
        self.failUnlessEqual(list(generator_splitter([{'a':2}], ['a'])['a']), [2])
    
    def test_bad_input(self):
        self.failUnlessRaises(TypeError, lambda: generator_splitter([13], [0])[0].next())
    
    def test_bad_key(self):
        try:
            generator_splitter([[1]], [0])[1].next()
            self.fail('did not raise KeyError')
        except KeyError, e:
            self.failUnless('the key you asked for, 1, was not in the list of keys to retrieve' in str(e))
    
    def test_split_async(self):
        import time
        event_log = []
        
        def log(s):
            while True:
                event_log.append(s)
                yield
        
        def src():
            yield (3, 1, 0)
            yield (0, 1, 4)
            yield (0, 5, 0)
        
        @async('i', 'l', buffer=1)
        def dst(i, l):
            for v in i:
                time.sleep(v/100.)
                l.next()
            yield
        
        gs = generator_splitter(src(), [0, 1, 2])
        all_dst = list( dst(i=gs[i], l=log(n)) for i, n in enumerate('cba') )
        for d in all_dst:
            list(d)
        
        self.failUnlessEqual(''.join(event_log), 'abbcccaab')
    
    def test_split_premature_stop(self):
        import time
        
        @async
        def src():
            time.sleep(.01)
            yield (0, 1, 2)
        
        @async('i', buffer=1)
        def dst(i):
            yield i.next()
        
        gs = generator_splitter(src(), [0, 1, 2])
        outs = list( dst(i=gs[n]) for n in range(3) )
        
        for n in range(3):
            self.failUnlessEqual(list(outs[n]), [n])

if __name__ == '__main__':
    unittest.main()
