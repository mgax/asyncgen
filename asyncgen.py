import pprocess

def _async_process(func, args, kwargs, input_names):
    channel = pprocess.create()
    if channel.pid != 0:
        return channel
    
    try:
        for i in input_names:
            kwargs[i].set_channel(channel)
        
        gen = func(*args, **kwargs)
        try:
            for v in gen:
                channel.send(('next', v))
        except Exception, e:
            channel.send(('exception', e))
        
        channel.send(('exception', StopIteration()))
    
    except Exception, e:
        channel.send(('exception', e))
    
    finally:
        pprocess.exit(channel)

class AsyncJob(object):
    def __init__(self, func, args, kwargs, input_names):
        self.input = {}
        
        for name in input_names:
            try:
                gen = kwargs[name]
            except KeyError:
                raise ValueError('Did not find async input named "%s" - did you pass it as a named argument?' % name)
            if '__iter__' not in dir(gen):
                raise TypeError('Expected all the async inputs to be generators')
            
            kwargs[name] = AsyncInput(name)
            self.input[name] = gen.__iter__()
        
        self.channel = _async_process(func, args, kwargs, input_names)
    
    def __iter__(self):
        return self
    
    def next(self):
        t, v = self.channel.receive()
        
        while t == 'pull':
            try:
                iv = self.input[v].next()
                self.channel.send(('next', iv))
            except Exception, ie:
                self.channel.send(('exception', ie))
            
            t, v = self.channel.receive()
        
        if t == 'next':
            return v
        else:
            raise v

class AsyncInput(object):
    def __init__(self, key):
        self.key = key
    
    def set_channel(self, channel):
        self.channel = channel
    
    def __iter__(self):
        return self
    
    def next(self):
        self.channel.send(('pull', self.key))
        t, v = self.channel.receive()
        if t == 'next':
            return v
        else:
            raise v

def async(*input_names, **kwargs):
    def decorator(func):
        # pop all expected keyword arguments
        if kwargs:
            raise TypeError("async() got an unexpected keyword argument '%s'" % kwargs.keys()[0])
        
        def wrapper(*args, **kwargs):
            return AsyncJob(func, args, kwargs, input_names)
        return wrapper
    
    if len(input_names) == 1 and len(kwargs) == 0 and '__call__' in dir(input_names[0]):
        func = input_names[0]
        input_names = []
        return decorator(func)
    else:
        return decorator

def generator_map(func, *inputs):
    generators = list(i.__iter__() for i in inputs)
    while True:
        has_next = False
        values = []
        for g in generators:
            try:
                values.append(g.next())
                has_next = True
            except StopIteration:
                values.append(None)
        if not has_next:
            raise StopIteration
        yield func(*values)
