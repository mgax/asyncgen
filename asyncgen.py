import pprocess

def generator_map(func, *generators):
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

def _async_process(func, args, kwargs, inputs):
    channel = pprocess.create()
    if channel.pid != 0:
        return channel
    
    try:
        for i in inputs:
            i.set_channel(channel)
        
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
    def __init__(self, func, args, kwargs, async_kwargs={}):
        async_inputs = {}
        self.input = {}
        
        for key, value in async_kwargs.iteritems():
            async_inputs[key] = AsyncInput(key)
            self.input[key] = value.__iter__()
        
        kwargs = dict(kwargs, **async_inputs)
        self.channel = _async_process(func, args, kwargs, async_inputs.values())
    
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

def async(*input_names, **async_kwargs):
    def decorator(func):
        def wrapper(*args, **kwargs):
            input_generators = {}
            for name in input_names:
                try:
                    gen = kwargs[name]
                except KeyError:
                    # TODO: testme
                    raise ValueError('Did not find async input named "%s" - did you pass it as a named argument?' % name)
                if '__iter__' not in dir(gen):
                    raise TypeError('Expected all the async inputs to be generators')
                input_generators[name] = gen
                del kwargs[name]
            return AsyncJob(func, args, kwargs, input_generators)
        return wrapper
    
    if len(input_names) == 1 and len(async_kwargs) == 0 and '__call__' in dir(input_names[0]):
        func = input_names[0]
        input_names = []
        return decorator(func)
    else:
        return decorator

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
