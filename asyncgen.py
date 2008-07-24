import pprocess

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
    def __init__(self, func, args, kwargs, async_kwargs=None):
        if async_kwargs:
            async_inputs = [AsyncInput()]
            kwargs = dict(kwargs, **{async_kwargs.keys()[0]: async_inputs[0]})
            self.input = async_kwargs.values()[0].__iter__()
        else:
            async_inputs = []
            self.input = None
        
        self.channel = _async_process(func, args, kwargs, async_inputs)
    
    def __iter__(self):
        return self
    
    def next(self):
        t, v = self.channel.receive()
        
        while t == 'pull':
            try:
                iv = self.input.next()
                self.channel.send(('next', iv))
            except Exception, ie:
                self.channel.send(('exception', ie))
            
            t, v = self.channel.receive()
        
        if t == 'next':
            return v
        else:
            raise v

def async(func):
    def wrapper(*args, **kwargs):
        return AsyncJob(func, args, kwargs)
    return wrapper

def async_with_input(**async_kwargs):
    if len(async_kwargs) > 1:
        raise NotImplemented('@async_with_input currently supports a single input source')
    
    if len(async_kwargs) == 0:
        async_kwargs = None
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            return AsyncJob(func, args, kwargs, async_kwargs)
        return wrapper
    return decorator

class AsyncInput(object):
    def set_channel(self, channel):
        self.channel = channel
    
    def __iter__(self):
        return self
    
    def next(self):
        self.channel.send(('pull', None))
        t, v = self.channel.receive()
        if t == 'next':
            return v
        else:
            raise v
