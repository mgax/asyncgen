import pprocess

def _async_process(func, args, kwargs):
    channel = pprocess.create()
    if channel.pid != 0:
        return channel
    
    ret = func(*args, **kwargs)
    try:
        for msg in ret:
            channel.send(('next', msg))
    except Exception, e:
        channel.send(('exception', e))
    
    channel.send(('exception', StopIteration()))
    
    pprocess.exit(channel)

class AsyncJob(object):
    def __init__(self, func, args, kwargs):
        self.channel = _async_process(func, args, kwargs)
    
    def __iter__(self):
        return self
    
    def next(self):
        ty, val = self.channel.receive()
        if ty == 'next':
            return val
        else:
            raise val

def async(func):
    def wrapper(*args, **kwargs):
        return AsyncJob(func, args, kwargs)
    
    return wrapper
