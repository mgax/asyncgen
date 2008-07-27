import pprocess

def _async_process(func, args, kwargs, input_names):
    channel = pprocess.create()
    if channel.pid != 0:
        return channel
    
    for i in input_names:
        kwargs[i].set_channel(channel)
    
    try:
        gen = func(*args, **kwargs).__iter__()
        channel.send(('ready', None))
        
        while(True):
            cmd = channel.receive()
            if cmd == 'pull_output':
                try:
                    v = gen.next()
                    channel.send(('next_value', v))
                except StopIteration, e:
                    channel.send(('stop_iteration', e))
            elif cmd == 'quit':
                break
            else:
                raise NotImplementedError('_async_process: command "%s" not implemented' % cmd)
    
    except Exception, e:
        channel.send(('exception', e))
    
    finally:
        pprocess.exit(channel)

class WorkerQueue(pprocess.Exchange):
    def __init__(self, *args, **kwargs):
        pprocess.Exchange.__init__(self, *args, **kwargs)
        self.queue = []
    
    def store_data(self, channel):
        channel.worker.store_data()
    
    def tick(self):
        if self.active():
            self.store()

class Worker(object):
    def __init__(self, channel, job):
        self.channel = channel
        self.job = job
        channel.worker = self
    
    def send(self, msg):
        self.channel.send(msg)
    
    def store_data(self):
        self.job.worker_has_message(self, self.channel.receive())

class AsyncInput(object):
    def __init__(self, key):
        self.key = key
    
    def set_channel(self, channel):
        self.channel = channel
    
    def __iter__(self):
        return self
    
    def next(self):
        self.channel.send(('pull_input', self.key))
        t, v = self.channel.receive()
        if t == 'next':
            return v
        else:
            raise v

class AsyncJob(object):
    def __init__(self, func, args, kwargs, input_names, options):
        self.idle_workers = []
        self.workers_waiting_input = []
        self.ready_data = []
        self.n_workers = options['workers']
        self.worker_queue = WorkerQueue()
        self.input = {}
        self.exception_flag = False
        
        for name in input_names:
            try:
                gen = kwargs[name]
            except KeyError:
                raise ValueError('Did not find async input named "%s" - did you pass it as a named argument?' % name)
            if '__iter__' not in dir(gen):
                raise TypeError('Expected all the async inputs to be generators')
            
            kwargs[name] = AsyncInput(name)
            self.input[name] = gen.__iter__()
        
        for c in range(self.n_workers):
            channel = self.launch_worker(func, args, kwargs, input_names)
            self.idle_workers.insert(0, Worker(channel, self))
            self.worker_queue.add(channel)
    
    def launch_worker(self, func, args, kwargs, input_names):
        channel = _async_process(func, args, kwargs, input_names)
        t, v = channel.receive()
        
        if t == 'ready':
            return channel
        elif t == 'exception':
            raise v
        else:
            raise RuntimeError('Child process did not start up correctly')
    
    def wait_for_workers(self):
        self.worker_queue.tick()
        waiting = self.workers_waiting_input
        self.workers_waiting_input = []
        for worker, name in waiting:
            try:
                v = self.input[name].next()
                worker.send(('next', v))
            except Exception, e:
                worker.send(('exception', e))
    
    def worker_has_message(self, worker, message):
        t, v = message
        if t == 'pull_input':
            self.workers_waiting_input.append((worker, v))
        elif t == 'next_value':
            self.ready_data.insert(0, ('next', v))
            self.idle_workers.insert(0, worker)
        elif t == 'stop_iteration':
            worker.send('quit')
            self.n_workers -= 1
            if self.n_workers:
                self.pull_value()
        elif t == 'exception':
            self.ready_data = [('exception', v)]
        else:
            raise NotImplementedError('AsyncJob.worker_has_message: message "%s" not implemented' % t)
    
    def pull_value(self):
        self.idle_workers.pop().send('pull_output')
    
    def __iter__(self):
        return self
    
    def next(self):
        if self.exception_flag:
            raise StopIteration
        
        if not self.ready_data and self.n_workers:
            self.pull_value()
        
        while self.n_workers and not self.ready_data:
            self.wait_for_workers()
        
        if not self.n_workers:
            raise StopIteration
        
        t, v = self.ready_data.pop()
        if t == 'next':
            return v
        elif t == 'exception':
            self.exception_flag = True
            raise v
        else:
            raise NotImplementedError

def async(*input_names, **kwargs):
    def decorator(func):
        workers = kwargs.pop('workers', 1)
        if kwargs:
            raise TypeError("async() got an unexpected keyword argument '%s'" % kwargs.keys()[0])
        
        def wrapper(*args, **kwargs):
            return AsyncJob(func, args, kwargs, input_names, {'workers': workers})
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
