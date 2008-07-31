import tempfile
import os
from cPickle import dump as pickle_dump, load as pickle_load

import pprocess

def _unpickle_and_remove_file(f):
    data = pickle_load(open(f, 'rb'))
    os.remove(f)
    return data

def _pickle_and_return_filename(data):
    f = tempfile.mkstemp()[1]
    pickle_dump(data, open(f, 'wb'))
    return f

def _async_process(func, args, kwargs, input_names):
    channel = pprocess.create()
    if channel.pid != 0:
        return channel
    
    for i in input_names:
        kwargs[i].set_channel(channel)
    
    try:
        try:
            gen = func(*args, **kwargs).__iter__()
            channel.send(('ready', None))
            
            def get_next_value(tempfile_output=False):
                try:
                    v = gen.next()
                    if tempfile_output:
                        return ('next_value_tempfile', _pickle_and_return_filename(v))
                    else:
                        return ('next_value', v)
                except StopIteration, e:
                    return ('stop_iteration', e)
            
            while(True):
                cmd = channel.receive()
                if cmd == 'pull_output':
                    channel.send(get_next_value())
                elif cmd == 'pull_output_tempfile':
                    channel.send(get_next_value(tempfile_output=True))
                elif cmd == 'quit':
                    break
                else:
                    raise NotImplementedError('_async_process: command "%s" not implemented' % cmd)
        
        except Exception, e:
            channel.send(('exception', e))
    
    finally:
        pprocess.exit(channel)

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
        if t == 'next_input':
            return v
        elif t == 'next_input_tempfile':
            return _unpickle_and_remove_file(v)
        elif t == 'exception':
            raise v
        else:
            raise NotImplemented

class WorkerQueue(pprocess.Exchange):
    def __init__(self, *args, **kwargs):
        pprocess.Exchange.__init__(self, *args, **kwargs)
        self.queue = []
        self.jobs = []
    
    def store_data(self, channel):
        channel.worker.store_data()
    
    def tick(self):
        for job in self.jobs:
            job.do_pre_poll()
        if self.active():
            self.store()
    
    def register(self, job):
        self.jobs.append(job)

class Worker(object):
    def __init__(self, channel, job):
        self.channel = channel
        self.job = job
        channel.worker = self
    
    def send(self, msg):
        self.channel.send(msg)
    
    def store_data(self):
        self.job.worker_has_message(self, self.channel.receive())

_async_job_global_queue = WorkerQueue()
class AsyncJob(object):
    def __init__(self, func, args, kwargs, input_names, options):
        self.idle_workers = []
        self.busy_workers = []
        self.workers_waiting_input = []
        self.ready_data = []
        self.buffer_size = options['buffer_size']
        self.tempfile_output = options['tempfile_output']
        self.worker_queue = _async_job_global_queue
        self.worker_queue.register(self)
        self.input = {}
        self.waiting_data = 0
        self.stop_iteration = False
        
        for name in input_names:
            try:
                gen = kwargs[name]
            except KeyError:
                raise ValueError('Did not find async input named "%s" - did you pass it as a named argument?' % name)
            if '__iter__' not in dir(gen):
                raise TypeError('Expected all the async inputs to be generators')
            
            kwargs[name] = AsyncInput(name)
            self.input[name] = gen.__iter__()
        
        for c in range(options['workers']):
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
        elif t == 'pull_input':
            # a worker function is requesting input before we pull anything
            # from it; this means it's not a generator function.
            raise NotImplementedError('All async functions must be generators')
        else:
            raise RuntimeError('Child process did not start up correctly')
    
    def do_pre_poll(self):
        """
        make sure no workers are blocking on us, to avoid deadlocks
        """
        
        while self.idle_workers and \
                (len(self.ready_data) + len(self.busy_workers)) \
                < (self.buffer_size + self.waiting_data):
            worker = self.idle_workers.pop()
            self.busy_workers.append(worker)
            if self.tempfile_output:
                worker.send('pull_output_tempfile')
            else:
                worker.send('pull_output')
        
        if not (self.idle_workers or self.busy_workers or self.ready_data):
            self.stop_iteration = True
        
        while self.workers_waiting_input:
            worker, name = self.workers_waiting_input.pop()
            try:
                input_source = self.input[name]
                if isinstance(input_source, AsyncJob) and input_source.tempfile_output:
                    v = input_source.next(want_tempfile=True)
                    worker.send(('next_input_tempfile', v))
                else:
                    v = input_source.next()
                    worker.send(('next_input', v))
            except Exception, e:
                worker.send(('exception', e))
    
    def worker_has_message(self, worker, message):
        t, v = message
        if t == 'pull_input':
            self.workers_waiting_input.insert(0, (worker, v))
        elif t in ('next_value', 'next_value_tempfile'):
            self.ready_data.insert(0, (t, v))
            self.busy_workers.remove(worker)
            self.idle_workers.insert(0, worker)
        elif t == 'stop_iteration':
            worker.send('quit')
            self.busy_workers.remove(worker)
            self.worker_queue.remove(worker.channel)
            self.do_pre_poll()
        elif t == 'exception':
            self.ready_data = [('exception', v)]
        else:
            raise NotImplementedError('AsyncJob.worker_has_message: message "%s" not implemented' % t)
    
    def __iter__(self):
        return self
    
    def _request_data(self):
        self.waiting_data += 1
    
    def _wait_for_next(self, callback=lambda: False):
        while not (self.ready_data or self.stop_iteration or callback()):
            self.worker_queue.tick()
    
    def _get_data(self, want_tempfile=False):
        self.waiting_data -= 1
        
        if self.stop_iteration:
            raise StopIteration
        
        t, v = self.ready_data.pop()
        if t == 'next_value':
            if want_tempfile:
                raise RuntimeError('tempfile data was requested; worker returned normal data')
            else:
                return v
        elif t == 'next_value_tempfile':
            if want_tempfile:
                return v
            else:
                return _unpickle_and_remove_file(v)
        elif t == 'exception':
            self.stop_iteration = True
            raise v
        else:
            raise NotImplementedError
    
    def next(self, want_tempfile=False):
        self._request_data()
        self._wait_for_next()
        return self._get_data(want_tempfile)

class SplitterOutput(object):
    def __init__(self, splitter, key):
        self.splitter = splitter
        self.key = key
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.splitter._pull(self.key)

class Splitter(object):
    def __init__(self, input_generator, keys):
        self.input = input_generator.__iter__()
        self.queues = dict( (key, []) for key in keys )
        self.waiting_for_next = False
    
    def get(self, key):
        if key not in self.queues.keys():
            raise KeyError('Splitter: the key you asked for, %s, was not in the list of keys to retrieve' % str(key))
        return SplitterOutput(self, key)
    
    def __getitem__(self, key):
        return self.get(key)
    
    def _pull_input(self):
        if isinstance(self.input, AsyncJob):
            if not self.waiting_for_next:
                self.waiting_for_next = True
                self.input._request_data()
            
            self.input._wait_for_next(callback=lambda: not self.waiting_for_next)
            
            if self.waiting_for_next:
                try:
                    data = self.input._get_data()
                finally:
                    self.waiting_for_next = False
            else:
                return
        
        else:
            data = self.input.next()
        
        for key, queue in self.queues.iteritems():
            queue.insert(0, data[key])
    
    def _pull(self, key):
        queue = self.queues[key]
        if not queue:
            self._pull_input()
        if not queue:
            raise StopIteration
        return queue.pop()

def async(*input_names, **kwargs):
    def decorator(func):
        options = {
            'workers': kwargs.pop('workers', 1),
            'buffer_size': kwargs.pop('buffer', 0),
            'tempfile_output': kwargs.pop('tempfile_output', False),
        }
        if kwargs:
            raise TypeError("async() got an unexpected keyword argument '%s'" % kwargs.keys()[0])
        
        def wrapper(*args, **kwargs):
            return AsyncJob(func, args, kwargs, input_names, options)
        return wrapper
    
    if len(input_names) == 1 and len(kwargs) == 0 and '__call__' in dir(input_names[0]):
        func = input_names[0]
        input_names = []
        return decorator(func)
    else:
        return decorator

def generator_splitter(input_generator, keys):
    return Splitter(input_generator, keys)

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
