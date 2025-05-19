class ObjectLoader2:

    def __init__(self, concurrency, tags, post_hook=None):
        self.lock      = Lock()
        self.executor  = ThreadPoolExecutor(max_workers=int(concurrency))
        self.queue     = ObjectLoaderQueue2(concurrency=int(concurrency), tags=tags)
        self.post_hook = post_hook

    def run(self):
        self.__log(f"Concurrency : {self.queue.global_maximum_concurrency}")
        self.__log(f"Connections : {len(self.queue.queued)}")
        self.__log(f"Objects : {self.queue.length}\n")
        futures = []
        while self.queue.not_empty():
            # Get eligible objects including connection details from queue and submit them to executor.
            while object := self.queue.get():
                futures.append(self.executor.submit(self.__load_object, object))
            # Check for completed futures and report back to queue.
            completed = 0
            for i, future in enumerate(futures):
                if future.done():
                    future = futures.pop(i)
                    self.queue.complete(future.result())
                    completed += 1
            if completed > 0:
                # Resort queue to maximize concurrency utilization.
                self.queue.sort()
            # Take a short nap so the main thread is not constantly calling queue.not_empty() and queue.get().
            sleep(0.1)

    def __load_object(self, object):
        attempt = 0
        start = datetime.now()
        while True:
            try:
                attempt += 1
                if attempt == 1:
                    self.__log(f"{start.time().strftime('%H:%M:%S')}  [Starting]   {object.ObjectName}")
                else:
                    self.__log(f"{datetime.now().time().strftime('%H:%M:%S')}  [Retrying]   {object.ObjectName}")
                object.loader_result = 10
                if self.post_hook is not None:
                    try:
                        self.post_hook(object)
                    except Exception as e:
                        object.loader_exception = e
                break
            except Exception as e:
                if attempt >= 2:
                    object.loader_exception = e
                    break
                sleep(5)
        end = datetime.now()
        object.loader_duration = int(round((end - start).total_seconds(), 0))
        if hasattr(object, "loader_exception"):
            object.loader_status = "Failed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Failed]     {object.ObjectName} ({object.loader_duration} Seconds) ({attempt} Attempts)")
        else:
            object.loader_status = "Completed"
            self.__log(f"{end.time().strftime('%H:%M:%S')}  [Completed]  {object.ObjectName} ({object.loader_duration} Seconds) ({object.loader_result} Rows)")
        return object

    def __log(self, message):
        self.lock.acquire()
        print(message)
        self.lock.release()

    def print_errors(self):
        failed = 0
        for object in self.queue.completed.values():
            if object.loader_status == "Failed":
                failed += 1
                self.__log(f"{object.ObjectName}:")
                self.__log(object.loader_exception)
        if failed == 0:
            self.__log("No errors")
