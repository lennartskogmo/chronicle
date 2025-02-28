class ObjectLoader:

    def __init__(self, concurrency, tag, post_hook=None):
        self.lock      = Lock()
        self.executor  = ThreadPoolExecutor(max_workers=int(concurrency))
        self.queue     = ObjectLoaderQueue(concurrency=int(concurrency), tag=tag)
        self.post_hook = post_hook

    def run(self):
        futures = []
        print(f"Concurrency : {self.queue.global_maximum_concurrency}")
        print(f"Objects : {self.queue.length}\n")
        while self.queue.not_empty():
            # Get eligible objects including connection details from queue and submit them to executor.
            while object := self.queue.get():
                connection = self.queue.connections[object["ConnectionName"]]
                connection_with_secrets = self.queue.connections_with_secrets[object["ConnectionName"]]
                futures.append(self.executor.submit(self.__load_object, object, connection, connection_with_secrets))
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
    
    def __load_object(self, object, connection, connection_with_secrets):
        attempt = 0
        start = datetime.now()
        while True:
            try:
                attempt += 1
                self.lock.acquire()
                if attempt == 1:
                    print(f"{start.time().strftime('%H:%M:%S')}  [Starting]   {object['ObjectName']}")
                else:
                    print(f"{datetime.now().time().strftime('%H:%M:%S')}  [Retrying]   {object['ObjectName']}")
                self.lock.release()
                object["__rows"] = load_object(object, connection, connection_with_secrets)
                if self.post_hook is not None:
                    post_hook = self.post_hook
                    post_hook(object)
                break
            except Exception as e:
                if attempt >= 2:
                    object["__exception"] = e
                    break
                sleep(5)
        end = datetime.now()
        object["__duration"] = int(round((end - start).total_seconds(), 0))
        self.lock.acquire()
        if "__rows" in object:
            object["__status"] = "Completed"
            print(f"{end.time().strftime('%H:%M:%S')}  [Completed]  {object['ObjectName']} ({object['__duration']} Seconds) ({object['__rows']} Rows)")
        else:
            object["__status"] = "Failed"
            print(f"{end.time().strftime('%H:%M:%S')}  [Failed]     {object['ObjectName']} ({object['__duration']} Seconds) ({attempt} Attempts)")
        self.lock.release()
        return object
    
    def print_errors(self):
        failed = 0
        for object in self.queue.completed.values():
            if object["__status"] == "Failed":
                failed += 1
                print(f"{object['ObjectName']}:")
                print(object["__exception"])
        if failed == 0:
            print("No errors")
