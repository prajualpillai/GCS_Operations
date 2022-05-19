import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd

THREAD_POOL_SIZE = 20
thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)

pending_threads: dict = {
    "threads": []
}


def wait_for_ops():
    while pending_threads["threads"]:
        thread = pending_threads["threads"].pop()
        thread.result()


class MultiThreading():

    def __init__(self):
        self.actual = []
        self.lock = threading.Lock()

    def add_data(self, x):
        x += 25
        self.lock.acquire()
        self.actual.append(x)
        self.lock.release()

    def add_async(self):
        for i in range(10):
            add_thread = thread_pool.submit(
                self.add_data,
                x=np.random.randint(1, 100)
            )
            pending_threads["threads"].append(add_thread)

        wait_for_ops()

        return pd.DataFrame(self.actual, columns=["array_data"])

    def core_fn(self):
        df = self.add_async()
        print(df)


obj = MultiThreading()
obj.core_fn()
