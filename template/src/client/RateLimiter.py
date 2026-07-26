import threading


class RateLimiter:
    def __init__(self, pool_size):
        self.semaphore = threading.Semaphore(pool_size)
    def acquire(self):
        self.semaphore.acquire()
    def release(self):
        self.semaphore.release()
