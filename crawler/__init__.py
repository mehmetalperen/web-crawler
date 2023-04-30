from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):       # default parameters 
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)     # frontier object
        self.workers = list()
        self.worker_factory = worker_factory                  # worker object

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]          # make a thread for i in range(threads_count-->should be 4 threads ids 0-3), each with worker_id, config and frontier object
        for worker in self.workers:
            worker.start()           # calls threading start() bc it's a worker object, not start() on line 20 for Crawler object

    def start(self):
        self.start_async()
        self.join()

    def join(self):
        for worker in self.workers:
            worker.join()
