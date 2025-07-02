import redis
import requests
import os
import sys
import signal
from rq import Queue, Worker

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

class SentimentWorker:
    def __init__(self):
        self.redis_conn = None
        self.queue = None
        self.worker = None
        self.running = False
        
    def connect_redis(self):
        try:
            self.redis_conn = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db = 0
            )
            self.redis_conn.ping()
            return True
        except redis.ConnectionError as e:
            print("Redis conn failed: ")
            return False
    
    def setup_queue(self):
        try:
            self.queue = Queue('sentiment_analysis', connection=self.redis_conn)
            return True
        except Exception as e:
            return False
    
    def check_dependencies(self):
        try:
            response = requests.get(f"{ML_SERVICE_URL}/health", timeout=5)
            if response.status_code == 200:
                print(f"ML service accessible at {ML_SERVICE_URL}")
            else:
                print(f" MLservice responded with status {response.status_code}")
        except Exception as e:
            return False
        
        try:
            from shared.database import test_connection
            if test_connection():
                print("Database connection ok")
            else:
                print("Database connection not ok")
                return False
        except ImportError:
            print("Database not found,")
        
        return True
    
    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            print(f"Received signal {signum}, shutting down gracefully...")
            self.shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def start(self):
        print("Starting Service")
        
        self.setup_signal_handlers()
    
        if not self.connect_redis():
            print("Failed to connect to Redis.Exiting.")
            sys.exit(1)
        
        if not self.setup_queue():
            print("Failed to setup queue.Exiting.")
            sys.exit(1)
        
        if not self.check_dependencies():
            print("Some dependencies are not accessible")
        
        print(f"Queue: {self.queue.name}")
        print(f"Pending jobs: {len(self.queue)}")
        print(f"Failed jobs: {len(self.queue.failed_job_registry)}")
        print("Worker ready")
        

        self.running = True
        try:
            self.worker = Worker([self.queue], name=f"sentiment-worker-{os.getpid()}", connection=self.redis_conn)
            self.worker.work(with_scheduler=True)
        except KeyboardInterrupt:
            print("Worker stopped")
        except Exception as e:
            print(f"Worker error: {e}")
            raise
        finally:
            self.shutdown()  
    def shutdown(self):
        if self.running:
            print("Shutting down worker service")
            self.running = False
            
            if self.worker:
                print("Waiting for current job to complete")
            
            if self.redis_conn:
                self.redis_conn.close()
                print("Redis connection closed")
            
            print("Worker service stopped")

def main():
    try:
        worker_service = SentimentWorker()
        worker_service.start()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()