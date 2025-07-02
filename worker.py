import redis
import sys

def clear_redis_queues():
    try:
        redis_conn = redis.Redis(
            host='localhost',
            port=6379,
            decode_responses=False 
        )
        
        redis_conn.ping()
        print("Connected to Redis")
        
        rq_keys = redis_conn.keys('rq:*')
        
        if rq_keys:    
            deleted = redis_conn.delete(*rq_keys)
        else:
            print("No RQ keys found")
        
        queue_keys = redis_conn.keys('*sentiment_analysis*')
        if queue_keys:
            deleted = redis_conn.delete(*queue_keys)        
        
    except Exception as e:
        print(f"Redis cleanup failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    clear_redis_queues()