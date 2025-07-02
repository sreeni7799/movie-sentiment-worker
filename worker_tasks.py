import requests
import os
from datetime import datetime
from typing import List, Dict, Any

ML_SERVICE_URL = os.getenv('ML_SERVICE_URL', 'http://localhost:8000')

def process_sentiment_batch(reviews_batch: List[Dict[str, str]]) -> Dict[str, Any]:
    job_start_time = datetime.now()
    
    try:
        if not reviews_batch:
            raise ValueError("Empty reviews batch provided")
    
        if reviews_batch:
            sample_review = reviews_batch[0]
        
        print(f"Sending batch to ML service: {ML_SERVICE_URL}")
        
        ml_response = requests.post(
            f"{ML_SERVICE_URL}/process-batch",
            json={"reviews": reviews_batch},
            timeout=300, 
            headers={'Content-Type': 'application/json'}
        )
        

        if ml_response.status_code != 200:
            error_msg = f"ML service returned status {ml_response.status_code}: {ml_response.text}"
            print(f" {error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "processed_count": 0,
                "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
            }
        
        ml_data = ml_response.json()
        batch_results = ml_data.get('results', [])
        
        if not batch_results:
            error_msg = "ML service returned no results"
            print(f"{error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "ml_response": ml_data,
                "processed_count": 0,
                "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
            }
        
        timestamp = datetime.now().isoformat()
        for result in batch_results:
            result['timestamp'] = timestamp
            result['processed_by'] = 'background_worker'
            result['processing_mode'] = 'queue_async'
            result['worker_job_id'] = os.getenv('RQ_JOB_ID', 'unknown')
        
        print(f"Storing {len(batch_results)} result")
        
        try:
            from shared.database import insert_results
            insert_count = insert_results(batch_results)
            
            processing_time = (datetime.now() - job_start_time).total_seconds()
            
            print(f"Worker completed successfully:")
            print(f"Processed: {len(batch_results)} reviews")
            print(f" Stored: {insert_count} records")
            print(f"Time: {processing_time:.2f} seconds")
            
            return {
                "success": True,
                "processed_count": len(batch_results),
                "stored_count": insert_count,
                "processing_time_seconds": processing_time,
                "message": "Background processing completed successfully",
                "timestamp": timestamp
            }
            
        except Exception as db_error:
            error_msg = f"Database storage failed: {str(db_error)}"
            print(f"{error_msg}")
            return {
                "success": False,
                "error": error_msg,
                "processed_count": len(batch_results),
                "stored_count": 0,
                "ml_results_available": True,
                "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
            }
        
    except requests.exceptions.Timeout:
        error_msg = "ML service timeout after 5 minutes"
        print(f"{error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "processed_count": 0,
            "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
        }
        
    except requests.exceptions.ConnectionError:
        error_msg = f"Cannot connect to ML service at {ML_SERVICE_URL}"
        print(f"{error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "processed_count": 0,
            "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
        }
        
    except Exception as e:
        error_msg = f"Unexpected worker error: {str(e)}"
        print(f"{error_msg}", exc_info=True)
        return {
            "success": False,
            "error": error_msg,
            "processed_count": 0,
            "processing_time_seconds": (datetime.now() - job_start_time).total_seconds()
        }

def background_search(movie_name: str = None, sentiment: str = None) -> Dict[str, Any]:
    try:
        print(f"Background search: movie='{movie_name}', sentiment='{sentiment}'")
        from shared.database import search_movies_by_sentiment
        
        results = search_movies_by_sentiment(
            movie_name=movie_name if movie_name else None,
            sentiment=sentiment if sentiment else None
        )
        
        print(f"Background search completed: {len(results)} results found")
        
        return {
            "success": True,
            "results": results,
            "total_count": len(results),
            "search_criteria": {
                "movie_name": movie_name or "Any",
                "sentiment": sentiment or "Any"
            }
        }
        
    except Exception as e:
        error_msg = f"Background search error: {str(e)}"
        print(f"{error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "results": [],
            "total_count": 0
        }

def worker_health_check() -> Dict[str, Any]:
    try:
        health_status = {
            "worker_status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        try:
            response = requests.get(f"{ML_SERVICE_URL}/health", timeout=5)
            health_status["checks"]["ml_service"] = {
                "status": "healthy" if response.status_code == 200 else "unhealthy",
                "response_code": response.status_code
            }
        except Exception as e:
            health_status["checks"]["ml_service"] = {
                "status": "unhealthy",
                "error": str(e)
            }
    
        try:
            from shared.database import test_connection
            db_healthy = test_connection()
            health_status["checks"]["database"] = {
                "status": "healthy" if db_healthy else "unhealthy"
            }
        except Exception as e:
            health_status["checks"]["database"] = {
                "status": "unhealthy",
                "error": str(e)
            }
        
        return health_status
        
    except Exception as e:
        return {
            "worker_status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }