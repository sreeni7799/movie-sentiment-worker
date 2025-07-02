from pymongo import MongoClient
import re
import os

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DATABASE_NAME = os.getenv('DATABASE_NAME', 'sentiment_db')
COLLECTION_NAME = os.getenv('COLLECTION_NAME', 'results')

client = None
mongo_db = None
results_collection = None

def initialize_database():
    global client, mongo_db, results_collection
    
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        print(f"Connected to MongoDB at {MONGO_URI}")
        
        mongo_db = client[DATABASE_NAME]
        results_collection = mongo_db[COLLECTION_NAME]
        
        existing_count = results_collection.count_documents({})
        if existing_count > 0:
            print("records found")
        else:
            print(f"Database empty")
        
        return True
        
    except Exception as e:
        print(f"Failed MongoDB conn.")
        client = None
        mongo_db = None
        results_collection = None
        return False

def test_connection():
    try:
        if client is None:
            initialize_database()
        
        if client is not None:
            client.admin.command('ping')
            return True
        return False
    except:
        return False

def insert_results(batch):
    if results_collection is None:
        initialize_database()
        if results_collection is None:
            print("db not ready")
            return 0

    if not batch:
        print("nothing")
        return 0

    try:
        try:
            mongo_db[COLLECTION_NAME].delete_many({})
        except:
            print("delete failed")
        inserted = results_collection.insert_many(batch)
        return len(inserted.inserted_ids)

    except:
        print("fail insert")
        return 0


def fetch_results_from_db():
    if results_collection is None:
        if not initialize_database():
            return []
    
    try:
        cursor = results_collection.find({}, {"_id": 0})
        results = list(cursor)
        return results
        
    except Exception as e:
        print("Failed to get results")
        return []

def search_movies_by_sentiment(movie_name=None, sentiment=None):
    if results_collection is None:
        if not initialize_database():
            print("MongoDB not connected")
            return []
    
    try:
        query = {}
        search_terms = []
        
        if movie_name and movie_name.strip():
            query["movie_name"] = {
                "$regex": re.escape(movie_name.strip()),
                "$options": "i"  # case-insensitive
            }
            search_terms.append(f"movie name containing '{movie_name.strip()}'")
        
        if sentiment and sentiment.strip():
            query["sentiment"] = sentiment.strip().lower()
            search_terms.append(f"sentiment: {sentiment.strip().lower()}")
        
        cursor = results_collection.find(query, {"_id": 0})
        results = list(cursor)
        
        if search_terms:
            search_description = " AND ".join(search_terms)
        else:
            print(f"Retrieved all {len(results)} results (no filters)")
        
        return results
        
    except Exception as e:
        print(f"Search failed: {e}")
        return []

def get_unique_movies():
    if results_collection is None:
        if not initialize_database():
            return []
    
    try:
        unique_movies = results_collection.distinct("movie_name")
        movies = sorted([movie for movie in unique_movies if movie and movie.strip()])
        return movies
        
    except Exception as e:
        print(f" Failed to get unique movies: {e}")
        return []

def get_sentiment_summary(movie_name=None):
    if results_collection is None:
        if not initialize_database():
            return []
    
    try:
        pipeline = []
        
        if movie_name and movie_name.strip():
            pipeline.append({
                "$match": {
                    "movie_name": {
                        "$regex": re.escape(movie_name.strip()),
                        "$options": "i"
                    }
                }
            })
        
        pipeline.extend([
            {
                "$group": {
                    "_id": {
                        "movie_name": "$movie_name",
                        "sentiment": "$sentiment"
                    },
                    "count": {"$sum": 1},
                    "avg_confidence": {"$avg": "$confidence"}
                }
            },
            {
                "$group": {
                    "_id": "$_id.movie_name",
                    "sentiments": {
                        "$push": {
                            "sentiment": "$_id.sentiment",
                            "count": "$count",
                            "avg_confidence": "$avg_confidence"
                        }
                    },
                    "total_reviews": {"$sum": "$count"}
                }
            },
            {"$sort": {"_id": 1}}
        ])
        
        cursor = results_collection.aggregate(pipeline)
        summary = list(cursor)
        
        return summary
        
    except Exception as e:
        return []

def get_database_stats():
    if results_collection is None:
        if not initialize_database():
            return {"status": "disconnected", "error": "MongoDB not connected"}
    
    try:
        total_docs = results_collection.count_documents({})
        unique_movies = len(results_collection.distinct("movie_name"))
        
        positive_count = results_collection.count_documents({"sentiment": "positive"})
        negative_count = results_collection.count_documents({"sentiment": "negative"})
        
        stats = {
            "status": "connected",
            "connection_type": "shared",
            "total_documents": total_docs,
            "unique_movies": unique_movies,
            "positive_reviews": positive_count,
            "negative_reviews": negative_count,
            "database_name": DATABASE_NAME,
            "collection_name": COLLECTION_NAME,
            "mongo_uri": MONGO_URI.split('@')[-1] if '@' in MONGO_URI else MONGO_URI 
        }
        
        return stats
        
    except Exception as e:
        return {"status": "error", "error": str(e)}

def clear_results_collection():
    if results_collection is None:
        if not initialize_database():
            return 0
    
    try:
        result = results_collection.delete_many({})
        count = result.deleted_count
        print(f"Cleared {count} results from database")
        return count
        
    except Exception as e:
        return 0

if not initialize_database():
    print("Database failed")