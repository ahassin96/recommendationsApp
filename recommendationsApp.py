from flask import Flask, jsonify
from neo4j import GraphDatabase
from pymongo import MongoClient
from bson import ObjectId
from flask_cors import CORS
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)
CORS(app)

uri = os.getenv("NEO4J_URI", "bolt://ec2-54-88-88-30.compute-1.amazonaws.com:7687")
username = os.getenv("NEO4J_USERNAME", "default_username")
password = os.getenv("NEO4J_PASSWORD", "default_password")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://ec2-54-221-90-30.compute-1.amazonaws.com:27017")
client = MongoClient(MONGO_URI)
mongo_collection = client.admin["movies"]

def get_watch_history(user_id):
    with GraphDatabase.driver(uri, auth=(username, password)) as driver:
        with driver.session() as session:
            result = session.run("MATCH (u:User {user_id: $user_id})-[:WATCHED]->(m:Video) RETURN m.video_id", user_id=user_id)
            return [record["m.video_id"] for record in result]

def get_video_tags(video_id):
    video_document = mongo_collection.find_one({"_id": ObjectId(video_id)})
    return video_document.get("tags", []) if video_document else []

@app.route('/recommendations/<string:user_id>', methods=['GET'])
def get_recommendations(user_id):
    try:
        watch_history = get_watch_history(user_id)
        recommendations_set = set()

        for watched_video_id in watch_history:
            tags = get_video_tags(watched_video_id)
            similar_films = mongo_collection.find({"tags": {"$in": tags}, "_id": {"$nin": watch_history}})
            recommendations_set.update(str(film["_id"]) for film in similar_films if str(film["_id"]) not in watch_history)

        recommendations = [{'video_id': video_id, 'tags': get_video_tags(video_id)} for video_id in recommendations_set]

        return jsonify({'user_id': user_id, 'recommendations': recommendations})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=9092, debug=True)
