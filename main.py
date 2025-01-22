import os
from flask import Flask, request, jsonify, make_response
from pymongo import MongoClient, errors

# Load environment variables
MONGO_PASSWORD = os.getenv("MONGO_PASS")  # Replace with actual environment variable name

# Construct MongoDB URI using password
MONGO_URI = f"mongodb+srv://arora707vicky:{MONGO_PASSWORD}@gigs.7jzwe.mongodb.net/?retryWrites=true&w=majority&appName=gigs"

# Set up Flask app
app = Flask(__name__)

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client['aggregator']
    users_collection = db['users']
    gigs_collection = db['gigs']
    platform_analytics_collection = db['platform_analytics']
except errors.PyMongoError as e:
    raise RuntimeError(f"Failed to connect to MongoDB: {str(e)}")

# Flask API endpoints
@app.route("/users", methods=["GET"])
def get_users():
    try:
        users = list(users_collection.find({}, {"_id": 0}))
        return jsonify({"status": "success", "data": users}), 200
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to fetch users: {str(e)}"}), 500

@app.route("/gigs", methods=["GET"])
def get_gigs():
    try:
        gigs = list(gigs_collection.find({}, {"_id": 0}))
        return jsonify({"status": "success", "data": gigs}), 200
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to fetch gigs: {str(e)}"}), 500

@app.route("/platform_analytics", methods=["GET"])
def get_platform_analytics():
    try:
        analytics = list(platform_analytics_collection.find({}, {"_id": 0}))
        return jsonify({"status": "success", "data": analytics}), 200
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to fetch platform analytics: {str(e)}"}), 500

@app.route("/users", methods=["POST"])
def add_user():
    if not request.is_json:
        return jsonify({"status": "fail", "message": "Request must be in JSON format"}), 400
    try:
        user_data = request.json
        if not user_data:
            return jsonify({"status": "fail", "message": "Missing user data"}), 400
        users_collection.insert_one(user_data)
        return jsonify({"status": "success", "message": "User added successfully"}), 201
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to add user: {str(e)}"}), 500

@app.route("/gigs", methods=["POST"])
def add_gig():
    if not request.is_json:
        return jsonify({"status": "fail", "message": "Request must be in JSON format"}), 400
    try:
        gig_data = request.json
        if not gig_data:
            return jsonify({"status": "fail", "message": "Missing gig data"}), 400
        gigs_collection.insert_one(gig_data)
        return jsonify({"status": "success", "message": "Gig added successfully"}), 201
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to add gig: {str(e)}"}), 500

@app.route("/platform_analytics", methods=["POST"])
def add_platform_analytics():
    if not request.is_json:
        return jsonify({"status": "fail", "message": "Request must be in JSON format"}), 400
    try:
        analytics_data = request.json
        if not analytics_data:
            return jsonify({"status": "fail", "message": "Missing analytics data"}), 400
        platform_analytics_collection.insert_one(analytics_data)
        return jsonify({"status": "success", "message": "Platform analytics data added successfully"}), 201
    except errors.PyMongoError as e:
        return jsonify({"status": "error", "message": f"Failed to add platform analytics: {str(e)}"}), 500

@app.errorhandler(404)
def handle_404(e):
    return jsonify({"status": "fail", "message": "Resource not found"}), 404

@app.errorhandler(405)
def handle_405(e):
    return jsonify({"status": "fail", "message": "Method not allowed"}), 405

@app.errorhandler(500)
def handle_500(e):
    return jsonify({"status": "fail", "message": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(debug=True)
