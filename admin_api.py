# admin_api.py (ENHANCED - WITH INPUT VALIDATION for Full 0.5M)
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import mysql.connector
from typing import List
import uvicorn
import secrets
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import re  # ✅ NEW IMPORT for validation

app = FastAPI(title="Admin Control Service")

# --- ✅ NEW: Topic Validation Regex ---
TOPIC_REGEX = re.compile(r"^[a-zA-Z0-9_-]+$")

# --- (CORS Middleware is unchanged) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

# --- (Security Setup is unchanged) ---
security = HTTPBasic()

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, "admin")
    correct_password = secrets.compare_digest(credentials.password, "admin123")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- (Database Config & get_conn are unchanged) ---
DB_CONFIG = {
    "host": "localhost",
    "user": "kafkauser",
    "password": "Kafka123$team",
    "database": "kafka_dynamic"
}
def get_conn(): return mysql.connector.connect(**DB_CONFIG)

# --- NEW: Logging Helper Function ---
def log_action(message, source='API'):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO admin_logs (source, message) VALUES (%s, %s)",
            (source, message)
        )
        conn.commit()
    except Exception as e:
        print(f"!!! LOGGING FAILED: {e} !!!")
    finally:
        if conn.is_connected():
            conn.close()

# --- Protected Endpoints (UPDATED with Logging) ---

@app.get("/")
async def get_frontend(username: str = Depends(get_current_user)):
    return FileResponse("frontend/index.html")

class Topic(BaseModel): name: str
class Subscription(BaseModel): user: str; topic_name: str

@app.get("/topics")
def list_topics(username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT name, status FROM topics")
    res = [{"name": n, "status": s} for n, s in cur.fetchall()]
    conn.close()
    return res

@app.post("/topics/request")
def request_topic(t: Topic, username: str = Depends(get_current_user)):
    # ✅ NEW: Input Validation for topic name
    topic_name = t.name.strip()
    
    # Check if empty
    if not topic_name:
        raise HTTPException(400, "Topic name cannot be empty.")
    
    # Check format with regex
    if not TOPIC_REGEX.match(topic_name):
        raise HTTPException(
            400, 
            "Invalid topic name format. Use only letters, numbers, underscores (_), or hyphens (-). No spaces allowed."
        )
    
    # Check length
    if len(topic_name) < 3 or len(topic_name) > 50:
        raise HTTPException(
            400,
            "Topic name must be between 3 and 50 characters long."
        )
    
    # Check if topic name starts with number or special char (optional but good practice)
    if topic_name[0].isdigit():
        raise HTTPException(
            400,
            "Topic name cannot start with a number."
        )
    
    # ✅ END of Validation
    
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO topics (name, status) VALUES (%s, 'pending') "
            "ON DUPLICATE KEY UPDATE status='pending'", (topic_name,)
        )
        conn.commit()
        log_action(f"Topic request submitted: '{topic_name}'", source=f"User:{username}")
    except Exception as e:
        conn.rollback(); conn.close()
        raise HTTPException(400, f"Topic request failed: {e}")
    conn.close()
    return {"message": f"Topic '{topic_name}' added as pending"}

@app.post("/topics/{name}/approve")
def approve_topic(name: str, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE topics SET status='approved' WHERE name=%s", (name,))
    conn.commit()
    conn.close()
    log_action(f"Topic approved: '{name}'", source=f"Admin:{username}")
    return {"message": f"Topic '{name}' approved"}

@app.post("/topics/{name}/set-active")
def set_active(name: str, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE topics SET status='active' WHERE name=%s", (name,))
    conn.commit()
    conn.close()
    log_action(f"Topic set to ACTIVE: '{name}'", source="Producer")
    return {"message": f"Topic '{name}' is active"}

@app.post("/topics/{name}/reject")
def reject_topic(name: str, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("UPDATE topics SET status='rejected' WHERE name=%s", (name,))
    cur.execute("DELETE FROM user_subscriptions WHERE topic_name = %s", (name,))
    conn.commit()
    conn.close()
    log_action(f"Topic rejected/deleted: '{name}'", source=f"Admin:{username}")
    return {"message": f"Topic '{name}' rejected and will be deleted."}

@app.post("/topics/{name}/confirm-delete")
def confirm_delete(name: str, username: str = Depends(get_current_user)):
    """
    Called by the Producer AFTER it deletes a topic from Kafka.
    This removes the topic from the database permanently.
    """
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM topics WHERE name = %s", (name,))
    conn.commit()
    conn.close()
    log_action(f"Deletion confirmed and removed from DB: '{name}'", source="Producer")
    return {"message": f"Topic '{name}' fully deleted from database."}

@app.get("/topics/active")
def active_topics(username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT name FROM topics WHERE status='active'")
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res

@app.post("/subscribe")
def subscribe(s: Subscription, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO user_subscriptions (user, topic_name) VALUES (%s,%s)", (s.user, s.topic_name))
        conn.commit()
        log_action(f"User '{s.user}' subscribed to '{s.topic_name}'", source="Consumer")
    except:
        conn.rollback(); conn.close()
        raise HTTPException(400, "Already subscribed or invalid topic")
    conn.close()
    return {"message": f"{s.user} subscribed to {s.topic_name}"}

@app.post("/unsubscribe")
def unsubscribe(s: Subscription, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("DELETE FROM user_subscriptions WHERE user = %s AND topic_name = %s", (s.user, s.topic_name))
    conn.commit()
    conn.close()
    log_action(f"User '{s.user}' unsubscribed from '{s.topic_name}'", source="Consumer")
    return {"message": f"{s.user} unsubscribed from {s.topic_name}"}

@app.get("/subscriptions")
def list_subs(username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT user, topic_name FROM user_subscriptions")
    res = [{"user": u, "topic": t} for u, t in cur.fetchall()]
    conn.close()
    return res

@app.get("/subscriptions/{user}")
def list_user_subs(user: str, username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT topic_name FROM user_subscriptions WHERE user = %s", (user,))
    res = [r[0] for r in cur.fetchall()]
    conn.close()
    return res

# --- NEW Endpoints for Frontend Logging ---

@app.get("/logs")
def get_logs(username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT timestamp, source, message FROM admin_logs ORDER BY timestamp DESC LIMIT 20")
    res = [{"time": t, "source": s, "msg": m} for t, s, m in cur.fetchall()]
    conn.close()
    return res

@app.get("/live-data")
def get_live_data(username: str = Depends(get_current_user)):
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT timestamp, topic_name, message_content FROM live_data ORDER BY timestamp DESC LIMIT 20")
    res = [{"time": t, "topic": top, "msg": m} for t, top, m in cur.fetchall()]
    conn.close()
    return res

# --- (Run server is unchanged) ---
if __name__ == "__main__":
    uvicorn.run("admin_api:app", host="0.0.0.0", port=8000)