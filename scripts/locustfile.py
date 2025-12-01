import random
from locust import HttpUser, task, between

class TinyInstaUser(HttpUser):
    wait_time = between(0.5, 1.0)

    @task
    def get_timeline(self):
        user_id = random.randint(1, 1000)
        username = f"user{user_id}"
        self.client.get(f"/api/timeline?user={username}", name="/api/timeline?user=[id]")
