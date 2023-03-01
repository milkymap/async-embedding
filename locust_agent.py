from uuid import uuid4
from locust import HttpUser, task 

class Agent(HttpUser):

    @task 
    def compute_embedding(self):
        self.client.post(
            url='/compute_embedding',
            json={
                'text': '\n'.join([ str(uuid4()) for _ in range(10) ])
            }
        )