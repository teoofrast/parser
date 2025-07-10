import requests
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)

task_id = None

def test_create_task_success():
    response = client.post("/tasks/", data={"url": "https://12.kz"})

    assert response.status_code == 201
    global task_id
    task_id = response.json().get('task_id')
    assert task_id


def test_create_task_no_url():
    response = client.post("/tasks/", data={"url": ""})
    assert response.status_code == 400
    assert response.json() == {"detail": "The url not provided"}


def test_task_status_with_empty_id():
    response = requests.get(f"http://localhost:8000/tasks/{task_id}",)
    assert response.status_code == 200
    assert response.json() == {'status': 'PENDING', 'data': 'None'}
