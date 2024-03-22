import boto3
from moto import mock_sqs
import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

@pytest.fixture
def sqs_client():
    with mock_sqs():
        yield boto3.client('sqs', region_name='us-west-2')

def test_process_queue(sqs_client):
    # Create a mock SQS queue
    queue_url = sqs_client.create_queue(QueueName='test_queue')['QueueUrl']
    
    # Send test messages to the queue
    messages = [{'Id': str(i), 'MessageBody': f'Test Message {i}'} for i in range(5)]
    sqs_client.send_message_batch(QueueUrl=queue_url, Entries=messages)

    # Call the process_queue endpoint
    response = client.get("/process_queue")

    # Check if the endpoint returns a success message
    assert response.status_code == 200
    assert response.json() == {"message": "Queue processed successfully"}

    # Check if the messages are deleted from the queue
    messages = sqs_client.receive_message(QueueUrl=queue_url)
    assert 'Messages' not in messages
