import asyncio
import multiprocessing
import os
import uuid
import sqlite3
from fastapi import FastAPI, BackgroundTasks, HTTPException, status

# Initialize FastAPI app
app = FastAPI()

# SQLite3 database connection
conn = sqlite3.connect('jobs.db')
c = conn.cursor()

# Create jobs table if not exists
c.execute('''CREATE TABLE IF NOT EXISTS jobs
             (job_id TEXT PRIMARY KEY, status TEXT)''')
conn.commit()

# AWS SQS client
sqs = boto3.client('sqs', region_name='your_region')

# Sample function to process SQS message
def process_sqs_message(message):
    # Process message here
    print("Processing SQS message:", message)
    # Simulating processing time
    asyncio.sleep(5)
    # Delete message from SQS
    sqs.delete_message(
        QueueUrl='your_queue_url',
        ReceiptHandle=message['ReceiptHandle']
    )
    print("Message deleted from SQS:", message)

# Background task to pull messages from SQS
def pull_sqs():
    # Calculate the number of processes to use (half of available CPU cores)
    num_cores = os.cpu_count() or 1
    num_processes = max(1, num_cores // 2)  # Ensure at least 1 process is used

    # Poll SQS for messages
    while True:
        response = sqs.receive_message(
            QueueUrl='your_queue_url',
            MaxNumberOfMessages=1,
            VisibilityTimeout=10,
            WaitTimeSeconds=5
        )
        messages = response.get('Messages', [])
        if messages:
            pool = multiprocessing.Pool(processes=num_processes)
            pool.map(process_sqs_message, messages)
        else:
            asyncio.sleep(5)

# Sample function to simulate long-running task
async def execute_single_item_task(item: str):
    # Simulating long-running task
    await asyncio.sleep(10)
    return {"status": "Completed", "item": item}

# Endpoint to execute single item
@app.post('/execute_single_item/', status_code=status.HTTP_202_ACCEPTED)
async def execute_single_item(item: str, background_tasks: BackgroundTasks):
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    # Enqueue the task in a background process
    background_tasks.add_task(execute_single_item_task, item)
    
    # Insert job ID and status into SQLite3 database
    c.execute("INSERT INTO jobs (job_id, status) VALUES (?, ?)", (job_id, "In-Progress"))
    conn.commit()
    
    return {"job_id": job_id}

# Endpoint to get item status
@app.get('/get_item_status/{job_id}')
async def get_item_status(job_id: str):
    # Retrieve job status from SQLite3 database
    c.execute("SELECT status FROM jobs WHERE job_id=?", (job_id,))
    result = c.fetchone()
    
    if result:
        return {"status": result[0]}
    else:
        return {"status": "Job not found"}

# Endpoint to get SQS status
@app.get('/get_sqs_status/')
async def get_sqs_status():
    # Get SQS attributes to retrieve the number of pending messages
    response = sqs.get_queue_attributes(
        QueueUrl='your_queue_url',
        AttributeNames=['ApproximateNumberOfMessages']
    )
    pending_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
    return {"pending_messages": pending_messages}

# Start background task to pull messages from SQS
@app.on_event("startup")
async def startup_event():
    # Start the background task to pull messages from SQS
    asyncio.create_task(pull_sqs())

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
