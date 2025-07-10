from celery.result import AsyncResult
from fastapi import FastAPI, status, HTTPException, Form
import uvicorn
from fastapi.responses import JSONResponse
from starlette.status import HTTP_200_OK

from celery_worker import parse_url, celery_app

app = FastAPI()


@app.post("/tasks/")
async def create_task(
    url: str = Form(...),
):
    if not url:
        raise HTTPException(
            detail='The url not provided',
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    task = parse_url.delay(url)

    return JSONResponse(
        content={
            'task_id': task.id
        },
        status_code=status.HTTP_201_CREATED,
    )


@app.get('/tasks/{task_id}')
async def get_task_status(
    task_id: str,
):
    if not task_id:
        return HTTPException(
            detail='The task_id not provided',
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    task = AsyncResult(task_id)

    return JSONResponse(
        content={
            'status': task.status,
            'data': str(task.result),
        },
        status_code=HTTP_200_OK,
    )


if __name__ == '__main__':
    uvicorn.run(
        "main:app",
        host='0.0.0.0',
        port=8000,
    )
