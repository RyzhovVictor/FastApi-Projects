import os
import asyncio
from celery import Celery, result
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from uuid import UUID, uuid4
import pathlib  

import aiofiles
import subprocess

app: FastAPI = FastAPI()

celery: Celery = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
    broker_connection_retry_on_startup=True,
    result_backend='redis://localhost:6379/0'
)


ROOTDIR: str = os.path.dirname(os.path.abspath(__file__))
TMPDIR: str = os.path.join(ROOTDIR, 'tmp')

class DirectoryRequest(BaseModel):
    directory: str

def convert_vro_to_mp4(input_file):
    output_file = pathlib.Path(input_file).with_suffix('.mp4')
    command = ['ffmpeg', '-i', input_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', str(output_file)]
    result = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = result.communicate()
    if result.returncode != 0:
        raise Exception('Ошибка конвертации')
    else:
        return output_file.name


@celery.task
def convert_video_task(file_path):
    return convert_vro_to_mp4(file_path)

@celery.task
def delete_files_task(file_name):
    os.remove(os.path.join(TMPDIR, file_name))
    


@app.post("/convert-video/")
async def start_conversion(file: UploadFile = File(...)) -> str:
    os.makedirs(TMPDIR, exist_ok=True)
    name = str(uuid4())
    tmp_file: str = os.path.join(TMPDIR, name + pathlib.Path(file.filename).suffix)
    
    async with aiofiles.open(tmp_file, 'wb') as out_file:
        while content := await file.read(1024):
            await out_file.write(content)

    task = convert_video_task.delay(tmp_file)

    return str(task.id)



@app.get('/task/status/{task_id}')
async def get_task_status(task_id: UUID):
    task = result.AsyncResult(str(task_id))
    return task.status

@app.get('/task/result/{task_id}')
async def get_task_result(task_id :UUID):
    task = result.AsyncResult(str(task_id))
    return task.get()

@app.get("/download-video/") 
async def download_video(file_name: str) -> FileResponse:
    file_path: str = os.path.join(TMPDIR, file_name) 
    if os.path.exists(file_path):
        response = FileResponse(path=file_path, filename=file_name)  
        delete_files_task.apply_async(countdown=60, args=[file_name])
        return response
    else:
        raise HTTPException(status_code=404, detail="Файл не найден")
    

    




