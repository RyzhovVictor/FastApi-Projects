import os
import asyncio
import shutil
from uuid import UUID, uuid4
from celery import Celery, result
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
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
SUPPORTED_FORMATS = ['mp4', 'avi', 'mkv', 'mov', 'webm', 'flv', 'wmv', 'ogv', '3gp', '3g2', 'hevc', 'av1', 'vro']

class DirectoryRequest(BaseModel):
    directory: str

def convert_video(input_file, output_dir, output_format='mp4'):
    output_file = os.path.splitext(os.path.basename(input_file))[0] + f'_converted.{output_format}'
    output_path = os.path.join(output_dir, output_file)
    command = ['ffmpeg', '-i', input_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', output_path]
    result = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = result.communicate()
    if result.returncode != 0:
        raise Exception('Ошибка конвертации')
    else:
        return output_file

@celery.task
def convert_video_task(file_path, output_dir, output_format='mp4'):
    return convert_video(file_path, output_dir, output_format)

@celery.task
def delete_files_task(directory_path):
    shutil.rmtree(directory_path)

@app.post("/convert-video/")
async def start_conversion(file: UploadFile = File(...), output_format: str = 'mp4') -> str:
    if output_format not in SUPPORTED_FORMATS:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый формат. Доступные форматы: {', '.join(SUPPORTED_FORMATS)}")
    os.makedirs(TMPDIR, exist_ok=True)
    uuid_dir = os.path.join(TMPDIR, str(uuid4()))
    os.makedirs(uuid_dir)

    tmp_file: str = os.path.join(uuid_dir, file.filename)
    
    async with aiofiles.open(tmp_file, 'wb') as out_file:
        while content := await file.read(1024):
            await out_file.write(content)

    task = convert_video_task.delay(tmp_file, uuid_dir, output_format)

    return str(task.id)

@app.get('/task/status/{task_id}')
async def get_task_status(task_id: str):
    task = result.AsyncResult(task_id)
    return task.status

@app.get('/task/result/{task_id}')
async def get_task_result(task_id: str):
    task = result.AsyncResult(task_id)
    return task.get()

@app.get("/download-video/") 
async def download_video(file_name: str) -> FileResponse:
    for root, dirs, files in os.walk(TMPDIR):
        if file_name in files:
            uuid_dir = root
            break
    else:
        raise HTTPException(status_code=404, detail="Файл не найден")

    file_path: str = os.path.join(uuid_dir, file_name) 
    response = FileResponse(path=file_path, filename=file_name)
    delete_files_task.apply_async(countdown=60, args=[uuid_dir]) 
    return response

