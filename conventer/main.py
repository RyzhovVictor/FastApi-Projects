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
os.makedirs(TMPDIR, exist_ok=True)
SUPPORTED_FORMATS = ['mp4', 'avi', 'mkv', 'mov', 'webm', 'flv', 'wmv', 'ogv', '3gp', '3g2', 'hevc', 'av1', 'vro']

class DirectoryRequest(BaseModel):
    directory: str

def convert_video(tmp_file, new_file):
    output_path = os.path.join(TMPDIR, new_file)
    input_path = os.path.join(TMPDIR, tmp_file)
    command = ['ffmpeg', '-i', input_path, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', output_path]
    result = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = result.communicate()
    if result.returncode != 0:
        raise Exception('Ошибка конвертации')
    else:
        return new_file

@celery.task
def convert_video_task(tmp_file, new_file):
    return convert_video(tmp_file, new_file)

@celery.task
def delete_files_task(directory_path):
    directory_path = os.path.join(TMPDIR, directory_path)
    shutil.rmtree(directory_path)

@app.post("/convert-video/")
async def start_conversion(file: UploadFile = File(...), output_format: str = 'mp4') -> str:
    if output_format not in SUPPORTED_FORMATS:
        raise HTTPException(status_code=400, detail=f"Неподдерживаемый формат. Доступные форматы: {', '.join(SUPPORTED_FORMATS)}")

    uuid_dir = str(uuid4())
    os.makedirs(os.path.join(TMPDIR, uuid_dir))

    tmp_file: str = os.path.join(uuid_dir, file.filename)
    
    async with aiofiles.open(os.path.join(TMPDIR, tmp_file), 'wb') as out_file:
        while content := await file.read(1024):
            await out_file.write(content)
    new_file = os.path.splitext(os.path.basename(file.filename))[0] + f'_converted.{output_format}'
    new_file = os.path.join(uuid_dir, new_file)
    print(tmp_file, new_file)
    task = convert_video_task.delay(tmp_file, new_file)

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
async def download_video(task_id: str) -> FileResponse:
    task = result.AsyncResult(task_id)
    if task.state == "SUCCESS":
        output_file = task.result
        file_path = os.path.join(TMPDIR, output_file)
        
        if os.path.exists(file_path):
            response = FileResponse(path=file_path, filename=output_file.split("/")[1])
            delete_files_task.apply_async(countdown=60, args=[output_file.split("/")[0]]) 
            return response
        else:
            raise HTTPException(status_code=404, detail="Файл не найден")
    else:
        raise HTTPException(status_code=400, detail="Файл не готов для скачивания")



