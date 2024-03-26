import os
import asyncio
from celery import Celery
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse

import aiofiles
import subprocess

app: FastAPI = FastAPI()

celery: Celery = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

ROOTDIR: str = os.path.dirname(os.path.abspath(__file__))
TMPDIR: str = os.path.join(ROOTDIR, 'tmp')

class DirectoryRequest(BaseModel):
    directory: str

def convert_vro_to_mp4(input_file, output_file):
    command = ['ffmpeg', '-i', input_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', output_file]
    result = subprocess.Popen(command, stdout=subprocess.PIPE)
    output, error = result.communicate()
    if result.returncode != 0:
        print(f"Ошибка при выполнении команды: {error}")
    else:
        print(f"Результат выполнения команды: {output}")


def convert_files_in_dir(directory: str) -> None:
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.VRO'):
                vro_file: str = os.path.join(root, file)
                mp4_file: str = os.path.join(root, f"{file[:-4]}.mp4")

                try:
                    convert_vro_to_mp4(vro_file, mp4_file)
                    os.remove(vro_file)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Ошибка конвертации файла {vro_file}: {str(e)}")

@celery.task
def convert_video_task(directory: str) -> None:
    convert_files_in_dir(directory)

@app.post("/convert-video/")
async def start_conversion(file: UploadFile = File(...)) -> dict:
    os.makedirs(TMPDIR, exist_ok=True)
    tmp_file: str = os.path.join(TMPDIR, file.filename)

    async with aiofiles.open(tmp_file, 'wb') as out_file:
        while content := await file.read(1024):
            await out_file.write(content)

    convert_video_task.delay(TMPDIR)

    return {"message": "Процесс конвертации запущен"}

@app.get("/download-video/") 
async def download_video(file_name: str) -> FileResponse:
    file_path: str = os.path.join(TMPDIR, file_name) 
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_name) 
    else:
        raise HTTPException(status_code=404, detail="Файл не найден")
