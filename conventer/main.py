import os
import asyncio
from celery import Celery
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, UploadFile
import aiofiles
import subprocess

app = FastAPI()

celery = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

ROOTDIR = os.path.dirname(os.path.abspath(__file__))

@celery.task
def convert_video_task(path_file):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(path_file))

class DirectoryRequest(BaseModel):
    directory: str

def convert_vro_to_mp4(input_file, output_file):
    command = ['ffmpeg', '-i', input_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', output_file]
    result = subprocess.Popen(command, stdout=subprocess.PIPE)
    print(result)

async def convert_files_in_dir(directory):
    counter = 1
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.VRO'):
                vro_file = os.path.join(root, file)
                mp4_file = os.path.join(root, f"Rony_Love_You_{counter}.mp4")

                while os.path.exists(mp4_file):
                    counter += 1
                    mp4_file = os.path.join(root, f"Rony_Love_You_{counter}.mp4")

                try:
                    await convert_vro_to_mp4(vro_file, mp4_file)
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Ошибка конвертации файла {vro_file}: {str(e)}")

def to_convert_in_files(file_path):
    convert_file_path = file_path.split('.')
    convert_file_path[:-1] = '.mp4'
    convert_vro_to_mp4(file_path, os.path.join(*convert_file_path))


@app.post("/convert-video/")
async def start_conversion(file: UploadFile):
    tmp_file = os.path.join(ROOTDIR, 'tmp', file.filename)
    async with aiofiles.open(tmp_file, 'wb') as out_file:
        while content := await file.read(1024):  # async read chunk
            await out_file.write(content)  # async write chunk
    # convert_video_task.delay(directory)
    return {"message": "Процесс конвертации запущен"}
