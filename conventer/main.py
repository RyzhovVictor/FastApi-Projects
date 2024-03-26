import os
import asyncio
from celery import Celery
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import FileResponse  # Импорт класса FileResponse

import aiofiles
import subprocess

app = FastAPI()

# Настройка Celery
celery = Celery(
    'tasks',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/1'
)

ROOTDIR = os.path.dirname(os.path.abspath(__file__))
TMPDIR = os.path.join(ROOTDIR, 'tmp')  # Путь к временной директории для сохранения загруженных файлов

# Определение модели запроса, содержащей путь к директории
class DirectoryRequest(BaseModel):
    directory: str

# Функция для конвертации файла из формата VRO в MP4 с использованием ffmpeg
def convert_vro_to_mp4(input_file, output_file):
    command = ['ffmpeg', '-i', input_file, '-c:v', 'copy', '-c:a', 'aac', '-strict', 'experimental', output_file]
    subprocess.run(command)

# Функция для рекурсивного обхода директории и конвертации файлов в MP4
def convert_files_in_dir(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.VRO'):
                vro_file = os.path.join(root, file)
                mp4_file = os.path.join(root, f"{file[:-4]}.mp4")  # Создание нового имени для MP4 файла

                try:
                    convert_vro_to_mp4(vro_file, mp4_file)  # Конвертация файла
                    os.remove(vro_file)  # Удаление исходного файла после конвертации
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Ошибка конвертации файла {vro_file}: {str(e)}")

# Запуск Celery-задачи для конвертации файлов в директории
@celery.task
def convert_video_task(directory):
    convert_files_in_dir(directory)

# Обработчик для загрузки видеофайла через API
@app.post("/convert-video/")
async def start_conversion(file: UploadFile = File(...)):
    os.makedirs(TMPDIR, exist_ok=True)  # Создание временной директории, если она не существует
    tmp_file = os.path.join(TMPDIR, file.filename)  # Путь к временному файлу

    # Запись содержимого загруженного файла во временный файл
    async with aiofiles.open(tmp_file, 'wb') as out_file:
        while content := await file.read(1024):
            await out_file.write(content)

    # Запуск Celery-задачи для конвертации файла
    convert_video_task.delay(TMPDIR)

    return {"message": "Процесс конвертации запущен"}

# Обработчик для скачивания конвертированного видеофайла
@app.get("/download-video/")  # Использование метода GET для скачивания файла
async def download_video(file_name: str):
    file_path = os.path.join(TMPDIR, file_name)  # Путь к конвертированному файлу
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_name)  # Возвращение файла для скачивания
    else:
        raise HTTPException(status_code=404, detail="Файл не найден")  # Ошибка, если файл не найден
