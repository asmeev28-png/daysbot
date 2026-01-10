#!/usr/bin/env python3
import asyncio
import logging
import signal
import sys

from bot import BirthdayBot
from config import Config

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=getattr(logging, Config.LOG_LEVEL.upper())
)

logger = logging.getLogger(__name__)

class GracefulExit:
    def __init__(self):
        self.shutdown = False
    
    def __enter__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        return self
    
    def exit_gracefully(self, signum, frame):
        logger.info(f"Получен сигнал {signum}, начинаю graceful shutdown...")
        self.shutdown = True
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)

# ЗАМЕНИТЕ старый код запуска в main.py на этот:
async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск бота для дней рождения...")

    # Инициализация базы данных
    await db.connect()

    # Создание Application - ЭТО ОСНОВНОЙ ОБЪЕКТ в PTB v20+
    application = Application.builder().token(Config.BOT_TOKEN).build()

    # Регистрация обработчиков (вам нужно будет передать application в функцию)
    register_handlers(application)  # Создайте эту функцию, куда перенесете _register_handlers из класса

    # Запуск бота в режиме polling
    logger.info("Бот запущен и начал опрос (polling)...")
    async with application:
        await application.start()
        await application.updater.start_polling()  # Старт polling через application
        # Бесконечное ожидание
        await asyncio.Event().wait()

if __name__ == '__main__':
    # Упрощенный запуск без сложного GracefulExit для теста
    asyncio.run(main())
