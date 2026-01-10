#!/usr/bin/env python3
import asyncio
import logging
import sys

from database import db  # Импортируем объект базы данных
from config import Config
from bot import BirthdayBot  # Импортируем основной класс бота

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=getattr(logging, Config.LOG_LEVEL.upper())
)

logger = logging.getLogger(__name__)

async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск бота для дней рождения...")
    
    try:
        # 1. Подключаем базу данных
        await db.connect()
        logger.info("База данных успешно подключена")
        
        # 2. Создаем экземпляр бота и запускаем его
        bot = BirthdayBot()
        await bot.start()
        
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске бота: {e}")
        # Убедимся, что база данных корректно закрывается при ошибке
        await db.close()
        sys.exit(1)

if __name__ == '__main__':
    # Проверка обязательных переменных окружения
    if not Config.BOT_TOKEN:
        logger.error("Не указан BOT_TOKEN в переменных окружения!")
        sys.exit(1)
    
    if not Config.BOT_OWNER_ID:
        logger.error("Не указан BOT_OWNER_ID в переменных окружения!")
        sys.exit(1)
    
    # Запуск бота
    asyncio.run(main())
