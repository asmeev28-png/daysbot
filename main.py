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

async def main():
    """Основная функция запуска бота"""
    logger.info("Запуск бота для дней рождения...")
    
    bot = BirthdayBot()
    
    with GracefulExit() as exit_handler:
        try:
            await bot.start()
        except KeyboardInterrupt:
            logger.info("Получен KeyboardInterrupt, останавливаю бота...")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            sys.exit(1)
        finally:
            await bot.stop()
    
    logger.info("Бот завершил работу")

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
