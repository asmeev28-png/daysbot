import os
from typing import List

class Config:
    # Токен бота от @BotFather (обязательно)
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    
    # Владелец бота и резервный администратор
    BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", "817046014"))
    BACKUP_ADMIN_ID = int(os.getenv("BACKUP_ADMIN_ID", "1050151004"))
    
    # Главный чат (необязательно, можно добавить через команду)
    MAIN_CHAT_ID = os.getenv("MAIN_CHAT_ID")
    
    # Часовой пояс (фиксированный UTC+3)
    TIMEZONE = "UTC+3"
    
    # Время отправки (формат HH:MM)
    BIRTHDAY_TIME = "09:00"
    EVENT_TIME = "10:00"
    
    # Лимиты для бесплатного плана Railway
    MAX_CONGratulations = 50           # Максимум поздравлений в базе
    MAX_EVENTS_PER_CHAT = 20           # Максимум событий на чат
    MAX_BIRTHDAYS_PER_CHAT = 200       # Максимум дней рождений на чат
    
    # Настройки производительности
    MESSAGE_QUEUE_SIZE = 10            # Размер очереди сообщений
    DB_CACHE_SIZE = 100                # Размер кэша для БД
    
    # Путь к файлу базы данных (будет на volume в Railway)
    DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/birthday_bot.db")
    
    # Дополнительные настройки
    ENABLE_MONTHLY_REMINDER = True     # Включить ежемесячные напоминания
    ENABLE_EVENTS = True               # Включить события
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    
    @staticmethod
    def is_owner(user_id: int) -> bool:
        """Проверяет, является ли пользователь владельцем или резервным админом"""
        return user_id in [Config.BOT_OWNER_ID, Config.BACKUP_ADMIN_ID]
    
    @staticmethod
    def get_owners() -> List[int]:
        """Возвращает список всех владельцев"""
        return [Config.BOT_OWNER_ID, Config.BACKUP_ADMIN_ID]
