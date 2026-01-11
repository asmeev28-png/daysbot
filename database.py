import aiosqlite
import json
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any, Tuple
from config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.conn: Optional[aiosqlite.Connection] = None
    
    async def connect(self):
        """Подключение к базе данных"""
        try:
            # Создаем директорию для базы данных если её нет
            import os
            os.makedirs(os.path.dirname(Config.DATABASE_PATH), exist_ok=True)
            
            self.conn = await aiosqlite.connect(Config.DATABASE_PATH)
            self.conn.row_factory = aiosqlite.Row
            await self.init_database()
            logger.info(f"База данных подключена: {Config.DATABASE_PATH}")
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise
    
    async def init_database(self):
        """Инициализация таблиц"""
        await self.conn.executescript('''
            -- Таблица настроек бота
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Таблица разрешенных чатов
            CREATE TABLE IF NOT EXISTS allowed_chats (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                added_by INTEGER NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            );
            
            -- Таблица дней рождений
            CREATE TABLE IF NOT EXISTS birthdays (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
                month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
                year INTEGER,
                username TEXT,
                full_name TEXT,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, chat_id)
            );
            
            -- Индекс для поиска по дате
            CREATE INDEX IF NOT EXISTS idx_birthdays_date ON birthdays(month, day);
            CREATE INDEX IF NOT EXISTS idx_birthdays_chat ON birthdays(chat_id);
            
            -- Таблица поздравлений
            CREATE TABLE IF NOT EXISTS congratulations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                text TEXT NOT NULL,
                used_count INTEGER DEFAULT 0,
                added_by INTEGER,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Таблица событий
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
                month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
                year INTEGER,
                message TEXT NOT NULL,
                media_type TEXT,
                media_id TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_by INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chat_id) REFERENCES allowed_chats(chat_id) ON DELETE CASCADE
            );
           
            -- Индекс для событий
            CREATE INDEX IF NOT EXISTS idx_events_date ON events(month, day);
            CREATE INDEX IF NOT EXISTS idx_events_chat ON events(chat_id);
            
            -- Таблица отправленных поздравлений (для избежания дублирования)
            CREATE TABLE IF NOT EXISTS sent_congratulations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                congratulation_id INTEGER,
                sent_date DATE NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(chat_id, user_id, sent_date)
            );
            
            -- Таблица отправленных событий
            CREATE TABLE IF NOT EXISTS sent_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                sent_date DATE NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(event_id, sent_date)
            );
        ''')
        
        await self.conn.commit()
        
        # Создаем начальные настройки
        await self.conn.execute('''
            INSERT OR IGNORE INTO bot_settings (key, value) VALUES 
            ('owner_id', ?),
            ('backup_admin_id', ?)
        ''', (str(Config.BOT_OWNER_ID), str(Config.BACKUP_ADMIN_ID)))
        
        await self.conn.commit()
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ЧАТАМИ ==========
    
    async def add_chat_to_whitelist(self, chat_id: int, title: str, added_by: int) -> bool:
        """Добавить чат в белый список"""
        try:
            await self.conn.execute('''
                INSERT OR REPLACE INTO allowed_chats (chat_id, title, added_by, is_active)
                VALUES (?, ?, ?, 1)
            ''', (chat_id, title, added_by))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления чата в белый список: {e}")
            return False
    
    async def remove_chat_from_whitelist(self, chat_id: int) -> bool:
        """Удалить чат из белого списка"""
        try:
            await self.conn.execute('DELETE FROM allowed_chats WHERE chat_id = ?', (chat_id,))
            # Также удаляем все связанные данные
            await self.conn.execute('DELETE FROM birthdays WHERE chat_id = ?', (chat_id,))
            await self.conn.execute('DELETE FROM events WHERE chat_id = ?', (chat_id,))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления чата из белого списка: {e}")
            return False
    
    async def is_chat_allowed(self, chat_id: int) -> bool:
        """Проверяет, разрешен ли чат"""
        try:
            cursor = await self.conn.execute(
                'SELECT 1 FROM allowed_chats WHERE chat_id = ? AND is_active = 1',
                (chat_id,)
            )
            result = await cursor.fetchone()
            return result is not None
        except Exception as e:
            logger.error(f"Ошибка проверки разрешенного чата: {e}")
            return False
    
    async def get_all_allowed_chats(self) -> List[Dict]:
        """Получить все разрешенные чаты"""
        try:
            cursor = await self.conn.execute('''
                SELECT chat_id, title, added_by, added_at, is_active
                FROM allowed_chats
                WHERE is_active = 1
                ORDER BY added_at
            ''')
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения списка чатов: {e}")
            return []
    
    # ========== МЕТОДЫ ДЛЯ ДНЕЙ РОЖДЕНИЙ ==========
    
    async def add_birthday(self, user_id: int, chat_id: int, day: int, month: int, 
                          year: Optional[int], username: str, full_name: str, created_by: int) -> bool:
        """Добавить или обновить день рождения"""
        try:
            await self.conn.execute('''
                INSERT OR REPLACE INTO birthdays 
                (user_id, chat_id, day, month, year, username, full_name, created_by, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (user_id, chat_id, day, month, year, username, full_name, created_by))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления дня рождения: {e}")
            return False
    
    async def delete_birthday(self, user_id: int, chat_id: int) -> bool:
        """Удалить день рождения"""
        try:
            await self.conn.execute(
                'DELETE FROM birthdays WHERE user_id = ? AND chat_id = ?',
                (user_id, chat_id)
            )
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления дня рождения: {e}")
            return False
    
    async def get_birthday(self, user_id: int, chat_id: int) -> Optional[Dict]:
        """Получить день рождения пользователя"""
        try:
            cursor = await self.conn.execute(
                'SELECT * FROM birthdays WHERE user_id = ? AND chat_id = ?',
                (user_id, chat_id)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Ошибка получения дня рождения: {e}")
            return None
    
    async def get_birthdays_by_chat(self, chat_id: int) -> List[Dict]:
        """Получить все дни рождения в чате"""
        try:
            cursor = await self.conn.execute('''
                SELECT * FROM birthdays 
                WHERE chat_id = ?
                ORDER BY month, day
            ''', (chat_id,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения дней рождений чата: {e}")
            return []
    
    async def get_todays_birthdays(self, today: date) -> List[Dict]:
        """Получить дни рождения на сегодня с учетом 29 февраля"""
        day = today.day
        month = today.month
        year = today.year
        
        # Проверяем високосный год
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        
        try:
            if month == 2 and day == 28 and not is_leap:
                # Проверяем 28 февраля и 29 февраля (в невисокосный год)
                cursor = await self.conn.execute('''
                    SELECT * FROM birthdays 
                    WHERE (month = ? AND day = ?) OR (month = 2 AND day = 29)
                ''', (month, day))
            else:
                cursor = await self.conn.execute('''
                    SELECT * FROM birthdays 
                    WHERE month = ? AND day = ?
                ''', (month, day))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения сегодняшних дней рождений: {e}")
            return []
    
    async def get_upcoming_birthdays(self, chat_id: int, limit: int = 3) -> List[Dict]:
        """Получить ближайшие дни рождения"""
        try:
            # Сложный запрос для получения ближайших дней рождений
            cursor = await self.conn.execute('''
                WITH today AS (SELECT DATE('now') as today_date)
                SELECT b.*,
                       CASE 
                           WHEN (b.month > strftime('%m', today.today_date)) OR 
                                (b.month = strftime('%m', today.today_date) AND b.day >= strftime('%d', today.today_date))
                           THEN julianday(date(strftime('%Y', today.today_date) || '-' || printf('%02d', b.month) || '-' || printf('%02d', b.day))) - julianday(today.today_date)
                           ELSE julianday(date((strftime('%Y', today.today_date) + 1) || '-' || printf('%02d', b.month) || '-' || printf('%02d', b.day))) - julianday(today.today_date)
                       END as days_until
                FROM birthdays b, today
                WHERE b.chat_id = ?
                ORDER BY days_until
                LIMIT ?
            ''', (chat_id, limit))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения ближайших дней рождений: {e}")
            return []
    
    # ========== МЕТОДЫ ДЛЯ ПОЗДРАВЛЕНИЙ ==========
    
    async def add_congratulations(self, texts: List[str], added_by: int) -> int:
        """Добавить поздравления (заменяет старые)"""
        try:
            # Удаляем старые поздравления
            await self.conn.execute('DELETE FROM congratulations')
            
            # Добавляем новые (максимум Config.MAX_CONGratulations)
            count = 0
            for text in texts[:Config.MAX_CONGratulations]:
                if text.strip():  # Игнорируем пустые строки
                    await self.conn.execute(
                        'INSERT INTO congratulations (text, added_by) VALUES (?, ?)',
                        (text.strip(), added_by)
                    )
                    count += 1
            
            await self.conn.commit()
            return count
        except Exception as e:
            logger.error(f"Ошибка добавления поздравлений: {e}")
            return 0
    
    async def get_random_congratulation(self) -> Optional[Dict]:
        """Получить случайное поздравление"""
        try:
            cursor = await self.conn.execute('''
                SELECT * FROM congratulations 
                ORDER BY RANDOM() 
                LIMIT 1
            ''')
            row = await cursor.fetchone()
            if row:
                # Увеличиваем счетчик использования
                await self.conn.execute(
                    'UPDATE congratulations SET used_count = used_count + 1 WHERE id = ?',
                    (row['id'],)
                )
                await self.conn.commit()
                return dict(row)
            return None
        except Exception as e:
            logger.error(f"Ошибка получения случайного поздравления: {e}")
            return None
    
    async def mark_birthday_sent(self, user_id: int, chat_id: int, congratulation_id: int) -> bool:
        """Отметить отправленное поздравление"""
        try:
            await self.conn.execute('''
                INSERT INTO sent_congratulations (chat_id, user_id, congratulation_id, sent_date)
                VALUES (?, ?, ?, DATE('now'))
            ''', (chat_id, user_id, congratulation_id))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка отметки отправленного поздравления: {e}")
            return False
    
    # ========== МЕТОДЫ ДЛЯ СОБЫТИЙ ==========
    
    async def add_event(self, chat_id, name, day, month, year=None, message=None, 
                        media_type=None, media_id=None, created_by=None):
        """Добавление события - ВСЕ события повторяются ежегодно"""
        try:
            cursor = await self.conn.execute('''
                INSERT INTO events (chat_id, name, day, month, year, message, 
                                   media_type, media_id, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (chat_id, name, day, month, year, message, media_type, media_id, created_by))
        
            await self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Ошибка при добавлении события: {e}")
            raise
    
    async def get_todays_events(self, today: date) -> List[Dict]:
        """Получить события на сегодня - ВСЕ события ежегодные"""
        day = today.day
        month = today.month
        
        try:
            cursor = await self.conn.execute('''
                SELECT e.* FROM events e
                LEFT JOIN sent_events se ON e.id = se.event_id AND se.sent_date = DATE('now')
                WHERE e.is_active = 1 
                AND e.month = ? AND e.day = ?
                AND se.id IS NULL
            ''', (month, day))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения сегодняшних событий: {e}")
            return []
    
    async def get_events_by_date(self, day: int, month: int) -> List[Dict]:
        """Получить события на конкретную дату (для планировщика)"""
        try:
            cursor = await self.conn.execute('''
                SELECT * FROM events 
                WHERE is_active = 1 
                AND month = ? AND day = ?
                ORDER BY chat_id
            ''', (month, day))
            
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения событий на дату {day}.{month}: {e}")
            return []
            
    async def mark_event_sent(self, event_id: int) -> bool:
        """Отметить отправленное событие"""
        try:
            await self.conn.execute('''
                INSERT INTO sent_events (event_id, sent_date) VALUES (?, DATE('now'))
            ''', (event_id,))
            await self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка отметки отправленного события: {e}")
            return False
    
    async def close(self):
        """Закрыть соединение с базой данных"""
        if self.conn:
            await self.conn.close()

# Глобальный экземпляр базы данных
db = Database()
