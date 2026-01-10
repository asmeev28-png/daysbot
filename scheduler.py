import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import Optional, Dict, Any

from config import Config
from database import db  # Импортируем глобальный объект базы данных
from utils import get_msk_time

logger = logging.getLogger(__name__)

class Scheduler:
    def __init__(self, bot):
        self.bot = bot  # Объект telegram.Bot из application.bot
        self.is_running = False
        self.tasks = []
    
    async def start(self):
        """Запуск планировщика"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # Запускаем задачи
        self.tasks = [
            asyncio.create_task(self._birthday_scheduler()),
            asyncio.create_task(self._event_scheduler()),
            asyncio.create_task(self._monthly_reminder_scheduler()),
            asyncio.create_task(self._cleanup_scheduler())
        ]
        
        logger.info("Планировщик запущен")
    
    async def stop(self):
        """Остановка планировщика"""
        self.is_running = False
        
        for task in self.tasks:
            task.cancel()
        
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        
        logger.info("Планировщик остановлен")
    
    async def _birthday_scheduler(self):
        """Планировщик для дней рождения (09:00 MSK)"""
        while self.is_running:
            try:
                now = get_msk_time()
                
                # Вычисляем время следующего запуска
                target_time = now.replace(
                    hour=9, minute=0, second=0, microsecond=0
                )
                
                if now >= target_time:
                    target_time += timedelta(days=1)
                
                wait_seconds = (target_time - now).total_seconds()
                
                logger.debug(f"Следующая проверка дней рождения через {wait_seconds:.0f} секунд")
                await asyncio.sleep(wait_seconds)
                
                # Запускаем отправку поздравлений
                await self._send_birthday_congratulations()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в планировщике дней рождения: {e}")
                await asyncio.sleep(60)  # Ждем минуту при ошибке
    
    async def _event_scheduler(self):
        """Планировщик для событий (10:00 MSK)"""
        while self.is_running:
            try:
                now = get_msk_time()
                
                # Вычисляем время следующего запуска
                target_time = now.replace(
                    hour=10, minute=0, second=0, microsecond=0
                )
                
                if now >= target_time:
                    target_time += timedelta(days=1)
                
                wait_seconds = (target_time - now).total_seconds()
                
                logger.debug(f"Следующая проверка событий через {wait_seconds:.0f} секунд")
                await asyncio.sleep(wait_seconds)
                
                # Запускаем отправку событий
                await self._send_events()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в планировщике событий: {e}")
                await asyncio.sleep(60)
    
    async def _monthly_reminder_scheduler(self):
        """Планировщик для ежемесячных напоминаний (00:00 MSK 1-го числа)"""
        while self.is_running:
            try:
                now = get_msk_time()
                
                # Проверяем, сегодня ли 1-е число
                if now.day == 1 and now.hour == 0:
                    await self._send_monthly_reminders()
                
                # Ждем до следующего дня
                tomorrow = now + timedelta(days=1)
                tomorrow = tomorrow.replace(hour=0, minute=5, second=0, microsecond=0)
                
                wait_seconds = (tomorrow - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в планировщике месячных напоминаний: {e}")
                await asyncio.sleep(3600)  # Ждем час при ошибке
    
    async def _cleanup_scheduler(self):
        """Планировщик для очистки устаревших данных (03:00 MSK ежедневно)"""
        while self.is_running:
            try:
                now = get_msk_time()
                
                # Вычисляем время следующей очистки
                target_time = now.replace(
                    hour=3, minute=0, second=0, microsecond=0
                )
                
                if now >= target_time:
                    target_time += timedelta(days=1)
                
                wait_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                # Выполняем очистку
                await self._cleanup_old_data()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в планировщике очистки: {e}")
                await asyncio.sleep(3600)
    
    async def _send_birthday_congratulations(self):
        """Отправка поздравлений с днем рождения"""
        logger.info("Начинаю отправку поздравлений с днем рождения")
        
        try:
            today = get_msk_time().date()
            birthdays = await db.get_todays_birthdays(today)
            
            if not birthdays:
                logger.info("Сегодня нет дней рождения")
                return
            
            logger.info(f"Найдено {len(birthdays)} дней рождения на сегодня")
            
            for bd in birthdays:
                try:
                    # Проверяем, разрешен ли чат
                    if not await db.is_chat_allowed(bd['chat_id']):
                        logger.warning(f"Чат {bd['chat_id']} не разрешен, пропускаем")
                        continue
                    
                    # Получаем случайное поздравление
                    congrats = await db.get_random_congratulation()
                    if not congrats:
                        logger.warning("Нет поздравлений в базе")
                        continue
                    
                    # Формируем сообщение
                    username = f"@{bd['username']}" if bd['username'] else bd['full_name']
                    
                    message = f"🎉 Поздравляем {username} с днём рождения!\n\n"
                    message += congrats['text']
                    
                    # Отправляем сообщение через объект бота
                    await self.bot.send_message(
                        chat_id=bd['chat_id'],
                        text=message
                    )
                    
                    logger.info(f"Отправлено поздравление для {username} в чате {bd['chat_id']}")
                    
                    # Отмечаем как отправленное
                    await db.mark_birthday_sent(bd['user_id'], bd['chat_id'], congrats['id'])
                    
                    # Пауза между сообщениями чтобы избежать rate limit
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке поздравления для user_id={bd['user_id']}: {e}")
                    continue
            
            logger.info("Отправка поздравлений завершена")
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке поздравлений: {e}")
    
    async def _send_events(self):
        """Отправка событий"""
        logger.info("Начинаю отправку событий")
        
        try:
            today = get_msk_time().date()
            events = await db.get_todays_events(today)
            
            if not events:
                logger.info("Сегодня нет событий")
                return
            
            logger.info(f"Найдено {len(events)} событий на сегодня")
            
            for event in events:
                try:
                    # Проверяем, разрешен ли чат
                    if not await db.is_chat_allowed(event['chat_id']):
                        logger.warning(f"Чат {event['chat_id']} не разрешен, пропускаем")
                        continue
                    
                    # Формируем сообщение
                    message = f"🎉 {event['name']}\n\n"
                    message += event['message']
                    
                    # Отправляем сообщение
                    if event['media_type'] and event['media_id']:
                        # Отправляем с медиа
                        media_methods = {
                            'photo': self.bot.send_photo,
                            'video': self.bot.send_video,
                            'animation': self.bot.send_animation,
                            'document': self.bot.send_document,
                            'sticker': self.bot.send_sticker
                        }
                        
                        if event['media_type'] in media_methods:
                            await media_methods[event['media_type']](
                                chat_id=event['chat_id'],
                                **{event['media_type']: event['media_id']},
                                caption=message
                            )
                        else:
                            # Если тип медиа не поддерживается, отправляем только текст
                            await self.bot.send_message(
                                chat_id=event['chat_id'],
                                text=message
                            )
                    else:
                        # Отправляем только текст
                        await self.bot.send_message(
                            chat_id=event['chat_id'],
                            text=message
                        )
                    
                    logger.info(f"Отправлено событие '{event['name']}' в чате {event['chat_id']}")
                    
                    # Отмечаем как отправленное
                    await db.mark_event_sent(event['id'])
                    
                    # Пауза между сообщениями
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке события id={event['id']}: {e}")
                    continue
            
            logger.info("Отправка событий завершена")
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке событий: {e}")
    
    async def _send_monthly_reminders(self):
        """Отправка ежемесячных напоминаний"""
        if not Config.ENABLE_MONTHLY_REMINDER:
            return
        
        logger.info("Начинаю отправку ежемесячных напоминаний")
        
        try:
            chats = await db.get_all_allowed_chats()
            
            for chat in chats:
                try:
                    chat_id = chat['chat_id']
                    
                    # Получаем именинников текущего месяца
                    now = get_msk_time()
                    birthdays = await db.get_birthdays_by_chat(chat_id)
                    
                    # Фильтруем по текущему месяцу
                    month_birthdays = [
                        bd for bd in birthdays 
                        if bd['month'] == now.month
                    ]
                    
                    if not month_birthdays:
                        continue
                    
                    month_names = [
                        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
                    ]
                    
                    # Формируем сообщение
                    message = f"📅 Именинники {month_names[now.month-1]}:\n\n"
                    
                    for bd in month_birthdays:
                        username = f"@{bd['username']}" if bd['username'] else bd['full_name']
                        message += f"• {bd['day']} {month_names[now.month-1]} - {username}\n"
                    
                    message += "\nНе забудьте поздравить! 🎉"
                    
                    # Отправляем сообщение
                    await self.bot.send_message(
                        chat_id=chat_id,
                        text=message
                    )
                    
                    logger.info(f"Отправлено месячное напоминание в чат {chat_id}")
                    
                    # Пауза между сообщениями
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Ошибка при отправке месячного напоминания в чат {chat['chat_id']}: {e}")
                    continue
            
            logger.info("Отправка ежемесячных напоминаний завершена")
            
        except Exception as e:
            logger.error(f"Критическая ошибка при отправке месячных напоминаний: {e}")
    
    async def _cleanup_old_data(self):
        """Очистка устаревших данных"""
        logger.info("Начинаю очистку устаревших данных")
        
        try:
            # Удаляем старые записи о отправленных поздравлениях (старше 30 дней)
            thirty_days_ago = (get_msk_time() - timedelta(days=30)).date()
            
            await db.conn.execute('''
                DELETE FROM sent_congratulations 
                WHERE sent_date < ?
            ''', (thirty_days_ago,))
            
            # Удаляем старые записи о отправленных событиях (старше 30 дней)
            await db.conn.execute('''
                DELETE FROM sent_events 
                WHERE sent_date < ?
            ''', (thirty_days_ago,))
            
            # Деактивируем разовые события, которые уже прошли
            today = get_msk_time().date()
            await db.conn.execute('''
                UPDATE events 
                SET is_active = 0 
                WHERE year IS NOT NULL 
                AND (year < ? OR (year = ? AND (month < ? OR (month = ? AND day < ?))))
            ''', (today.year, today.year, today.month, today.month, today.day))
            
            await db.conn.commit()
            
            logger.info("Очистка устаревших данных завершена")
            
        except Exception as e:
            logger.error(f"Ошибка при очистке устаревших данных: {e}")
