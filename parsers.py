import re
import dateparser
import logging
from datetime import datetime, date
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

class DateParser:
    @staticmethod
    def parse_birthday(text: str) -> Optional[Tuple[int, int, Optional[int]]]:
        """
        Парсит дату дня рождения из текста.
        Возвращает (день, месяц, год) или (день, месяц, None) если год не указан.
        """
        try:
            # Убираем ключевые слова
            patterns_to_remove = [
                r'^мой\s*др\s*',
                r'^мой\s*день\s*рождения\s*',
                r'^др\s*'
            ]
            
            for pattern in patterns_to_remove:
                text = re.sub(pattern, '', text, flags=re.IGNORECASE)
            
            text = text.strip()
            
            if not text:
                return None
            
            # Настройки для парсинга русских дат
            # В новых версиях dateparser используется 'languages' вместо 'LANGUAGES'
            settings = {
                'DATE_ORDER': 'DMY',  # День-Месяц-Год
                'PREFER_LOCALE_DATE_ORDER': True,
                'languages': ['ru'],  # Исправлено: 'languages' вместо 'LANGUAGES'
                'PREFER_DAY_OF_MONTH': 'first',
                'REQUIRE_PARTS': ['day', 'month']  # Обязательно день и месяц
            }
            
            parsed_date = dateparser.parse(text, settings=settings)
            
            if not parsed_date:
                # Пробуем альтернативный вариант без явного указания языка
                parsed_date = dateparser.parse(text, date_formats=['%d.%m', '%d.%m.%Y', '%d %B', '%d %B %Y'])
            
            if not parsed_date:
                return None
            
            day = parsed_date.day
            month = parsed_date.month
            year = parsed_date.year
            
            # Проверяем, что дата валидна
            try:
                # Для проверки используем високосный год, чтобы проверить 29 февраля
                test_year = year if year else 2020  # 2020 - високосный
                datetime(test_year, month, day)
            except ValueError:
                logger.warning(f"Некорректная дата: день={day}, месяц={month}")
                return None
            
            # Если год равен текущему, считаем что год не указан
            if year and year == datetime.now().year:
                year = None
            
            return (day, month, year)
            
        except Exception as e:
            logger.error(f"Ошибка парсинга даты '{text}': {e}")
            return None
    
    @staticmethod
    def parse_event_command(text: str) -> Optional[Dict[str, Any]]:
        """
        Парсит команду добавления события.
        Формат: /add_event DD.MM[.YYYY] Название события
        Затем на следующей строке текст поздравления.
        """
        try:
            lines = text.strip().split('\n', 2)
            
            if len(lines) < 2:
                return None
            
            first_line = lines[0].strip()
            
            # Убираем команду
            first_line = re.sub(r'^/add_event\s*', '', first_line, flags=re.IGNORECASE)
            
            # Ищем дату в начале строки
            date_pattern = r'^(\d{1,2}[\.\/]\d{1,2}(?:[\.\/]\d{4})?|\d{1,2}\s+[а-яё]+(?:\s+\d{4})?)'
            match = re.match(date_pattern, first_line, re.IGNORECASE)
            
            if not match:
                return None
            
            date_str = match.group(1)
            event_name = first_line[match.end():].strip()
            
            # Парсим дату с исправленными настройками
            settings = {
                'DATE_ORDER': 'DMY',
                'PREFER_LOCALE_DATE_ORDER': True,
                'languages': ['ru']  # Исправлено: 'languages' вместо 'LANGUAGES'
            }
            
            parsed_date = dateparser.parse(date_str, settings=settings)
            
            if not parsed_date:
                return None
            
            # Определяем тип события
            has_year = parsed_date.year != datetime.now().year
            event_type = 'once' if has_year else 'yearly'
            
            message_text = lines[1].strip()
            
            if not message_text:
                return None
            
            return {
                'day': parsed_date.day,
                'month': parsed_date.month,
                'year': parsed_date.year if has_year else None,
                'event_name': event_name,
                'message_text': message_text,
                'event_type': event_type
            }
            
        except Exception as e:
            logger.error(f"Ошибка парсинга команды события: {e}")
            return None
    
    @staticmethod
    def extract_user_identifier(text: str) -> Optional[str]:
        """
        Извлекает идентификатор пользователя из текста.
        Может быть: @username, user_id, или имя.
        """
        try:
            # Убираем команду если есть
            text = re.sub(r'^/(?:dr|delete|add|force_congratulate)\s*', '', text, flags=re.IGNORECASE)
            text = text.strip()
            
            if not text:
                return None
            
            # Проверяем, это user_id (только цифры)
            if text.isdigit():
                return f"user_id:{text}"
            
            # Проверяем, это username (начинается с @)
            if text.startswith('@'):
                return f"username:{text[1:]}"
            
            # Иначе считаем это именем
            return f"name:{text}"
            
        except Exception as e:
            logger.error(f"Ошибка извлечения идентификатора пользователя: {e}")
            return None

class DateValidator:
    @staticmethod
    def is_valid_date(day: int, month: int, year: Optional[int] = None) -> bool:
        """Проверяет, существует ли такая дата"""
        try:
            # Для проверки используем високосный год если год не указан
            check_year = year if year else 2020  # 2020 - високосный
            datetime(check_year, month, day)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def is_leap_year(year: int) -> bool:
        """Проверяет, является ли год високосным"""
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
