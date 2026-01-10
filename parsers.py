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
            # Убираем ключевые слова (регистронезависимо)
            patterns_to_remove = [
                r'^мой\s*др\s*',
                r'^мой\s*день\s*рождения\s*',
                r'^др\s*'
            ]
            
            text_lower = text.lower()  # Для регистронезависимого поиска
            
            for pattern in patterns_to_remove:
                text_lower = re.sub(pattern, '', text_lower, flags=re.IGNORECASE)
            
            text_clean = text_lower.strip()
            
            if not text_clean:
                return None
            
            # Упрощенный парсинг без устаревших настроек
            # Вариант 1: Прямой парсинг с локализацией
            try:
                # Новый API dateparser (после версии 1.1.x)
                parsed_date = dateparser.parse(
                    text_clean, 
                    languages=['ru'],  # Просто передаем languages как параметр
                    settings={
                        'DATE_ORDER': 'DMY',
                        'PREFER_DAY_OF_MONTH': 'first'
                    }
                )
            except TypeError:
                # Если не поддерживается languages как параметр
                parsed_date = dateparser.parse(
                    text_clean,
                    settings={
                        'DATE_ORDER': 'DMY',
                        'PREFER_DAY_OF_MONTH': 'first',
                        'LOCALES': ['ru']  # Альтернативная настройка
                    }
                )
            
            if not parsed_date:
                # Вариант 2: Пробуем явные форматы дат
                date_formats = [
                    '%d.%m',       # 28.06
                    '%d.%m.%Y',    # 28.06.1998
                    '%d %B',       # 28 июня
                    '%d %B %Y',    # 28 июня 1998
                    '%d/%m',       # 28/06
                    '%d/%m/%Y',    # 28/06/1998
                    '%d %b',       # 28 июн
                    '%d %b %Y',    # 28 июн 1998
                ]
                
                for date_format in date_formats:
                    try:
                        parsed_date = datetime.strptime(text_clean, date_format)
                        break
                    except ValueError:
                        continue
            
            if not parsed_date:
                return None
            
            day = parsed_date.day
            month = parsed_date.month
            year = parsed_date.year if hasattr(parsed_date, 'year') else None
            
            # Проверяем, что дата валидна
            try:
                # Для проверки используем високосный год, чтобы проверить 29 февраля
                test_year = year if year else 2020  # 2020 - високосный
                datetime(test_year, month, day)
            except ValueError:
                logger.warning(f"Некорректная дата: день={day}, месяц={month}")
                return None
            
            # Если год равен текущему, считаем что год не указан
            current_year = datetime.now().year
            if year and year == current_year:
                year = None
            
            # Если год в будущем (например, 2025 при текущем 2024), исправляем
            if year and year > current_year:
                # Скорее всего пользователь ввел год рождения, а не будущий год
                # Проверяем, если год рождения вероятный (не старше 120 лет и не в будущем)
                if year > current_year or year < current_year - 120:
                    year = None  # Игнорируем нереалистичный год
            
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
            # Приводим к нижнему регистру для регистронезависимого поиска
            text_lower = text.lower()
            lines = text_lower.strip().split('\n', 2)
            
            if len(lines) < 2:
                return None
            
            first_line = lines[0].strip()
            
            # Убираем команду (регистронезависимо)
            first_line = re.sub(r'^/add_event\s*', '', first_line, flags=re.IGNORECASE)
            
            # Ищем дату в начале строки
            date_pattern = r'^(\d{1,2}[\.\/]\d{1,2}(?:[\.\/]\d{4})?|\d{1,2}\s+[а-яё]+(?:\s+\d{4})?)'
            match = re.match(date_pattern, first_line)
            
            if not match:
                return None
            
            date_str = match.group(1)
            # Восстанавливаем оригинальный регистр для названия события
            original_first_line = text.strip().split('\n', 2)[0]
            original_first_line = re.sub(r'^/add_event\s*', '', original_first_line, flags=re.IGNORECASE)
            event_name = original_first_line[match.end():].strip()
            
            # Парсим дату
            parsed_date = dateparser.parse(
                date_str,
                languages=['ru'],
                settings={'DATE_ORDER': 'DMY'}
            )
            
            if not parsed_date:
                return None
            
            # Определяем тип события
            current_year = datetime.now().year
            has_year = parsed_date.year != current_year
            event_type = 'once' if has_year else 'yearly'
            
            # Используем оригинальный текст поздравления (сохраняем регистр)
            original_lines = text.strip().split('\n', 2)
            message_text = original_lines[1].strip() if len(original_lines) > 1 else ""
            
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
            # Убираем команду если есть (регистронезависимо)
            text_clean = re.sub(r'^/(?:dr|delete|add|force_congratulate)\s*', '', text, flags=re.IGNORECASE)
            text_clean = text_clean.strip()
            
            if not text_clean:
                return None
            
            # Проверяем, это user_id (только цифры)
            if text_clean.isdigit():
                return f"user_id:{text_clean}"
            
            # Проверяем, это username (начинается с @)
            if text_clean.startswith('@'):
                return f"username:{text_clean[1:].lower()}"  # username всегда в нижнем регистре
            
            # Иначе считаем это именем (сохраняем оригинальный регистр для отображения)
            return f"name:{text_clean}"
            
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
