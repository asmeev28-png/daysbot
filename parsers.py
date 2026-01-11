# parsers.py - полностью переписанный класс DateParser
import re
import logging
from datetime import datetime, date
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)

class DateParser:
    @staticmethod
    def parse_birthday(text: str) -> Optional[Tuple[int, int, Optional[int]]]:
        """
        НАДЕЖНЫЙ парсер дат без использования dateparser
        """
        try:
            # Убираем ключевые слова (регистронезависимо)
            text_lower = text.lower()
            patterns_to_remove = [
                r'мой\s*др\s*',
                r'мой\s*день\s*рождения\s*',
                r'др\s*'
            ]
            
            for pattern in patterns_to_remove:
                text_lower = re.sub(pattern, '', text_lower)
            
            text_clean = text_lower.strip()
            
            if not text_clean:
                return None
            
            # Словарь месяцев
            month_names = {
                'января': 1, 'янв': 1, 'январь': 1,
                'февраля': 2, 'фев': 2, 'февраль': 2,
                'марта': 3, 'мар': 3, 'март': 3,
                'апреля': 4, 'апр': 4, 'апрель': 4,
                'мая': 5, 'май': 5,
                'июня': 6, 'июнь': 6,
                'июля': 7, 'июль': 7,
                'августа': 8, 'авг': 8, 'август': 8,
                'сентября': 9, 'сен': 9, 'сентябрь': 9,
                'октября': 10, 'окт': 10, 'октябрь': 10,
                'ноября': 11, 'ноя': 11, 'ноябрь': 11,
                'декабря': 12, 'дек': 12, 'декабрь': 12
            }
            
            day = None
            month = None
            year = None
            
            # Вариант 1: Формат DD.MM[.YYYY] или DD/MM[/YYYY]
            match = re.match(r'^(\d{1,2})[\.\/](\d{1,2})(?:[\.\/](\d{2,4}))?$', text_clean)
            if match:
                day = int(match.group(1))
                month = int(match.group(2))
                if match.group(3):
                    year = int(match.group(3))
                    # Корректируем год если введен 2-значный
                    if year < 100:
                        year += 2000 if year <= 50 else 1900
            
            # Вариант 2: Формат DD месяц [YYYY]
            if not day or not month:
                pattern = r'^(\d{1,2})\s+([а-яё]+)(?:\s+(\d{2,4}))?$'
                match = re.match(pattern, text_clean)
                if match:
                    day = int(match.group(1))
                    month_name = match.group(2).lower()
                    month = month_names.get(month_name)
                    if match.group(3):
                        year = int(match.group(3))
                        if year < 100:
                            year += 2000 if year <= 50 else 1900
            
            # Вариант 3: Формат месяц DD [YYYY]
            if not day or not month:
                pattern = r'^([а-яё]+)\s+(\d{1,2})(?:\s+(\d{2,4}))?$'
                match = re.match(pattern, text_clean)
                if match:
                    month_name = match.group(1).lower()
                    month = month_names.get(month_name)
                    day = int(match.group(2))
                    if match.group(3):
                        year = int(match.group(3))
                        if year < 100:
                            year += 2000 if year <= 50 else 1900
            
            if not day or not month:
                return None
            
            # Проверяем валидность даты
            try:
                test_year = year if year else 2024  # 2024 - високосный
                datetime(test_year, month, day)
            except ValueError:
                logger.warning(f"Некорректная дата: день={day}, месяц={month}")
                return None
            
            # Если год равен текущему или в будущем, не сохраняем
            current_year = datetime.now().year
            if year:
                if year == current_year:
                    year = None  # Год не указан
                elif year > current_year or year < current_year - 120:
                    year = None  # Нереалистичный год
            
            return (day, month, year)
            
        except Exception as e:
            logger.error(f"Ошибка парсинга даты '{text}': {e}")
            return None
    
        @staticmethod
        def parse_event_command(text: str) -> Optional[Dict[str, Any]]:
            """
            Парсинг команды добавления события
            ВСЕ события теперь ежегодные
            """
            try:
                # Приводим к нижнему регистру для поиска даты, но сохраняем оригинал
                text_lower = text.lower()
                lines = text.strip().split('\n', 2)  # Оригинальный текст с регистром
            
                if len(lines) < 2:
                    return None
            
                first_line_original = lines[0].strip()
                first_line = first_line_original.lower()
            
                # Убираем команду
                first_line = re.sub(r'^/add_event\s*', '', first_line, flags=re.IGNORECASE)
                first_line_original = re.sub(r'^/add_event\s*', '', first_line_original, flags=re.IGNORECASE)
            
                # Ищем дату в начале строки
                # Паттерны для поиска даты
                patterns = [
                    r'^(\d{1,2})[\.\/](\d{1,2})(?:[\.\/](\d{4}))?',  # 10.06 или 10.06.2024
                    r'^(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?',        # 10 июня или 10 июня 2024
                    r'^([а-яё]+)\s+(\d{1,2})(?:\s+(\d{4}))?'         # июня 10 или июня 10 2024
                ]
            
                date_match = None
                date_format = None
            
                for pattern in patterns:
                    date_match = re.match(pattern, first_line)
                    if date_match:
                        date_format = pattern
                        break
            
                if not date_match:
                    return None
            
                day = month = year = None
                month_names = {
                    'января': 1, 'янв': 1, 'январь': 1,
                    'февраля': 2, 'фев': 2, 'февраль': 2,
                    'марта': 3, 'мар': 3, 'март': 3,
                    'апреля': 4, 'апр': 4, 'апрель': 4,
                    'мая': 5, 'май': 5,
                    'июня': 6, 'июнь': 6,
                    'июля': 7, 'июль': 7,
                    'августа': 8, 'авг': 8, 'август': 8,
                    'сентября': 9, 'сен': 9, 'сентябрь': 9,
                    'октября': 10, 'окт': 10, 'октябрь': 10,
                    'ноября': 11, 'ноя': 11, 'ноябрь': 11,
                    'декабря': 12, 'дек': 12, 'декабрь': 12
                }
            
                if 'а-я' in date_format:  # Текстовый месяц
                    if date_match.group(1).isalpha():  # Месяц день
                        month_name = date_match.group(1).lower()
                        month = month_names.get(month_name)
                        day = int(date_match.group(2))
                        if date_match.group(3):
                            year = int(date_match.group(3))
                    else:  # День месяц
                        day = int(date_match.group(1))
                        month_name = date_match.group(2).lower()
                        month = month_names.get(month_name)
                        if date_match.group(3):
                            year = int(date_match.group(3))
                else:  # Числовой формат
                    day = int(date_match.group(1))
                    month = int(date_match.group(2))
                    if date_match.group(3):
                        year = int(date_match.group(3))
            
                if not day or not month:
                    return None
            
                # Получаем название события (после даты)
                event_start = date_match.end()
                event_name = first_line_original[event_start:].strip()
            
                # Текст поздравления
                message_text = lines[1].strip()
            
                if not message_text:
                    return None
            
                # ВСЕ события теперь ежегодные, но год сохраняем для информации
                # Если год указан, но он равен текущему - не сохраняем
                current_year = datetime.now().year
                if year and year == current_year:
                    year = None
            
                return {
                    'day': day,
                    'month': month,
                    'year': year,  # Сохраняем только исторические года (> текущего)
                    'event_name': event_name,
                    'message_text': message_text
                    # Убрали event_type - все события ежегодные
                }
            
            except Exception as e:
                logger.error(f"Ошибка парсинга команды события: {e}")
                return None
    
    @staticmethod
    def extract_user_identifier(text: str) -> Optional[str]:
        """Извлекает идентификатор пользователя (регистронезависимо)"""
        try:
            # Убираем команду
            text_clean = re.sub(r'^/(?:dr|delete|add|force_congratulate)\s*', '', text, flags=re.IGNORECASE)
            text_clean = text_clean.strip()
            
            if not text_clean:
                return None
            
            # user_id
            if text_clean.isdigit():
                return f"user_id:{text_clean}"
            
            # username (приводим к нижнему регистру)
            if text_clean.startswith('@'):
                return f"username:{text_clean[1:].lower()}"
            
            # имя
            return f"name:{text_clean}"
            
        except Exception as e:
            logger.error(f"Ошибка извлечения идентификатора пользователя: {e}")
            return None

class DateValidator:
    @staticmethod
    def is_valid_date(day: int, month: int, year: Optional[int] = None) -> bool:
        """Проверяет, существует ли такая дата"""
        try:
            check_year = year if year else 2024  # 2024 - високосный
            datetime(check_year, month, day)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def is_leap_year(year: int) -> bool:
        """Проверяет, является ли год високосным"""
        return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
