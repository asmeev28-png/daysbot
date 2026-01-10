import logging
import pytz
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
import re

logger = logging.getLogger(__name__)

def format_birthday_list(birthdays: List[Dict[str, Any]]) -> str:
    """Форматирует список дней рождения в читаемый вид"""
    if not birthdays:
        return "📅 В этом чате пока нет дней рождений."
    
    # Месяца в именительном падеже (для заголовков)
    month_names_nominative = [
        'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
        'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
    ]
    
    # Месяца в родительном падеже (для дат)
    month_names_genitive = [
        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
    ]
    
    result = "📅 Дни рождения в этом чате:\n\n"
    
    # Группируем по месяцам
    birthdays_by_month = {}
    for bd in birthdays:
        month = bd['month']
        if month not in birthdays_by_month:
            birthdays_by_month[month] = []
        birthdays_by_month[month].append(bd)
    
    # Сортируем месяцы
    for month_num in sorted(birthdays_by_month.keys()):
        # Именительный падеж для заголовка
        month_name_nom = month_names_nominative[month_num - 1]
        result += f"**{month_name_nom}**:\n"
        
        # Сортируем дни в месяце
        month_birthdays = sorted(birthdays_by_month[month_num], key=lambda x: x['day'])
        
        for bd in month_birthdays:
            username = f"@{bd['username']}" if bd['username'] else bd['full_name']
            # Родительный падеж для дат
            result += f"• {bd['day']} {month_names_genitive[month_num-1]} - {username}\n"
        
        result += "\n"
    
    return result

def format_upcoming_birthdays(birthdays: List[Dict[str, Any]]) -> str:
    """Форматирует список ближайших дней рождения"""
    if not birthdays:
        return "🎂 Ближайших дней рождений нет."
    
    # Родительный падеж для дат
    month_names_genitive = [
        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
    ]
    
    today = datetime.now(pytz.timezone('Europe/Moscow')).date()
    
    result = "🎂 Ближайшие дни рождения:\n\n"
    
    for i, bd in enumerate(birthdays[:3], 1):
        # Вычисляем дату следующего дня рождения
        next_birthday_year = today.year
        next_birthday = date(next_birthday_year, bd['month'], bd['day'])
        
        if next_birthday < today:
            next_birthday = date(next_birthday_year + 1, bd['month'], bd['day'])
        
        days_until = (next_birthday - today).days
        
        username = f"@{bd['username']}" if bd['username'] else bd['full_name']
        # Родительный падеж для дат
        date_str = f"{bd['day']} {month_names_genitive[bd['month']-1]}"
        
        if days_until == 0:
            days_text = "🎉 сегодня!"
        elif days_until == 1:
            days_text = "завтра"
        else:
            days_text = f"через {days_until} дней"
        
        result += f"{i}. {username} - {date_str} ({days_text})\n"
    
    return result

def format_event_list(events: List[Dict[str, Any]]) -> str:
    """Форматирует список событий"""
    if not events:
        return "📅 В этом чате пока нет событий."
    
    month_names_genitive = [
        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
    ]
    
    result = "📅 События в этом чате:\n\n"
    
    for event in events:
        status = "✅" if event['is_active'] else "❌"
        # Родительный падеж для дат
        date_str = f"{event['day']} {month_names_genitive[event['month']-1]}"
        
        if event['year']:
            date_str += f" {event['year']} г."
            event_type = "разовое"
        else:
            event_type = "ежегодное"
        
        result += f"{status} **{date_str}** - {event['name']}\n"
        result += f"   Тип: {event_type}, ID: {event['id']}\n\n"
    
    return result

def get_msk_time() -> datetime:
    """Получить текущее время по Москве (фиксированный UTC+3)"""
    utc_now = datetime.utcnow()
    msk_offset = timedelta(hours=3)
    return utc_now + msk_offset

def calculate_next_birthday(birth_day: int, birth_month: int, today: date) -> date:
    """Вычисляет дату следующего дня рождения"""
    year = today.year
    next_birthday = date(year, birth_month, birth_day)
    
    if next_birthday < today:
        next_birthday = date(year + 1, birth_month, birth_day)
    
    return next_birthday

def parse_time_string(time_str: str) -> Optional[Tuple[int, int]]:
    """Парсит строку времени формата HH:MM"""
    try:
        match = re.match(r'^(\d{1,2}):(\d{2})$', time_str)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2))
            
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return (hour, minute)
    except:
        pass
    
    return None

def escape_markdown(text: str) -> str:
    """Экранирует символы Markdown"""
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, '\\' + char)
    return text
