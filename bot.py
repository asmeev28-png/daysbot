import re
import logging
import asyncio
from datetime import datetime
from typing import Optional, List

from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackContext
)
from telegram.constants import ParseMode

from config import Config
from database import db
from parsers import DateParser
from scheduler import Scheduler
from utils import (
    format_birthday_list, format_upcoming_birthdays,
    format_event_list, escape_markdown, get_msk_time
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG  # Измените на DEBUG для подробных логов
)

logger = logging.getLogger(__name__)

class BirthdayBot:
    def __init__(self):
        self.application: Optional[Application] = None
        self.scheduler: Optional[Scheduler] = None
    
    async def start(self):
        """Запуск бота для PTB v20+"""
        try:
            # Простое создание Application без дополнительных опций
            self.application = Application.builder().token(Config.BOT_TOKEN).build()
                        
            # Сохраняем данные в application.bot_data для доступа из обработчиков
            self.application.bot_data['db'] = db
            self.application.bot_data['owner_id'] = Config.BOT_OWNER_ID
            
            # Регистрируем обработчики
            self._register_handlers()
            
            # Настройка команд меню
            await self._set_commands()
            
            # Инициализация и запуск планировщика
            self.scheduler = Scheduler(self.application.bot)
            await self.scheduler.start()
            
            # Запуск опроса (polling) через application
            logger.info(f"Бот запущен! Владелец: {Config.BOT_OWNER_ID}, Резервный: {Config.BACKUP_ADMIN_ID}")
            await self.application.initialize()
            await self.application.start()
            
            # Запускаем polling
            await self.application.updater.start_polling(drop_pending_updates=True)
            
            # Бот работает. Ожидаем сигнала остановки.
            await asyncio.Event().wait()
            
        except Exception as e:
            logger.error(f"Критическая ошибка при запуске бота: {e}", exc_info=True)
            raise
            
    async def _post_init(self, application: Application):
        """Пост-инициализация Application"""
        # Отключаем встроенную обработку неизвестных команд
        if hasattr(application, 'arbitrary_callback_data'):
            # Эта настройка помогает игнорировать неизвестные команды
            pass
        
    def _register_handlers(self):
        """Регистрация всех обработчиков команд для PTB v20+"""
        # ОБЩИЕ КОМАНДЫ
        self.application.add_handler(CommandHandler("start", self._handle_start))
        self.application.add_handler(CommandHandler("about", self._handle_about))
        self.application.add_handler(CommandHandler("mybirthday", self._handle_mybirthday))
        self.application.add_handler(CommandHandler("birthlist", self._handle_birthlist))
        self.application.add_handler(CommandHandler("dr", self._handle_dr_search))
        self.application.add_handler(CommandHandler("whoisnext", self._handle_whoisnext))
        self.application.add_handler(CommandHandler("list_events", self._handle_list_events))
        self.application.add_handler(CommandHandler("next_events", self._handle_next_events))
        self.application.add_handler(CommandHandler("debug", self._handle_debug, filters=filters.ChatType.GROUPS))
        self.application.add_handler(CommandHandler("add", self._handle_add_with_reply, filters=filters.ChatType.GROUPS))
        self.application.add_handler(CommandHandler(
            "force_congratulate", 
            self._handle_force_congratulate_reply, 
            filters=filters.ChatType.GROUPS
        ))

        async def _handle_force_congratulate_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
            """Обработчик /force_congratulate через reply на сообщение"""
            message = update.message
            chat = update.effective_chat
    
            # Проверяем, что это reply на чье-то сообщение
            if not message.reply_to_message:
                # Если не reply, передаем обычному обработчику
                return await self._handle_force_congratulate(update, context)
    
            replied_user = message.reply_to_message.from_user
    
            # Проверяем права
            db_conn = context.bot_data['db']
            admins = await chat.get_administrators()
            admin_ids = [admin.user.id for admin in admins]
    
            if update.effective_user.id not in admin_ids and update.effective_user.id not in Config.get_owners():
                await update.message.reply_text("❌ Только администраторы могут принудительно поздравлять.")
                return
    
            # Проверяем, разрешен ли чат
            if not await db_conn.is_chat_allowed(chat.id):
                return await self._handle_command_in_disallowed_chat(update, context)
    
            # Получаем информацию о пользователе
            target_user_id = replied_user.id
            target_username = replied_user.username
            target_full_name = replied_user.full_name
    
            # Проверяем, есть ли день рождения
            birthday = await db_conn.get_birthday(target_user_id, chat.id)
            has_birthday = False
            birthday_info = ""
    
            if birthday:
                has_birthday = True
                month_names = [
                    'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                    'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
                ]
        
                date_str = f"{birthday['day']} {month_names[birthday['month']-1]}"
        
                if birthday['year']:
                    date_str += f" {birthday['year']} года"
        
                birthday_info = f"\n🎂 День рождения: {date_str}"
    
            # Получаем случайное поздравление
            congrats = await db_conn.get_random_congratulation()
    
            if not congrats:
                await update.message.reply_text("❌ Нет поздравлений в базе.")
                return
    
            # Формируем сообщение
            username_display = f"@{target_username}" if target_username else target_full_name
    
            message_text = f"🎉 Принудительное поздравление для {username_display}!\n"
    
            if has_birthday:
                message_text += birthday_info + "\n"
            else:
                message_text += "📝 (День рождения не указан)\n"
    
            message_text += f"\n{congrats['text']}"
    
            # Отправляем сообщение
            await update.message.reply_text(message_text)
    
            # Отмечаем как отправленное
            if has_birthday:
                await db_conn.mark_birthday_sent(target_user_id, chat.id, congrats['id'])
    
            logger.info(f"Отправлено принудительное поздравление через reply для user_id={target_user_id}")
    
        # Обработчик добавления ДР через сообщение
        self.application.add_handler(MessageHandler(
            filters.Regex(re.compile(r'^(мой\s+др|мой\s+день\s+рождения|др)\s+.+', re.IGNORECASE)) &
            filters.ChatType.GROUPS,
            self._handle_birthday_message
        ))
        
        # АДМИНСКИЕ КОМАНДЫ
        self.application.add_handler(CommandHandler(
            "add", self._handle_add_birthday_admin, filters=filters.ChatType.GROUPS
        ))
        self.application.add_handler(CommandHandler(
            "delete", self._handle_delete_birthday, filters=filters.ChatType.GROUPS
        ))
        self.application.add_handler(CommandHandler(
            "force_congratulate", self._handle_force_congratulate, filters=filters.ChatType.GROUPS
        ))
        self.application.add_handler(MessageHandler(
            filters.Regex(re.compile(r'^/add_event\s+.+', re.IGNORECASE)) & filters.ChatType.GROUPS,
            self._handle_add_event
        ))
        self.application.add_handler(CommandHandler(
            "delete_event", self._handle_delete_event, filters=filters.ChatType.GROUPS
        ))
        self.application.add_handler(CommandHandler(
            "toggle_event", self._handle_toggle_event, filters=filters.ChatType.GROUPS
        ))
        
        # КОМАНДЫ ВЛАДЕЛЬЦА (только в ЛС)
        owner_filter = filters.ChatType.PRIVATE & filters.User(Config.get_owners())
        self.application.add_handler(CommandHandler("add_chat", self._handle_add_chat_owner, filters=owner_filter))
        self.application.add_handler(CommandHandler("remove_chat", self._handle_remove_chat_owner, filters=owner_filter))
        self.application.add_handler(CommandHandler("list_chats", self._handle_list_chats_owner, filters=owner_filter))
        self.application.add_handler(CommandHandler("stats", self._handle_stats_owner, filters=owner_filter))
        self.application.add_handler(CommandHandler("owner_help", self._handle_owner_help, filters=owner_filter))
        self.application.add_handler(MessageHandler(
            filters.Document.TEXT & owner_filter,
            self._handle_upload_congrats
        ))
        
        # ОБРАБОТЧИКИ СОБЫТИЙ ЧАТА
        self.application.add_handler(MessageHandler(
            filters.StatusUpdate.LEFT_CHAT_MEMBER,
            self._handle_user_left
        ))
        self.application.add_handler(MessageHandler(
            filters.StatusUpdate.NEW_CHAT_MEMBERS,
            self._handle_new_chat_members
        ))
        
        # Обработчик команд в неразрешенных чатах
        self.application.add_handler(MessageHandler(
            filters.ChatType.GROUPS & filters.COMMAND,
            self._handle_command_check
        ))
        
        # Обработчик подтверждений для владельца
        self.application.add_handler(MessageHandler(
            filters.TEXT & owner_filter,
            self._handle_confirmation
        ))
      
        # 8. В САМОМ КОНЦЕ - пустой обработчик для неизвестных команд
        self.application.add_handler(MessageHandler(
            filters.COMMAND,
            self._handle_ignore_command
        ))  

        # Глобальный обработчик ошибок
        self.application.add_error_handler(self._error_handler)

    async def _handle_command_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Проверяет команду и либо обрабатывает, либо игнорирует"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        command = update.message.text.split()[0].lower()
    
        # Список известных команд
        known_commands = [
            '/start', '/about', '/mybirthday', '/birthlist', '/dr', '/whoisnext',
            '/list_events', '/next_events', '/add', '/delete', '/force_congratulate',
            '/add_event', '/delete_event', '/toggle_event', '/add_chat', '/remove_chat',
            '/list_chats', '/stats', '/owner_help'
        ]
    
        # Если команда известна - проверяем разрешен ли чат
        if command in known_commands:
            if not await db_conn.is_chat_allowed(chat.id):
                await self._handle_command_in_disallowed_chat(update, context)
            # Если чат разрешен - команда обработается соответствующим обработчиком выше
        else:
            # Неизвестная команда - просто игнорируем
            logger.debug(f"Игнорируем неизвестную команду: {command}")
            # НИЧЕГО не делаем
    
    async def _handle_ignore_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Пустой обработчик для полного игнорирования команд"""
        # АБСОЛЮТНО НИЧЕГО не делаем
        pass
        
    async def _handle_debug(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Команда для отладки"""
        chat = update.effective_chat
        user = update.effective_user
    
        # Проверяем права бота
        try:
            bot_member = await chat.get_member(context.bot.id)
            bot_is_admin = bot_member.status in ['administrator', 'creator']
        except Exception as e:
            bot_is_admin = False
    
        # Пытаемся получить список участников
        try:
            member_count = await chat.get_member_count()
        except Exception as e:
            member_count = "не удалось получить"
    
        message = (
            f"🔧 Информация для отладки:\n\n"
            f"Чат ID: `{chat.id}`\n"
            f"Название: {chat.title}\n"
            f"Бот админ: {'✅' if bot_is_admin else '❌'}\n"
            f"Участников: {member_count}\n"
            f"Ваш ID: `{user.id}`\n"
            f"Ваш username: @{user.username if user.username else 'нет'}\n"
            f"Ваше имя: {user.full_name}"
        )
    
        await update.message.reply_text(message, parse_mode='Markdown')


    async def _handle_add_with_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик /add через reply на сообщение пользователя"""
        message = update.message
        chat = update.effective_chat
    
        # Проверяем, что это reply на чье-то сообщение
        if not message.reply_to_message:
            # Если не reply, передаем старому обработчику
            return await self._handle_add_birthday_admin(update, context)
    
        replied_user = message.reply_to_message.from_user
    
        if len(context.args) < 1:
            await update.message.reply_text(
                "Использование: Ответьте на сообщение пользователя и напишите:\n"
                "`/add [дата]`\n\n"
                "Пример: `/add 11.01`\n\n"
                f"Будет добавлен: {replied_user.full_name} (@{replied_user.username if replied_user.username else 'нет username'})"
            )
            return
    
        date_arg = ' '.join(context.args)
    
        # Парсим дату
        parsed = DateParser.parse_birthday(f"др {date_arg}")
    
        if not parsed:
            await update.message.reply_text("❌ Не удалось распознать дату.")
            return
    
        day, month, year = parsed
    
        # Проверяем существование даты
        from parsers import DateValidator
        if not DateValidator.is_valid_date(day, month, year):
            await update.message.reply_text("❌ Такой даты не существует.")
            return
    
        # Добавляем в базу
        db_conn = context.bot_data['db']
        success = await db_conn.add_birthday(
            user_id=replied_user.id,
            chat_id=chat.id,
            day=day,
            month=month,
            year=year,
            username=replied_user.username,
            full_name=replied_user.full_name,
            created_by=update.effective_user.id
        )
    
        if success:
            month_names = [
                'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
            ]
        
            date_str = f"{day} {month_names[month-1]}"
            if year:
                date_str += f" {year} года"
        
            username_display = f"@{replied_user.username}" if replied_user.username else replied_user.full_name
            await update.message.reply_text(f"✅ День рождения для {username_display} добавлен: {date_str}")
        else:
            await update.message.reply_text("❌ Ошибка при добавлении.")
    
    async def _set_commands(self):
        """Установка команд меню"""
        commands = [
            BotCommand("mybirthday", "Мой день рождения"),
            BotCommand("birthlist", "Список всех дней рождений"),
            BotCommand("dr", "Найти день рождения"),
            BotCommand("whoisnext", "Ближайшие дни рождения"),
            BotCommand("list_events", "Список событий"),
            BotCommand("next_events", "Ближайшие события"),
            BotCommand("about", "О боте"),
        ]
        await self.application.bot.set_my_commands(commands)
    
    # ========== ОБЩИЕ ОБРАБОТЧИКИ ==========
    
    async def _handle_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        chat = update.effective_chat
        
        if chat.type == 'private':
            message = (
                "👋 Привет! Я бот для дней рождения.\n\n"
                "Я могу:\n"
                "• Поздравлять участников с днём рождения\n"
                "• Напоминать о приближающихся днях рождения\n"
                "• Хранить памятные даты и события\n\n"
                "Чтобы добавить меня в чат:\n"
                "1. Добавьте меня в групповой чат\n"
                "2. Назначьте администратором\n"
                "3. Напишите в чате любую команду\n"
                "4. Я покажу ID чата\n"
                "5. Сообщите ID владельцу бота для активации\n\n"
                "Команды владельца (только в ЛС):\n"
                "/add_chat - добавить чат\n"
                "/list_chats - список чатов\n"
                "/owner_help - все команды"
            )
        else:
            # Проверяем, разрешен ли чат
            if not await db_conn.is_chat_allowed(chat.id):
                message = (
                    "❌ Этот бот не активирован в данном чате.\n\n"
                    f"ID чата: `{chat.id}`\n"
                    f"Название: {chat.title or 'Без названия'}\n\n"
                    "Для активации бота администратору чата необходимо:\n"
                    "1. Скопировать ID чата выше\n"
                    "2. Обратиться к владельцу бота\n"
                    "3. Предоставить владельцу ID чата для активации"
                )
            else:
                message = (
                    "👋 Привет! Я бот для дней рождения.\n\n"
                    "Доступные команды:\n"
                    "• `мой др [дата]` - добавить свой день рождения\n"
                    "• `/mybirthday` - показать свою дату\n"
                    "• `/birthlist` - список всех дней рождений\n"
                    "• `/whoisnext` - ближайшие дни рождения\n"
                    "• `/dr [имя]` - найти день рождения\n\n"
                    "Для администраторов:\n"
                    "• `/add [пользователь] [дата]` - добавить ДР\n"
                    "• `/delete [пользователь]` - удалить ДР\n"
                    "• `/add_event` - добавить событие\n"
                    "• `/list_events` - список событий"
                )
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _handle_about(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /about"""
        message = (
            "🎂 **Бот для дней рождения**\n\n"
            "Функции:\n"
            "• Автоматические поздравления в 09:00 MSK\n"
            "• Хранение дней рождения участников\n"
            "• Памятные даты и события\n"
            "• Ежемесячные напоминания\n"
            "• Поиск дней рождения\n\n"
            "Добавить день рождения:\n"
            "• `мой др 28.06`\n"
            "• `мой др 28 июня`\n"
            "• `мой др 28.06.1998`\n\n"
            "Используйте `/start` для списка команд.\n"
            "Вопросы и предложения: @yasmeev"
        )
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _handle_mybirthday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /mybirthday"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        chat = update.effective_chat
        
        # Проверяем, разрешен ли чат
        if chat.type != 'private' and not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Получаем день рождения пользователя в этом чате
        birthday = await db_conn.get_birthday(user.id, chat.id)
        
        if not birthday:
            await update.message.reply_text(
                "❌ Ваш день рождения не указан в этом чате.\n\n"
                "Добавьте его командой:\n"
                "`мой др [дата]`\n\n"
                "Примеры:\n"
                "• `мой др 28.06`\n"
                "• `мой др 28 июня`\n"
                "• `мой др 28.06.1998`"
            )
            return
        
        month_names = [
            'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
        ]
        
        date_str = f"{birthday['day']} {month_names[birthday['month']-1]}"
        
        if birthday['year']:
            date_str += f" {birthday['year']} года"
        
        message = f"📅 Ваш день рождения: {date_str}"
        
        await update.message.reply_text(message)
    
    async def _handle_birthlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /birthlist"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        
        # Проверяем, разрешен ли чат
        if chat.type != 'private' and not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Получаем все дни рождения в чате
        birthdays = await db_conn.get_birthdays_by_chat(chat.id)
        
        # Форматируем список
        message = format_birthday_list(birthdays)
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _handle_dr_search(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /dr - поиск дня рождения"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        
        # Проверяем, разрешен ли чат
        if chat.type != 'private' and not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите имя пользователя, username или ID.\n"
                "Примеры:\n"
                "• `/dr @username`\n"
                "• `/dr 123456789`\n"
                "• `/dr Имя Фамилия`"
            )
            return
        
        search_term = ' '.join(context.args).lower()  # Приводим к нижнему регистру
        
        # Получаем все дни рождения в чате
        birthdays = await db_conn.get_birthdays_by_chat(chat.id)
        
        # Ищем совпадения
        results = []
        for bd in birthdays:
            username_lower = bd['username'].lower() if bd['username'] else ''
            fullname_lower = bd['full_name'].lower() if bd['full_name'] else ''
            
            # Проверяем разные варианты совпадения
            if (username_lower and search_term in username_lower) or \
               (fullname_lower and search_term in fullname_lower) or \
               str(bd['user_id']) == search_term:
                results.append(bd)
        
        if not results:
            await update.message.reply_text("❌ Пользователь не найден.")
            return
        
        month_names = [
            'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
        ]
        
        if len(results) == 1:
            bd = results[0]
            date_str = f"{bd['day']} {month_names[bd['month']-1]}"
            
            if bd['year']:
                date_str += f" {bd['year']} года"
            
            username = f"@{bd['username']}" if bd['username'] else bd['full_name']
            message = f"📅 {username}: {date_str}"
        else:
            message = "📅 Найдено несколько пользователей:\n\n"
            for bd in results[:5]:  # Ограничиваем 5 результатами
                date_str = f"{bd['day']} {month_names[bd['month']-1]}"
                username = f"@{bd['username']}" if bd['username'] else bd['full_name']
                message += f"• {username}: {date_str}\n"
            
            if len(results) > 5:
                message += f"\n... и еще {len(results) - 5}"
        
        await update.message.reply_text(message)
    
    async def _handle_whoisnext(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /whoisnext"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        
        # Проверяем, разрешен ли чат
        if chat.type != 'private' and not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Получаем ближайшие дни рождения
        birthdays = await db_conn.get_upcoming_birthdays(chat.id, limit=3)
        
        # Форматируем список
        message = format_upcoming_birthdays(birthdays)
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _handle_list_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /list_events"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
    
        # Проверяем, разрешен ли чат - ТОЛЬКО для групповых чатов
        if chat.type != 'private':
            if not await db_conn.is_chat_allowed(chat.id):
                # Вместо ошибки, покажем события если они есть
                logger.info(f"Чат {chat.id} не в белом списке, но покажем события если есть")
    
        # Получаем события чата - ВСЕ события
        cursor = await db_conn.conn.execute(
            'SELECT * FROM events WHERE chat_id = ? ORDER BY month, day',
            (chat.id,)
        )
        rows = await cursor.fetchall()
        events = [dict(row) for row in rows]
    
        if not events:
            await update.message.reply_text(
                "📅 Событий пока нет.\n\n"
                "Чтобы добавить событие (админы):\n"
                "`/add_event 01.05 Название события`\n"
                "Текст поздравления на следующей строке\n\n"
                "Пример:\n"
                "`/add_event 01.05 День весны и труда`\n"
                "Поздравляем с 1 мая! Ура!"
            )
            return
    
        # Форматируем список
        message = "📅 **Список событий:**\n\n"
    
        month_names = [
            'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
        ]
    
        for event in events:
            date_str = f"{event['day']} {month_names[event['month']-1]}"
        
            if event['year']:
                date_str += f" ({event['year']} г.)"
        
            status = "✅" if event['is_active'] else "❌"
        
            message += f"{status} **{event['name']}**\n"
            message += f"   📅 {date_str}\n"
            message += f"   ID: {event['id']}\n"
        
            if event['message']:
                message_preview = event['message'][:50] + "..." if len(event['message']) > 50 else event['message']
                message += f"   💬 {message_preview}\n"
        
            message += "\n"
    
        message += "\n**Управление событиями:**\n"
        message += "• `/add_event [дата] [название]` + текст - добавить\n"
        message += "• `/delete_event [ID]` - удалить событие\n"
        message += "• `/toggle_event [ID]` - вкл/выкл событие\n"
        message += "• `/next_events` - ближайшие события"
    
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _handle_next_events(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /next_events - ближайшие события"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
    
        # Проверяем, разрешен ли чат
        if chat.type != 'private' and not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
    
        # Получаем ближайшие события (только активные)
        from datetime import date
        today = date.today()
    
        cursor = await db_conn.conn.execute('''
            WITH today AS (SELECT DATE('now') as today_date)
            SELECT e.*,
                   CASE 
                       WHEN (e.month > strftime('%m', today.today_date)) OR 
                            (e.month = strftime('%m', today.today_date) AND e.day >= strftime('%d', today.today_date))
                       THEN julianday(date(strftime('%Y', today.today_date) || '-' || printf('%02d', e.month) || '-' || printf('%02d', e.day))) - julianday(today.today_date)
                       ELSE julianday(date((strftime('%Y', today.today_date) + 1) || '-' || printf('%02d', e.month) || '-' || printf('%02d', e.day))) - julianday(today.today_date)
                   END as days_until
            FROM events e, today
            WHERE e.chat_id = ? AND e.is_active = 1
            ORDER BY days_until
            LIMIT 5
        ''', (chat.id,))
    
        rows = await cursor.fetchall()
        events = [dict(row) for row in rows]
    
        if not events:
            await update.message.reply_text("📅 Ближайших событий нет.")
            return
    
        month_names = [
            'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
        ]
    
        message = "📅 **Ближайшие события:**\n\n"
    
        for event in events:
            date_str = f"{event['day']} {month_names[event['month']-1]}"
        
            days_until = int(event['days_until'])
        
            if days_until == 0:
                days_text = "🎉 **сегодня!**"
            elif days_until == 1:
                days_text = "завтра"
            else:
                days_text = f"через {days_until} дней"
        
            message += f"• **{event['name']}**\n"
            message += f"  📅 {date_str} ({days_text})\n"
        
            if event.get('year'):
                message += f"  📜 Историческая дата: {event['year']} г.\n"
        
            message += f"  ID: {event['id']}\n\n"
    
        await update.message.reply_text(message, parse_mode='Markdown')

    async def _handle_birthday_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик сообщения с днем рождения (регистронезависимый)"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        text = update.message.text
    
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
    
        # Приводим текст к нижнему регистру для проверки ключевых слов
        text_lower = text.lower()
    
        # Регистронезависимые ключевые слова
        keywords = ['мой др', 'мой день рождения', 'др']
        has_keyword = any(keyword in text_lower for keyword in keywords)
    
        if not has_keyword:
            return  # Игнорируем сообщения без ключевых слов
    
        # Парсим дату из ОРИГИНАЛЬНОГО текста
        parsed = DateParser.parse_birthday(text_lower)
    
        if not parsed:
            await update.message.reply_text(
                "❌ Не удалось распознать дату.\n\n"
                "Примеры форматов:\n"
                "• `28.06`\n"
                "• `28 июня`\n"
                "• `28.06.1998`\n"
                "• `28 июня 1998`"
            )
            return
    
        day, month, year = parsed
    
        # Проверяем существование даты
        from parsers import DateValidator
        if not DateValidator.is_valid_date(day, month, year):
            await update.message.reply_text("❌ Такой даты не существует.")
            return
    
        # Добавляем или обновляем день рождения
        success = await db_conn.add_birthday(
            user_id=user.id,
            chat_id=chat.id,
            day=day,
            month=month,
            year=year,
            username=user.username,
            full_name=user.full_name,
            created_by=user.id
        )
    
        if success:
            month_names_genitive = [
                'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
            ]
        
            date_str = f"{day} {month_names_genitive[month-1]}"
        
            if year:
                date_str += f" {year} года"
        
            # Проверяем 29 февраля
            if day == 29 and month == 2:
                await update.message.reply_text(
                    f"✅ День рождения добавлен: {date_str}\n\n"
                    "ℹ️ Вы указали 29 февраля. "
                    "В невисокосные годы поздравление будет отправляться 28 февраля."
                )
            else:
                await update.message.reply_text(f"✅ День рождения добавлен: {date_str}")
        else:
            await update.message.reply_text("❌ Ошибка при добавлении дня рождения.")
    
    # ========== АДМИНСКИЕ ОБРАБОТЧИКИ ==========
    
    async def _handle_add_birthday_admin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /add для админов - РАБОЧАЯ ВЕРСИЯ"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
    
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
    
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
    
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут добавлять дни рождения других участников.")
            return
    
        if len(context.args) < 2:
            await update.message.reply_text(
                "❌ Неверный формат команды.\n"
                "Используйте: `/add [пользователь] [дата]`\n\n"
                "Примеры:\n"
                "• `/add @username 28.06`\n"
                "• `/add 123456789 28 июня`\n"
                "• `/add Иван Иванов 28.06.1998`\n\n"
                "**Важно:** Для упоминания @username пользователь должен был писать в этот чат."
            )
            return
    
        # Парсим аргументы
        user_arg = context.args[0]
        date_arg = ' '.join(context.args[1:])
    
        # Определяем user_id по аргументу
        target_user_id = None
        target_username = None
        target_full_name = None
        found = False
    
        # ===== СПОСОБ 1: Упоминание через @username =====
        if user_arg.startswith('@'):
            username = user_arg[1:].lower()
        
            # Пробуем найти user_id из message.entities (текстовое упоминание)
            if message.entities:
                for entity in message.entities:
                    if entity.type == "mention":
                        # Проверяем, что это наш username
                        mention_text = message.text[entity.offset+1:entity.offset+entity.length].lower()
                        if mention_text == username:
                            # К сожалению, обычное mention не содержит user_id
                            # Нужен text_mention
                            pass
                    elif entity.type == "text_mention":
                        # УРА! text_mention содержит user_id!
                        mention_text = message.text[entity.offset:entity.offset+entity.length].lower()
                        if f"@{username}" in mention_text:
                            target_user_id = entity.user.id
                            target_username = entity.user.username or username
                            target_full_name = entity.user.full_name
                            found = True
                            logger.info(f"Найден пользователь через text_mention: {target_user_id}")
                            break
        
            # Если не нашли через entities, ищем другими способами
            if not found:
                # 1. Ищем среди администраторов
                for admin in admins:
                    if admin.user.username and admin.user.username.lower() == username:
                        target_user_id = admin.user.id
                        target_username = admin.user.username
                        target_full_name = admin.user.full_name
                        found = True
                        logger.info(f"Найден пользователь среди администраторов: {target_user_id}")
                        break
            
                # 2. Ищем в базе данных
                if not found:
                    cursor = await db_conn.conn.execute(
                        'SELECT user_id, username, full_name FROM birthdays WHERE chat_id = ? AND LOWER(username) = ?',
                        (chat.id, username)
                    )
                    result = await cursor.fetchone()
                
                    if result:
                        target_user_id = result['user_id']
                        target_username = result['username']
                        target_full_name = result['full_name']
                        found = True
                        logger.info(f"Найден пользователь в базе данных: {target_user_id}")
            
                # 3. Пытаемся получить через getChat (работает для некоторых пользователей)
                if not found:
                    try:
                        # Некоторые пользователи доступны через getChat даже если не админы
                        chat_member = await context.bot.get_chat_member(chat.id, username)
                        target_user_id = chat_member.user.id
                        target_username = chat_member.user.username
                        target_full_name = chat_member.user.full_name
                        found = True
                        logger.info(f"Найден пользователь через getChat_member: {target_user_id}")
                    except Exception as e:
                        logger.warning(f"Не удалось найти пользователя @{username} через getChat: {e}")
        
            if not found:
                # Последняя попытка: ищем среди упомянутых в сообщении пользователей
                await update.message.reply_text(
                    f"❌ Не удалось найти пользователя @{username}.\n\n"
                    f"**Причины и решения:**\n"
                    f"1. Пользователь должен был писать в этот чат\n"
                    f"2. Используйте **упоминание через reply** (ответьте на сообщение пользователя)\n"
                    f"3. Или используйте **ID пользователя**: `/add [ID] {date_arg}`\n\n"
                    f"**Как добавить через reply:**\n"
                    f"1. Ответьте на сообщение пользователя\n"
                    f"2. Напишите: `/add [дата]`\n\n"
                    f"**Как узнать ID:**\n"
                    f"• Через бота @userinfobot\n"
                    f"• ID пользователя: {self._find_user_id_in_message(message)}"
                )
                return
    
        # ===== СПОСОБ 2: ID пользователя =====
        elif user_arg.isdigit():
            target_user_id = int(user_arg)
            found = True
        
            # Пытаемся получить информацию о пользователе
            try:
                user_chat = await context.bot.get_chat(target_user_id)
                target_username = user_chat.username
                target_full_name = user_chat.full_name
                logger.info(f"Получена информация о пользователе по ID: {target_user_id}")
            except Exception as e:
                logger.warning(f"Не удалось получить информацию о пользователе {target_user_id}: {e}")
                target_username = None
                target_full_name = f"Пользователь {target_user_id}"
    
        # ===== СПОСОБ 3: Имя из базы данных =====
        else:
            # Ищем в базе данных по имени
            cursor = await db_conn.conn.execute(
                'SELECT user_id, username, full_name FROM birthdays WHERE chat_id = ? AND full_name LIKE ?',
                (chat.id, f'%{user_arg}%')
            )
            result = await cursor.fetchone()
        
            if result:
                target_user_id = result['user_id']
                target_username = result['username']
                target_full_name = result['full_name']
                found = True
            else:
                await update.message.reply_text(
                    f"❌ Пользователь '{user_arg}' не найден в базе.\n\n"
                    "Используйте:\n"
                    "1. @username с упоминанием\n"
                    "2. ID пользователя\n"
                    "3. Или попросите пользователя добавить себя: `мой др [дата]`"
                )
                return
    
        # ===== ПАРСИМ ДАТУ И ДОБАВЛЯЕМ =====
        parsed = DateParser.parse_birthday(f"др {date_arg}")
    
        if not parsed:
            await update.message.reply_text(
                "❌ Не удалось распознать дату.\n\n"
                "Примеры форматов:\n"
                "• `28.06`\n"
                "• `28 июня`\n"
                "• `28.06.1998`\n"
                "• `28 июня 1998`"
            )
            return
    
        day, month, year = parsed
    
        # Проверяем существование даты
        from parsers import DateValidator
        if not DateValidator.is_valid_date(day, month, year):
            await update.message.reply_text("❌ Такой даты не существует.")
            return
    
        # Добавляем день рождения
        success = await db_conn.add_birthday(
            user_id=target_user_id,
            chat_id=chat.id,
            day=day,
            month=month,
            year=year,
            username=target_username,
            full_name=target_full_name,
            created_by=user.id
        )
    
        if success:
            month_names = [
                'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
            ]
        
            date_str = f"{day} {month_names[month-1]}"
        
            if year:
                date_str += f" {year} года"
        
            username_display = f"@{target_username}" if target_username else target_full_name
        
            await update.message.reply_text(f"✅ День рождения для {username_display} добавлен: {date_str}")
        else:
            await update.message.reply_text("❌ Ошибка при добавлении дня рождения.")
    
    
    async def _handle_delete_birthday(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /delete для админов"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
        
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут удалять дни рождения.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите пользователя.\n"
                "Примеры:\n"
                "• `/delete @username`\n"
                "• `/delete 123456789`\n"
                "• `/delete Иван Иванов`"
            )
            return
        
        user_arg = ' '.join(context.args).lower()
        
        # Определяем user_id по аргументу
        target_user_id = None
        
        if user_arg.isdigit():
            target_user_id = int(user_arg)
        elif user_arg.startswith('@'):
            username = user_arg[1:].lower()

            # Поиск в базе
            cursor = await db_conn.conn.execute(
                'SELECT user_id FROM birthdays WHERE chat_id = ? AND LOWER(username) = ?',
                (chat.id, username)
            )
            result = await cursor.fetchone()
        
            if result:
                target_user_id = result['user_id']
            else:
                # Поиск в чате
                found = False
                try:
                    async for member in chat.get_members():
                        if member.user.username and member.user.username.lower() == username:
                            target_user_id = member.user.id
                            found = True
                            break
                except Exception as e:
                    logger.error(f"Ошибка поиска пользователя по username: {e}")
            
                if not found:
                    await update.message.reply_text(
                        f"❌ Пользователь @{username} не найден.\n\n"
                        "Убедитесь, что:\n"
                        "1. Пользователь есть в этом чате\n"
                        "2. У пользователя установлен username\n"
                        "3. Бот является администратором"
                    )
                    return
        else:
            # Поиск по имени
            cursor = await db_conn.conn.execute(
                'SELECT user_id FROM birthdays WHERE chat_id = ? AND full_name LIKE ?',
                (chat.id, f'%{user_arg}%')
            )
            result = await cursor.fetchone()
        
            if result:
                target_user_id = result['user_id']
            else:
                await update.message.reply_text(
                    f"❌ Пользователь '{user_arg}' не найден в базе.\n\n"
                    "Попробуйте:\n"
                    "1. Использовать username с @\n"
                    "2. Использовать ID пользователя\n"
                    "3. Проверить правильность написания имени"
                )
                return
           
         
        # Удаляем день рождения
        success = await db_conn.delete_birthday(target_user_id, chat.id)
        
        if success:
            await update.message.reply_text("✅ День рождения удален.")
        else:
            await update.message.reply_text("❌ Ошибка при удалении дня рождения.")
    
    async def _handle_force_congratulate(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /force_congratulate для админов"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        message = update.message
    
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
    
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
    
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут принудительно поздравлять.")
            return
    
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите пользователя.\n"
                "Примеры:\n"
                "• `/force_congratulate @username`\n"
                "• `/force_congratulate 123456789`\n"
                "• `/force_congratulate Иван Иванов`\n\n"
                "📝 **Примечание:** Можно поздравить любого пользователя, даже если у него нет дня рождения в базе."
            )
            return
    
        user_arg = ' '.join(context.args)
    
        # Переменные для информации о пользователе
        target_user_id = None
        target_username = None
        target_full_name = None
        has_birthday = False
        birthday_info = ""
    
        # ===== СПОСОБ 1: Поиск через упоминания в сообщении =====
        if message.entities:
            for entity in message.entities:
                if entity.type == "text_mention":
                    # Упоминание с user_id
                    mention_text = message.text[entity.offset:entity.offset+entity.length]
                    if user_arg in mention_text:
                        target_user_id = entity.user.id
                        target_username = entity.user.username
                        target_full_name = entity.user.full_name
                        break
                elif entity.type == "mention":
                    # Обычное @упоминание
                    mention_text = message.text[entity.offset+1:entity.offset+entity.length]
                    if mention_text.lower() == user_arg.lstrip('@').lower():
                        # Для обычного упоминания нет user_id, нужно искать другими способами
                        pass
    
        # ===== СПОСОБ 2: Поиск по ID =====
        if not target_user_id and user_arg.isdigit():
            target_user_id = int(user_arg)
        
            # Пытаемся получить информацию о пользователе
            try:
                user_chat = await context.bot.get_chat(target_user_id)
                target_username = user_chat.username
                target_full_name = user_chat.full_name
            except Exception as e:
                logger.warning(f"Не удалось получить информацию о пользователе {target_user_id}: {e}")
                target_full_name = f"Пользователь {target_user_id}"
    
        # ===== СПОСОБ 3: Поиск по username =====
        elif not target_user_id and user_arg.startswith('@'):
            username = user_arg[1:].lower()
        
            # Ищем в базе данных
            cursor = await db_conn.conn.execute(
                'SELECT user_id, username, full_name FROM birthdays WHERE chat_id = ? AND LOWER(username) = ?',
                (chat.id, username)
            )
            result = await cursor.fetchone()
        
            if result:
                target_user_id = result['user_id']
                target_username = result['username']
                target_full_name = result['full_name']
            else:
                # Ищем среди администраторов чата
                for admin in admins:
                    if admin.user.username and admin.user.username.lower() == username:
                        target_user_id = admin.user.id
                        target_username = admin.user.username
                        target_full_name = admin.user.full_name
                        break
    
        # ===== СПОСОБ 4: Поиск по имени =====
        elif not target_user_id:
            # Ищем в базе данных
            cursor = await db_conn.conn.execute(
                'SELECT user_id, username, full_name FROM birthdays WHERE chat_id = ? AND full_name LIKE ?',
                (chat.id, f'%{user_arg}%')
            )
            result = await cursor.fetchone()
        
            if result:
                target_user_id = result['user_id']
                target_username = result['username']
                target_full_name = result['full_name']
    
        # ===== Если пользователь не найден =====
        if not target_user_id:
            await update.message.reply_text(
                f"❌ Не удалось идентифицировать пользователя '{user_arg}'.\n\n"
                "Попробуйте:\n"
                "1. **Упоминание через reply** (ответьте на сообщение пользователя)\n"
                "2. **Точный username с @** (например @username)\n"
                "3. **ID пользователя** (узнать через @userinfobot)\n\n"
                "📝 Для reply-способа:\n"
                "1. Ответьте на сообщение пользователя\n"
                "2. Напишите `/force_congratulate`"
            )
            return
    
        # ===== Проверяем, есть ли день рождения =====
        birthday = await db_conn.get_birthday(target_user_id, chat.id)
    
        if birthday:
            has_birthday = True
            month_names = [
                'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
            ]
        
            date_str = f"{birthday['day']} {month_names[birthday['month']-1]}"
        
            if birthday['year']:
                date_str += f" {birthday['year']} года"
        
            birthday_info = f"\n🎂 День рождения: {date_str}"
    
        # ===== Получаем случайное поздравление =====
        congrats = await db_conn.get_random_congratulation()
    
        if not congrats:
            await update.message.reply_text("❌ Нет поздравлений в базе.")
            return
    
        # ===== Формируем сообщение =====
        username_display = f"@{target_username}" if target_username else target_full_name
    
        message_text = f"🎉 Принудительное поздравление для {username_display}!\n"
    
        if has_birthday:
            message_text += birthday_info + "\n"
        else:
            message_text += "📝 (День рождения не указан)\n"
    
        message_text += f"\n{congrats['text']}"
    
        # ===== Отправляем сообщение =====
        await update.message.reply_text(message_text)
    
        # ===== Отмечаем как отправленное (только если есть день рождения) =====
        if has_birthday:
            await db_conn.mark_birthday_sent(target_user_id, chat.id, congrats['id'])
    
        logger.info(f"Отправлено принудительное поздравление для user_id={target_user_id} (ДР: {has_birthday})")
    
    async def _handle_add_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /add_event для админов - ВСЕ события ежегодные"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
    
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
    
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
    
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут добавлять события.")
            return
    
        # Парсим команду
        text = update.message.text
        parsed = DateParser.parse_event_command(text)
    
        if not parsed:
            await update.message.reply_text(
                "❌ Неверный формат команды.\n\n"
                "Используйте:\n"
                "`/add_event 01.05 Название события`\n"
                "Текст поздравления на следующей строке\n\n"
                "Пример:\n"
                "`/add_event 01.05 День весны и труда`\n"
                "Поздравляем с 1 мая! Ура!"
            )
            return
    
        # Проверяем медиа
        media_type = None
        media_id = None
    
        if update.message.photo:
            media_type = 'photo'
            media_id = update.message.photo[-1].file_id
        elif update.message.video:
            media_type = 'video'
            media_id = update.message.video.file_id
        elif update.message.animation:  # GIF
            media_type = 'animation'
            media_id = update.message.animation.file_id
        elif update.message.document:
            media_type = 'document'
            media_id = update.message.document.file_id
        elif update.message.sticker:
            media_type = 'sticker'
            media_id = update.message.sticker.file_id
    
        try:
            # Добавляем событие (теперь все события ежегодные)
            event_id = await db_conn.add_event(
                chat_id=chat.id,
                name=parsed['event_name'],
                day=parsed['day'],
                month=parsed['month'],
                year=parsed['year'],  # Год только для справки
                message=parsed['message_text'],
                media_type=media_type,
                media_id=media_id,
                created_by=user.id
            )
        
            # Формируем ответ
            month_names = [
                'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
            ]
        
            date_str = f"{parsed['day']:02d}.{parsed['month']:02d}"
            if parsed['year']:
                date_str += f".{parsed['year']} (историческая дата)"
            
            response = (
                f"✅ Событие добавлено!\n\n"
                f"📅 {date_str}\n"
                f"🎉 {parsed['event_name']}\n"
                f"ID: {event_id}"
            )
        
            if media_type:
                response += f"\n📎 Медиа: {media_type}"
        
            await update.message.reply_text(response)
        
        except ValueError as e:
            await update.message.reply_text(f"❌ {str(e)}")
        except Exception as e:
            logger.error(f"Ошибка при добавлении события: {e}")
            await update.message.reply_text("❌ Произошла ошибка при добавлении события.")
    
    async def _handle_delete_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /delete_event для админов"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
        
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут удалять события.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите ID события.\n"
                "Используйте `/list_events` чтобы узнать ID."
            )
            return
        
        try:
            event_id = int(context.args[0])
            
            # Проверяем, существует ли событие в этом чате
            cursor = await db_conn.conn.execute(
                'SELECT id FROM events WHERE id = ? AND chat_id = ?',
                (event_id, chat.id)
            )
            result = await cursor.fetchone()
            
            if not result:
                await update.message.reply_text("❌ Событие с таким ID не найдено в этом чате.")
                return
            
            # Удаляем событие
            await db_conn.conn.execute('DELETE FROM events WHERE id = ?', (event_id,))
            await db_conn.conn.commit()
            
            await update.message.reply_text(f"✅ Событие {event_id} удалено.")
            
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом.")
        except Exception as e:
            logger.error(f"Ошибка при удалении события: {e}")
            await update.message.reply_text("❌ Ошибка при удалении события.")
    
    async def _handle_toggle_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /toggle_event для админов"""
        db_conn = context.bot_data['db']
        chat = update.effective_chat
        user = update.effective_user
        
        # Проверяем, разрешен ли чат
        if not await db_conn.is_chat_allowed(chat.id):
            return await self._handle_command_in_disallowed_chat(update, context)
        
        # Проверяем права админа
        admins = await chat.get_administrators()
        admin_ids = [admin.user.id for admin in admins]
        
        if user.id not in admin_ids and user.id not in Config.get_owners():
            await update.message.reply_text("❌ Только администраторы могут управлять событиями.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите ID события.\n"
                "Используйте `/list_events` чтобы узнать ID."
            )
            return
        
        try:
            event_id = int(context.args[0])
            
            # Получаем текущее состояние
            cursor = await db_conn.conn.execute(
                'SELECT id, is_active FROM events WHERE id = ? AND chat_id = ?',
                (event_id, chat.id)
            )
            result = await cursor.fetchone()
            
            if not result:
                await update.message.reply_text("❌ Событие с таким ID не найдено в этом чате.")
                return
            
            # Меняем состояние
            new_state = 0 if result['is_active'] else 1
            
            await db_conn.conn.execute(
                'UPDATE events SET is_active = ? WHERE id = ?',
                (new_state, event_id)
            )
            await db_conn.conn.commit()
            
            status = "активировано" if new_state else "деактивировано"
            await update.message.reply_text(f"✅ Событие {event_id} {status}.")
            
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID. ID должен быть числом.")
        except Exception as e:
            logger.error(f"Ошибка при переключении события: {e}")
            await update.message.reply_text("❌ Ошибка при переключении события.")
    
    # ========== ОБРАБОТЧИКИ ВЛАДЕЛЬЦА ==========
    
    async def _handle_add_chat_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /add_chat для владельца в ЛС"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        if update.effective_chat.type != 'private':
            await update.message.reply_text("❌ Эта команда доступна только в личных сообщениях.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите ID чата.\n"
                "Пример: `/add_chat -123456789`\n\n"
                "Чтобы получить ID чата:\n"
                "1. Добавьте бота в чат\n"
                "2. Попросите любого участника написать команду (например, `/start`)\n"
                "3. Бот покажет ID чата в сообщении об ошибке"
            )
            return
        
        try:
            chat_id = int(context.args[0])
            
            # Проверяем, что бот есть в этом чате
            try:
                chat = await context.bot.get_chat(chat_id)
                chat_title = chat.title or "Без названия"
                
                # Проверяем, что бот является администратором
                try:
                    bot_member = await chat.get_member(context.bot.id)
                    if bot_member.status not in ['administrator', 'creator']:
                        await update.message.reply_text(
                            f"⚠️ Внимание: бот не является администратором в чате.\n\n"
                            f"ID: `{chat_id}`\n"
                            f"Название: {chat_title}\n\n"
                            f"Для корректной работы бота необходимо предоставить ему права администратора.\n\n"
                            f"Продолжить добавление? (да/нет)"
                        )
                        
                        # Сохраняем информацию о чате для подтверждения
                        context.user_data['pending_chat_add'] = {
                            'chat_id': chat_id,
                            'chat_title': chat_title,
                            'needs_admin': True
                        }
                        return
                        
                except Exception as e:
                    logger.warning(f"Не удалось проверить права бота в чате {chat_id}: {e}")
                    needs_admin = True
                else:
                    needs_admin = False
                
                # Добавляем в белый список
                success = await db_conn.add_chat_to_whitelist(chat_id, chat_title, user.id)
                
                if success:
                    response = (
                        f"✅ Чат добавлен в белый список!\n\n"
                        f"ID: `{chat_id}`\n"
                        f"Название: {chat_title}\n"
                        f"Добавил: {user.full_name}\n"
                    )
                    
                    if needs_admin:
                        response += "\n⚠️ **Внимание:** Для корректной работы бота необходимо предоставить ему права администратора в этом чате."
                    
                    await update.message.reply_text(response, parse_mode='Markdown')
                    
                    # Отправляем приветственное сообщение в чат
                    try:
                        welcome_message = (
                            f"🎉 Бот активирован в этом чате!\n\n"
                            f"Теперь участники могут использовать команды:\n"
                            f"• `мой др [дата]` - добавить свой день рождения\n"
                            f"• `/birthlist` - список дней рождений\n"
                            f"• `/whoisnext` - ближайшие дни рождения\n"
                            f"• `/list_events` - список событий\n\n"
                            f"Администраторы чата могут:\n"
                            f"• Добавлять события командой `/add_event`\n"
                            f"• Управлять днями рождениями участников"
                        )
                        
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=welcome_message,
                            parse_mode='Markdown'
                        )
                    except Exception as e:
                        logger.error(f"Не удалось отправить приветственное сообщение в чат {chat_id}: {e}")
                        
                else:
                    await update.message.reply_text("❌ Не удалось добавить чат.")
                    
            except Exception as e:
                await update.message.reply_text(
                    f"❌ Не удалось получить информацию о чате.\n"
                    f"Убедитесь, что:\n"
                    f"1. Бот добавлен в этот чат\n"
                    f"2. Указан правильный ID чата\n"
                    f"3. Бот может видеть информацию о чате"
                )
                logger.error(f"Ошибка при получении чата {chat_id}: {e}")
                
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID чата. ID должен быть числом.")
    
    async def _handle_remove_chat_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /remove_chat для владельца в ЛС"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        if update.effective_chat.type != 'private':
            await update.message.reply_text("❌ Эта команда доступна только в личных сообщениях.")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ Укажите ID чата.\n"
                "Пример: `/remove_chat -123456789`"
            )
            return
        
        try:
            chat_id = int(context.args[0])
            
            # Получаем информацию о чате
            cursor = await db_conn.conn.execute(
                'SELECT chat_id, title FROM allowed_chats WHERE chat_id = ?',
                (chat_id,)
            )
            chat_info = await cursor.fetchone()
            
            if not chat_info:
                await update.message.reply_text(f"❌ Чат `{chat_id}` не найден в белом списке.", parse_mode='Markdown')
                return
            
            # Запрашиваем подтверждение
            await update.message.reply_text(
                f"⚠️ **Подтверждение удаления**\n\n"
                f"Вы действительно хотите удалить чат из белого списка?\n\n"
                f"ID: `{chat_id}`\n"
                f"Название: {chat_info['title']}\n\n"
                f"Это действие:\n"
                f"• Удалит все дни рождения из этого чата\n"
                f"• Удалит все события из этого чата\n"
                f"• Остановит работу бота в этом чате\n\n"
                f"Для подтверждения введите: `да, удалить {chat_id}`",
                parse_mode='Markdown'
            )
            
            # Сохраняем информацию для подтверждения
            context.user_data['pending_chat_remove'] = {
                'chat_id': chat_id,
                'chat_title': chat_info['title']
            }
                
        except ValueError:
            await update.message.reply_text("❌ Неверный формат ID чата. ID должен быть числом.")
    
    async def _handle_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка подтверждений от владельца"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            return
        
        text = update.message.text.strip().lower()
        
        # Проверяем подтверждение удаления чата
        if 'pending_chat_remove' in context.user_data:
            chat_info = context.user_data['pending_chat_remove']
            chat_id = chat_info['chat_id']
            
            if text == f"да, удалить {chat_id}" or text == f"да удалить {chat_id}":
                # Удаляем чат из белого списка
                success = await db_conn.remove_chat_from_whitelist(chat_id)
                
                if success:
                    await update.message.reply_text(
                        f"✅ Чат `{chat_id}` удален из белого списка.\n"
                        f"Все связанные данные были удалены.",
                        parse_mode='Markdown'
                    )
                    
                    # Пытаемся отправить сообщение в чат
                    try:
                        farewell_message = (
                            "❌ Бот деактивирован в этом чате владельцем.\n\n"
                            "Все данные о днях рождениях и событиях удалены.\n"
                            "Для повторной активации обратитесь к владельцу бота."
                        )
                        
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=farewell_message
                        )
                    except Exception as e:
                        logger.info(f"Не удалось отправить сообщение в деактивированный чат {chat_id}: {e}")
                else:
                    await update.message.reply_text("❌ Не удалось удалить чат.")
                
                # Очищаем данные о подтверждении
                del context.user_data['pending_chat_remove']
            
            elif text in ['нет', 'отмена', 'cancel']:
                await update.message.reply_text("❌ Удаление отменено.")
                del context.user_data['pending_chat_remove']

        pass
    
    async def _handle_list_chats_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /list_chats для владельца в ЛС"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        if update.effective_chat.type != 'private':
            await update.message.reply_text("❌ Эта команда доступна только в личных сообщениях.")
            return
        
        # Получаем список чатов
        chats = await db_conn.get_all_allowed_chats()
        
        if not chats:
            await update.message.reply_text("📋 Белый список пуст.")
            return
        
        # Получаем статистику для каждого чата
        stats_text = "📋 **Разрешенные чаты:**\n\n"
        
        total_birthdays = 0
        total_events = 0
        
        for chat in chats:
            # Количество дней рождений
            cursor = await db_conn.conn.execute(
                'SELECT COUNT(*) as count FROM birthdays WHERE chat_id = ?',
                (chat['chat_id'],)
            )
            birthdays_result = await cursor.fetchone()
            birthdays_count = birthdays_result['count'] if birthdays_result else 0
            
            # Количество событий
            cursor = await db_conn.conn.execute(
                'SELECT COUNT(*) as count FROM events WHERE chat_id = ? AND is_active = 1',
                (chat['chat_id'],)
            )
            events_result = await cursor.fetchone()
            events_count = events_result['count'] if events_result else 0
            
            added_date = datetime.fromisoformat(chat['added_at']).strftime('%d.%m.%Y')
            
            stats_text += f"**{chat['title']}**\n"
            stats_text += f"ID: `{chat['chat_id']}`\n"
            stats_text += f"Добавлен: {added_date}\n"
            stats_text += f"Дней рождений: {birthdays_count}\n"
            stats_text += f"Событий: {events_count}\n"
            stats_text += f"Добавил: ID {chat['added_by']}\n\n"
            
            total_birthdays += birthdays_count
            total_events += events_count
        
        stats_text += f"**Итого:**\n"
        stats_text += f"• Чатов: {len(chats)}\n"
        stats_text += f"• Всего дней рождений: {total_birthdays}\n"
        stats_text += f"• Всего событий: {total_events}\n\n"
        stats_text += "Для управления используйте:\n"
        stats_text += "• `/add_chat [ID]` - добавить чат\n"
        stats_text += "• `/remove_chat [ID]` - удалить чат"
        
        # Разбиваем на части если слишком длинное
        if len(stats_text) > 4000:
            parts = [stats_text[i:i+4000] for i in range(0, len(stats_text), 4000)]
            for part in parts:
                await update.message.reply_text(part, parse_mode='Markdown')
        else:
            await update.message.reply_text(stats_text, parse_mode='Markdown')
    
    async def _handle_stats_owner(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /stats для владельца в ЛС"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        if update.effective_chat.type != 'private':
            await update.message.reply_text("❌ Эта команда доступна только в личных сообщениях.")
            return
        
        try:
            # Статистика по базе данных
            stats = {}
            
            # Количество чатов
            cursor = await db_conn.conn.execute('SELECT COUNT(*) as count FROM allowed_chats')
            result = await cursor.fetchone()
            stats['chats'] = result['count'] if result else 0
            
            # Количество дней рождений
            cursor = await db_conn.conn.execute('SELECT COUNT(*) as count FROM birthdays')
            result = await cursor.fetchone()
            stats['birthdays'] = result['count'] if result else 0
            
            # Количество событий
            cursor = await db_conn.conn.execute('SELECT COUNT(*) as count FROM events')
            result = await cursor.fetchone()
            stats['events'] = result['count'] if result else 0
            
            # Количество поздравлений
            cursor = await db_conn.conn.execute('SELECT COUNT(*) as count FROM congratulations')
            result = await cursor.fetchone()
            stats['congratulations'] = result['count'] if result else 0
            
            # Самые популярные поздравления
            cursor = await db_conn.conn.execute('''
                SELECT text, used_count FROM congratulations 
                ORDER BY used_count DESC 
                LIMIT 3
            ''')
            top_congrats = await cursor.fetchall()
            
            # Формируем отчет
            report = "📊 **Статистика бота:**\n\n"
            report += f"• Чатов в белом списке: {stats['chats']}\n"
            report += f"• Записей о днях рождениях: {stats['birthdays']}\n"
            report += f"• Событий: {stats['events']}\n"
            report += f"• Поздравлений в базе: {stats['congratulations']}\n\n"
            
            if top_congrats:
                report += "**Самые популярные поздравления:**\n"
                for i, congrats in enumerate(top_congrats, 1):
                    text_short = congrats['text'][:50] + "..." if len(congrats['text']) > 50 else congrats['text']
                    report += f"{i}. Использовано {congrats['used_count']} раз\n   {text_short}\n"
            
            await update.message.reply_text(report, parse_mode='Markdown')
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            await update.message.reply_text("❌ Ошибка при получении статистики.")
    
    async def _handle_owner_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /owner_help для владельца в ЛС"""
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        if update.effective_chat.type != 'private':
            await update.message.reply_text("❌ Эта команда доступна только в личных сообщениях.")
            return
        
        help_text = """
👑 **Команды владельца бота** (только в ЛС)

**Управление белым списком:**
• `/add_chat [ID]` - добавить чат в белый список
• `/remove_chat [ID]` - удалить чат из белого списка
• `/list_chats` - список всех разрешенных чатов
• `/stats` - статистика бота

**Управление поздравлениями:**
• Пришлите файл `.txt` с поздравлениями (каждая строка - отдельное поздравление, максимум 50)

**Для администраторов чатов:**
• `/add [пользователь] [дата]` - добавить ДР
• `/delete [пользователь]` - удалить ДР
• `/force_congratulate [пользователь]` - принудительно поздравить
• `/add_event [дата] [название]` + текст - добавить событие
• `/delete_event [ID]` - удалить событие
• `/toggle_event [ID]` - включить/выключить событие

**Как получить ID чата:**
1. Добавьте бота в чат
2. Попросите любого участника написать команду (например, `/start`)
3. Бот покажет ID чата в сообщении об ошибке
4. Используйте этот ID с командой `/add_chat`
        """
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def _handle_upload_congrats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик загрузки файла с поздравлениями для владельца в ЛС"""
        db_conn = context.bot_data['db']
        user = update.effective_user
        
        if not Config.is_owner(user.id):
            await update.message.reply_text("❌ Недостаточно прав.")
            return
        
        document = update.message.document
        
        if not document.file_name.endswith('.txt'):
            await update.message.reply_text("❌ Файл должен быть в формате .txt")
            return
        
        try:
            # Скачиваем файл
            file = await context.bot.get_file(document.file_id)
            file_bytes = await file.download_as_bytearray()
            
            # Читаем строки
            text = file_bytes.decode('utf-8').strip()
            lines = text.split('\n')
            
            # Ограничение в Config.MAX_CONGratulations строк
            if len(lines) > Config.MAX_CONGratulations:
                lines = lines[:Config.MAX_CONGratulations]
                ignored = len(lines) - Config.MAX_CONGratulations
                warning = f"⚠️ Принято только первые {Config.MAX_CONGratulations} строк. Остальные {ignored} строк проигнорированы.\n\n"
            else:
                warning = ""
            
            # Добавляем поздравления в базу
            added_count = await db_conn.add_congratulations(lines, user.id)
            
            response = (
                f"{warning}"
                f"✅ Файл загружен успешно!\n"
                f"📊 Добавлено поздравлений: {added_count}\n"
                f"👤 Загрузил: {user.full_name}"
            )
            
            await update.message.reply_text(response)
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла с поздравлениями: {e}")
            await update.message.reply_text("❌ Ошибка при загрузке файла.")
    
    # ========== ОБРАБОТЧИКИ СОБЫТИЙ И ОШИБОК ==========
    
    async def _handle_user_left(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик выхода пользователя из чата"""
        db_conn = context.bot_data['db']
        try:
            if update.effective_chat and update.message.left_chat_member:
                user_id = update.message.left_chat_member.id
                chat_id = update.effective_chat.id
                
                # Проверяем, разрешен ли чат
                if not await db_conn.is_chat_allowed(chat_id):
                    return
                
                # Удаляем запись о дне рождения
                await db_conn.delete_birthday(user_id, chat_id)
                logger.info(f"Удалена запись для user_id={user_id} из chat_id={chat_id}")
                
        except Exception as e:
            logger.error(f"Ошибка при обработке выхода пользователя из чата: {e}")
    
    async def _handle_new_chat_members(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик добавления новых участников в чат"""
        db_conn = context.bot_data['db']
        try:
            if context.bot.id in [user.id for user in update.message.new_chat_members]:
                chat = update.effective_chat
                
                # Проверяем, разрешен ли чат
                if await db_conn.is_chat_allowed(chat.id):
                    return
                
                # Отправляем сообщение о необходимости активации
                welcome_message = (
                    f"👋 Привет! Я бот для дней рождения.\n\n"
                    f"Чтобы активировать меня в этом чате, администратору необходимо:\n"
                    f"1. Скопировать ID чата: `{chat.id}`\n"
                    f"2. Обратиться к владельцу бота (@yasmeev)\n"
                    f"3. Предоставить ID чата для активации\n\n"
                    f"После активации я смогу:\n"
                    f"• Поздравлять участников с днями рождения\n"
                    f"• Хранить памятные даты\n"
                    f"• Отправлять напоминания"
                )
                
                await update.message.reply_text(welcome_message, parse_mode='Markdown')
                
        except Exception as e:
            logger.error(f"Ошибка при обработке добавления бота в чат: {e}")
    
    async def _handle_command_in_disallowed_chat(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команд в неразрешенных чатах"""
        chat = update.effective_chat
        
        message = (
            "❌ **Бот не активирован в этом чате**\n\n"
            f"ID чата: `{chat.id}`\n"
            f"Название: {chat.title or 'Без названия'}\n\n"
            "Для активации бота администратору чата необходимо:\n"
            "1. Скопировать ID чата выше\n"
            "2. Обратиться к владельцу бота (@yasmeev)\n"
            "3. Предоставить владельцу ID чата для активации\n\n"
            "После активации бот сможет:\n"
            "• Поздравлять с днями рождения\n"
            "• Управлять памятными датами\n"
            "• Отправлять ежемесячные напоминания"
        )
        
        await update.message.reply_text(message, parse_mode='Markdown')
    
    async def _error_handler(self, update: Update, context: CallbackContext):
        """Обработчик ошибок"""
        logger.error(f"Ошибка: {context.error}", exc_info=context.error)
        
        # Отправляем сообщение владельцу об ошибке
        try:
            if Config.BOT_OWNER_ID:
                error_msg = f"❌ Ошибка в боте: {context.error}"
                await context.bot.send_message(
                    chat_id=Config.BOT_OWNER_ID,
                    text=error_msg[:4000]
                )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке владельцу: {e}")
    
    async def stop(self):
        """Остановка бота"""
        if self.scheduler:
            await self.scheduler.stop()
        
        if self.application and self.application.updater.running:
            await self.application.updater.stop()
        
        if self.application:
            await self.application.stop()
            await self.application.shutdown()
        
        logger.info("Бот остановлен")
