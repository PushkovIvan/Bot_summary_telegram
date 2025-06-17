import asyncio
import logging
import schedule
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Union
import json

from telegram import Update, Bot
from telegram.ext import Application, ContextTypes, CommandHandler, MessageHandler, filters
from telegram.error import TelegramError

from config import CONFIG
from gigachat_client import GigaChatClient

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class TelegramSummaryBot:
    def __init__(self):
        self.config = CONFIG
        self.bot_token = self.config["token"]["telegram"]
        self.groups_config = self.config.get("groups", [])
        self.max_messages = self.config["bot"]["max_messages_per_group"]
        self.summary_time = self.config["bot"]["summary_time"]
        self.language = self.config["bot"]["summary_language"]
        
        # Инициализируем хранилище сообщений: {group_id: {topic_id: [messages]}}
        self.messages_storage: Dict[int, Dict[int, List[Dict]]] = {}
        
        # Создаем словарь групп для быстрого доступа
        self.groups_dict = {}
        for group in self.groups_config:
            group_id = group["id"]
            self.groups_dict[group_id] = group
            
            # Инициализируем хранилище для группы
            if group_id not in self.messages_storage:
                self.messages_storage[group_id] = {}
            
            # Инициализируем хранилище для каждого топика
            for topic in group.get("topics", []):
                topic_id = topic["id"]
                if topic_id not in self.messages_storage[group_id]:
                    self.messages_storage[group_id][topic_id] = []
            
            # Для обычных групп (без топиков) используем ключ 0
            if not group.get("topics"):
                if 0 not in self.messages_storage[group_id]:
                    self.messages_storage[group_id][0] = []
        
        # Инициализация GigaChat клиента
        self.giga_client = GigaChatClient()
        
        # Application будет инициализирован в методе start
        self.application = None
    
    def setup_handlers(self):
        """Настройка обработчиков команд и сообщений"""
        if not self.application:
            return
            
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("summary", self.manual_summary))
        self.application.add_handler(CommandHandler("status", self.status_command))
        self.application.add_handler(CommandHandler("topics", self.topics_command))
        
        # Обработчик всех сообщений в группах
        self.application.add_handler(MessageHandler(filters.ALL, self.handle_message))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        await update.message.reply_text(
            "🤖 Бот для создания сводок активирован!\n\n"
            "Доступные команды:\n"
            "/help - показать справку\n"
            "/summary - создать сводку сейчас\n"
            "/status - показать статус бота\n"
            "/topics - показать топики"
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = """
📋 **Справка по боту**

**Основные функции:**
• Автоматический сбор сообщений из групп и топиков
• Создание ежедневных сводок в 9:00
• Анализ сообщений с помощью GigaChat
• Поддержка мультигрупп с топиками

**Команды:**
/start - запуск бота
/help - эта справка
/summary - создать сводку сейчас
/status - статус бота
/topics - показать топики

**Настройка:**
1. Добавьте бота в группы
2. Укажите ID групп и топиков в конфиге
3. Бот автоматически начнет сбор сообщений

**Получение ID группы:**
1. Добавьте @userinfobot в группу
2. Отправьте любое сообщение
3. Бот покажет ID группы (начинается с минуса)

**Получение ID топика:**
1. Отправьте сообщение в топик
2. Используйте @RawDataBot для получения message_thread_id
        """
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def topics_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /topics - показать топики"""
        topics_text = "📋 **Настроенные группы и топики:**\n\n"
        
        for group in self.groups_config:
            group_id = group["id"]
            group_name = group.get("name", f"Группа {group_id}")
            topics = group.get("topics", [])
            
            topics_text += f"**{group_name}** (ID: {group_id})\n"
            
            if topics:
                for topic in topics:
                    topic_id = topic["id"]
                    topic_name = topic["name"]
                    message_count = len(self.messages_storage.get(group_id, {}).get(topic_id, []))
                    topics_text += f"  • {topic_name} (ID: {topic_id}) - {message_count} сообщений\n"
            else:
                message_count = len(self.messages_storage.get(group_id, {}).get(0, []))
                topics_text += f"  • Обычная группа - {message_count} сообщений\n"
            
            topics_text += "\n"
        
        await update.message.reply_text(topics_text, parse_mode='Markdown')
    
    async def status_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /status"""
        total_messages = 0
        groups_count = 0
        topics_count = 0
        
        for group_id, topics_data in self.messages_storage.items():
            groups_count += 1
            for topic_id, messages in topics_data.items():
                topics_count += 1
                total_messages += len(messages)
        
        status_text = f"""
📊 **Статус бота**

**Мониторинг групп:** {groups_count}
**Мониторинг топиков:** {topics_count}
**Всего сообщений:** {total_messages}
**Время сводки:** {self.summary_time}
**Язык:** {self.language}

**Групп в конфиге:** {len(self.groups_config)}
        """
        await update.message.reply_text(status_text, parse_mode='Markdown')
    
    async def manual_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /summary - создание сводки вручную"""
        await update.message.reply_text("📝 Создаю сводку...")
        
        try:
            summary = await self.create_summary()
            if summary:
                await update.message.reply_text(summary, parse_mode='Markdown')
            else:
                await update.message.reply_text("❌ Не удалось создать сводку или нет данных для анализа")
        except Exception as e:
            logger.error(f"Ошибка при создании сводки: {e}")
            await update.message.reply_text(f"❌ Ошибка при создании сводки: {str(e)}")
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик всех сообщений в группах"""
        if not update.message or not update.message.chat:
            return
        
        chat_id = update.message.chat.id
        message_thread_id = update.message.message_thread_id if hasattr(update.message, 'message_thread_id') else None
        
        # Отладочная информация
        logger.info(f"📨 Получено сообщение: чат {chat_id}, топик {message_thread_id}")
        
        # Проверяем, что это группа из нашего списка
        if chat_id not in self.groups_dict:
            logger.info(f"❌ Группа {chat_id} не настроена в конфиге")
            return
        
        logger.info(f"✅ Группа {chat_id} найдена в конфиге")
        
        group_config = self.groups_dict[chat_id]
        topics = group_config.get("topics", [])
        
        # Определяем topic_id
        topic_id = 0  # По умолчанию для обычных групп
        topic_name = "Основной чат"
        
        if topics and message_thread_id:
            # Ищем топик по message_thread_id
            for topic in topics:
                if topic["id"] == message_thread_id:
                    topic_id = message_thread_id
                    topic_name = topic["name"]
                    logger.info(f"✅ Найден топик: {topic_name} (ID: {topic_id})")
                    break
            else:
                # Топик не найден в конфиге, пропускаем
                logger.info(f"⚠️  Топик {message_thread_id} не найден в конфиге группы {chat_id}")
                return
        elif topics and not message_thread_id:
            # В мультигруппе сообщение без топика - пропускаем
            logger.info(f"⚠️  Сообщение без топика в мультигруппе {chat_id}")
            return
        else:
            logger.info(f"✅ Обычная группа, используем topic_id = 0")
        
        # Сохраняем сообщение
        message_data = {
            'id': update.message.message_id,
            'text': update.message.text or update.message.caption or "",
            'user_id': update.message.from_user.id if update.message.from_user else None,
            'username': update.message.from_user.username if update.message.from_user else None,
            'first_name': update.message.from_user.first_name if update.message.from_user else None,
            'timestamp': update.message.date.isoformat(),
            'chat_id': chat_id,
            'topic_id': topic_id,
            'topic_name': topic_name
        }
        
        # Инициализируем хранилище, если нужно
        if chat_id not in self.messages_storage:
            self.messages_storage[chat_id] = {}
        if topic_id not in self.messages_storage[chat_id]:
            self.messages_storage[chat_id][topic_id] = []
        
        # Добавляем сообщение
        self.messages_storage[chat_id][topic_id].append(message_data)
        
        # Ограничиваем количество сообщений
        if len(self.messages_storage[chat_id][topic_id]) > self.max_messages:
            self.messages_storage[chat_id][topic_id] = self.messages_storage[chat_id][topic_id][-self.max_messages:]
        
        logger.info(f"💾 Сообщение сохранено в группе {chat_id}, топик {topic_name}. Всего сообщений: {len(self.messages_storage[chat_id][topic_id])}")
        logger.info(f"📝 Текст сообщения: {message_data['text'][:100]}...")
    
    async def create_summary(self) -> Optional[str]:
        """Создание сводки на основе собранных сообщений"""
        logger.info("🔍 Начинаю создание сводки...")
        logger.info(f"📊 Хранилище сообщений: {self.messages_storage}")
        
        if not self.messages_storage:
            logger.warning("❌ Хранилище сообщений пустое")
            return None
        
        # Подготавливаем данные для анализа
        all_messages = []
        for chat_id, topics_data in self.messages_storage.items():
            group_config = self.groups_dict.get(chat_id, {})
            group_name = group_config.get("name", f"Группа {chat_id}")
            
            logger.info(f"📁 Обрабатываю группу: {group_name} (ID: {chat_id})")
            
            for topic_id, messages in topics_data.items():
                topic_name = "Основной чат"
                if topic_id != 0:
                    # Ищем название топика
                    for topic in group_config.get("topics", []):
                        if topic["id"] == topic_id:
                            topic_name = topic["name"]
                            break
                
                logger.info(f"  📝 Топик {topic_name}: {len(messages)} сообщений")
                
                for msg in messages:
                    if msg['text'].strip():  # Только текстовые сообщения
                        all_messages.append({
                            'group': group_name,
                            'topic': topic_name,
                            'text': msg['text'],
                            'user': msg['username'] or msg['first_name'] or f"User{msg['user_id']}",
                            'time': msg['timestamp']
                        })
        
        logger.info(f"📊 Всего собрано сообщений для анализа: {len(all_messages)}")
        
        if not all_messages:
            logger.warning("❌ Нет текстовых сообщений для анализа")
            return None
        
        # Создаем промпт для GigaChat
        prompt = self.create_summary_prompt(all_messages)
        
        try:
            # Получаем сводку от GigaChat
            logger.info("🤖 Отправляю запрос к GigaChat...")
            summary = await self.giga_client.get_summary(prompt)
            
            if summary:
                logger.info("✅ Сводка успешно создана")
                return summary
            else:
                logger.error("❌ GigaChat вернул пустой ответ")
                return None
                
        except Exception as e:
            logger.error(f"❌ Ошибка при получении сводки от GigaChat: {e}")
            return None
    
    def create_summary_prompt(self, messages: List[Dict]) -> str:
        """Создание промпта для GigaChat"""
        # Берем последние сообщения за последние 24 часа (все в UTC)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        recent_messages = []
        for msg in messages:
            msg_time = datetime.fromisoformat(msg['time'])
            if msg_time.tzinfo is None:
                # Если вдруг время без таймзоны, считаем его UTC
                msg_time = msg_time.replace(tzinfo=timezone.utc)
            if msg_time > yesterday:
                recent_messages.append(msg)
        
        if not recent_messages:
            recent_messages = messages[-50:]  # Если нет сообщений за 24 часа, берем последние 50
        
        # Формируем текст для анализа
        messages_text = ""
        for msg in recent_messages:
            messages_text += f"[{msg['group']} - {msg['topic']}] {msg['user']}: {msg['text']}\n"
        
        # Группируем по группам и топикам для статистики
        groups_topics = {}
        for msg in recent_messages:
            group = msg['group']
            topic = msg['topic']
            if group not in groups_topics:
                groups_topics[group] = {}
            if topic not in groups_topics[group]:
                groups_topics[group][topic] = 0
            groups_topics[group][topic] += 1
        
        stats_text = ""
        for group, topics in groups_topics.items():
            stats_text += f"- {group}:\n"
            for topic, count in topics.items():
                stats_text += f"  • {topic}: {count} сообщений\n"
        
        prompt = f"""
Создай краткую утреннюю сводку на основе сообщений из Telegram групп и топиков.

**Сообщения для анализа:**
{messages_text}

**Статистика по группам и топикам:**
{stats_text}

**Требования к сводке:**
1. Структурированный обзор основных тем и событий
2. Выделение ключевых моментов и важной информации
3. Краткость и информативность
4. Язык: {self.language}
5. Формат: Markdown
6. Учитывай разделение по топикам

**Создай сводку в следующем формате:**
# 📊 Утренняя сводка

## 🎯 Основные темы
[список основных тем по группам и топикам]

## 📝 Ключевые события
[важные события]

## 💡 Интересные моменты
[заслуживающие внимания детали]

## 📈 Статистика
- Количество сообщений: {len(recent_messages)}
- Количество групп: {len(groups_topics)}
- Количество топиков: {sum(len(topics) for topics in groups_topics.values())}
- Период: последние 24 часа
        """
        
        return prompt
    
    async def send_daily_summary(self):
        """Отправка ежедневной сводки"""
        try:
            summary = await self.create_summary()
            if summary:
                # Отправляем сводку во все группы
                for group in self.groups_config:
                    chat_id = group["id"]
                    try:
                        await self.application.bot.send_message(
                            chat_id=chat_id,
                            text=summary,
                            parse_mode='Markdown'
                        )
                        logger.info(f"Сводка отправлена в группу {chat_id}")
                    except TelegramError as e:
                        logger.error(f"Ошибка отправки в группу {chat_id}: {e}")
                
                # Очищаем старые сообщения
                self.clear_old_messages()
            else:
                logger.info("Нет данных для создания сводки")
        except Exception as e:
            logger.error(f"Ошибка при отправке ежедневной сводки: {e}")
    
    def clear_old_messages(self):
        """Очистка старых сообщений (старше 7 дней)"""
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        
        for chat_id in list(self.messages_storage.keys()):
            for topic_id in list(self.messages_storage[chat_id].keys()):
                self.messages_storage[chat_id][topic_id] = [
                    msg for msg in self.messages_storage[chat_id][topic_id]
                    if datetime.fromisoformat(msg['timestamp']).replace(tzinfo=timezone.utc) > week_ago
                ]
                
                # Если в топике не осталось сообщений, удаляем его
                if not self.messages_storage[chat_id][topic_id]:
                    del self.messages_storage[chat_id][topic_id]
            
            # Если в группе не осталось топиков, удаляем её из хранилища
            if not self.messages_storage[chat_id]:
                del self.messages_storage[chat_id]
    
    def schedule_daily_summary(self):
        """Планирование ежедневной сводки"""
        schedule.every().day.at(self.summary_time).do(
            lambda: asyncio.create_task(self.send_daily_summary())
        )
        logger.info(f"Ежедневная сводка запланирована на {self.summary_time}")
    
    async def run_scheduler(self):
        """Запуск планировщика"""
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)  # Проверяем каждую минуту
    
    async def start(self):
        """Запуск бота"""
        logger.info("Запуск Telegram Summary Bot...")
        
        # Инициализируем Application
        self.application = Application.builder().token(self.bot_token).build()
        self.setup_handlers()
        
        # Планируем ежедневную сводку
        self.schedule_daily_summary()
        
        # Запускаем планировщик в отдельной задаче
        asyncio.create_task(self.run_scheduler())
        
        # Запускаем бота
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        logger.info("Бот запущен и готов к работе!")
        
        # Держим бота запущенным
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки...")
        finally:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()

async def main():
    """Главная функция"""
    bot = TelegramSummaryBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main()) 