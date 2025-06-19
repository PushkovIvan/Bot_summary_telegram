import asyncio
import json
import logging
import os
import schedule
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from telegram import Update
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
        
        self.messages_storage: Dict[int, Dict[int, List[Dict]]] = {}
        self.groups_dict = {group["id"]: group for group in self.groups_config}
        self.giga_client = GigaChatClient()
        self.application = None

        # Инициализация хранилища из JSON при запуске
        self.load_history_from_file()

    def load_history_from_file(self, filename: str = 'history.json') -> int:
        """Загрузка всех сообщений из JSON файла"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                total = 0
                for chat_id_str, topics in data.items():
                    chat_id = int(chat_id_str)
                    self.messages_storage[chat_id] = {}
                    
                    for topic_id_str, messages in topics.items():
                        topic_id = int(topic_id_str)
                        self.messages_storage[chat_id][topic_id] = messages
                        total += len(messages)
                
                logger.info(f"Загружено {total} сообщений из {filename}")
                return total
            return 0
        except Exception as e:
            logger.error(f"Ошибка загрузки из {filename}: {e}")
            return 0

    def save_messages_to_json(self, filename: str = 'history.json') -> bool:
        """Сохранение всех сообщений в JSON файл"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.messages_storage, f, ensure_ascii=False, indent=2)
            logger.info(f"Сообщения сохранены в {filename}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения в {filename}: {e}")
            return False

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка входящих сообщений и сохранение в JSON"""
        if not update.message or not update.message.chat:
            return

        chat_id = update.message.chat.id
        if chat_id not in self.groups_dict:
            return

        # Определяем topic_id (0 - основной чат)
        topic_id = getattr(update.message, 'message_thread_id', 0)
        
        # Проверяем, что топик есть в конфиге (если это не основной чат)
        if topic_id != 0 and topic_id not in [t['id'] for t in self.groups_dict[chat_id].get('topics', [])]:
            return

        # Создаем запись сообщения
        message_data = {
            'id': update.message.message_id,
            'text': update.message.text or update.message.caption or "",
            'user_id': update.message.from_user.id if update.message.from_user else None,
            'username': update.message.from_user.username if update.message.from_user else None,
            'first_name': update.message.from_user.first_name if update.message.from_user else None,
            'timestamp': update.message.date.isoformat(),
            'chat_id': chat_id,
            'topic_id': topic_id,
            'topic_name': next(
                (t['name'] for t in self.groups_dict[chat_id].get('topics', []) if t['id'] == topic_id),
                'Основной чат'
            )
        }

        # Инициализируем хранилище при необходимости
        if chat_id not in self.messages_storage:
            self.messages_storage[chat_id] = {}
        if topic_id not in self.messages_storage[chat_id]:
            self.messages_storage[chat_id][topic_id] = []

        # Добавляем сообщение
        self.messages_storage[chat_id][topic_id].append(message_data)
        logger.info(f"Сообщение сохранено в {chat_id}/{topic_id}")

        # Сохраняем в JSON (можно оптимизировать для частого сохранения)
        self.save_messages_to_json()

    async def create_summary(self) -> Optional[str]:
        """Создание сводки на основе сообщений за последние 24 часа"""
        try:
            # Собираем сообщения за последние 24 часа
            time_threshold = datetime.now(timezone.utc) - timedelta(hours=24)
            analysis_messages = []
            
            for chat_id, topics in self.messages_storage.items():
                for topic_id, messages in topics.items():
                    for msg in messages:
                        try:
                            msg_time = datetime.fromisoformat(msg['timestamp'])
                            if msg_time.tzinfo is None:
                                msg_time = msg_time.replace(tzinfo=timezone.utc)
                            
                            if msg_time > time_threshold and msg['text'].strip():
                                analysis_messages.append({
                                    'text': msg['text'],
                                    'user': msg.get('username') or msg.get('first_name') or f"user_{msg['user_id']}",
                                    'time': msg['timestamp'],
                                    'topic': msg.get('topic_name', 'Основной чат')
                                })
                        except Exception as e:
                            logger.error(f"Ошибка обработки сообщения: {e}")

            if not analysis_messages:
                logger.warning("Нет сообщений для анализа")
                return None

            # Формируем промпт для GigaChat
            prompt = self._create_summary_prompt(analysis_messages)
            return await self.giga_client.get_summary(prompt)
            
        except Exception as e:
            logger.error(f"Ошибка создания сводки: {e}")
            return None

    def _create_summary_prompt(self, messages: List[Dict]) -> str:
        """Формирование промпта для сводки"""
        messages_text = "\n".join(
            f"[{msg['topic']}] {msg['user']}: {msg['text']}"
            for msg in messages[-self.max_messages:]  # Ограничиваем количество
        )

        return f"""
Создай аналитическую сводку на основе сообщений из Telegram за последние 24 часа.

**Требования:**
1. Выдели 3-5 ключевых тем
2. Отметь важные обсуждения и вопросы
3. Предложи рекомендации
4. Будь кратким и конкретным
5. Язык: {self.language}
6. Формат: Markdown

**Сообщения для анализа (последние {len(messages)}):**
{messages_text}

**Формат сводки:**
# 📊 Ежедневная сводка ({datetime.now().strftime('%d.%m.%Y')})

## 🔍 Основные темы
- ...

## 💬 Важные обсуждения
- ...

## 🚀 Рекомендации
- ...

## 📈 Статистика
- Сообщений: {len(messages)}
- Групп: {len(self.messages_storage)}
- Топиков: {sum(len(topics) for topics in self.messages_storage.values())}
"""

    async def send_daily_summary(self):
        """Отправка ежедневной сводки по расписанию"""
        summary = await self.create_summary()
        if not summary:
            logger.warning("Не удалось создать сводку")
            return

        for group_id in self.groups_dict:
            try:
                await self.application.bot.send_message(
                    chat_id=group_id,
                    text=summary,
                    parse_mode='Markdown'
                )
                logger.info(f"Сводка отправлена в группу {group_id}")
            except Exception as e:
                logger.error(f"Ошибка отправки в {group_id}: {e}")

    def setup_handlers(self):
        """Настройка обработчиков команд"""
        handlers = [
            CommandHandler("start", self._command_start),
            CommandHandler("summary", self._command_summary),
            CommandHandler("save", self._command_save),
            MessageHandler(filters.ALL, self.handle_message)
        ]
        for handler in handlers:
            self.application.add_handler(handler)

    async def _command_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        await update.message.reply_text(
            "🤖 Бот для ежедневных сводок активирован!\n"
            "Команды:\n"
            "/summary - создать сводку сейчас\n"
            "/save - сохранить историю сообщений"
        )

    async def _command_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /summary"""
        await update.message.reply_text("⌛ Создаю сводку...")
        summary = await self.create_summary()
        if summary:
            await update.message.reply_text(summary, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ Не удалось создать сводку")

    async def _command_save(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /save"""
        if self.save_messages_to_json():
            await update.message.reply_text("✅ История сообщений сохранена")
        else:
            await update.message.reply_text("❌ Ошибка при сохранении")

    def schedule_tasks(self):
        """Настройка расписания задач"""
        schedule.every().day.at(self.summary_time).do(
            lambda: asyncio.create_task(self.send_daily_summary())
        )
        logger.info(f"Ежедневная сводка запланирована на {self.summary_time}")

    async def run_scheduler(self):
        """Запуск фонового планировщика"""
        while True:
            schedule.run_pending()
            await asyncio.sleep(60)

    async def start(self):
        """Основной цикл работы бота"""
        self.application = Application.builder().token(self.bot_token).build()
        self.setup_handlers()
        self.schedule_tasks()

        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()

        # Запускаем планировщик в фоне
        asyncio.create_task(self.run_scheduler())

        logger.info("Бот запущен и работает")
        await asyncio.Event().wait()  # Бесконечное ожидание

async def main():
    bot = TelegramSummaryBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())