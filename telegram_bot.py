import asyncio
import json
import logging
import os
import schedule
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

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
        self.tasks_storage: List[Dict[str, Any]] = []
        self.groups_dict = {group["id"]: group for group in self.groups_config}
        self.giga_client = GigaChatClient()
        self.application = None

        # Инициализация хранилища из JSON при запуске
        self.load_history_from_file()

    def load_tasks_from_file(self, filename: str = 'tasks.json') -> bool:
        """Загрузка задач с сохранением существующих"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_tasks = json.load(f)
                    
                # Проверяем, что файл не пустой и содержит корректные данные
                if isinstance(existing_tasks, list) and len(existing_tasks) > 0:
                    # Объединяем с текущими задачами (без дубликатов)
                    existing_ids = {t['id'] for t in self.tasks_storage}
                    for task in existing_tasks:
                        if task.get('id') and task['id'] not in existing_ids:
                            self.tasks_storage.append(task)
                    
                    logger.info(f"Загружено {len(existing_tasks)} задач из файла (без дубликатов)")
                    return True
            
            # Если файла нет или он пустой, создаем новый
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.tasks_storage, f, ensure_ascii=False, indent=2)
                
            return True
            
        except Exception as e:
            logger.error(f"Ошибка загрузки задач: {e}")
            return False

    def save_tasks_to_json(self, filename: str = 'tasks.json') -> bool:
        """Добавляет новые задачи в файл без полной перезаписи"""
        try:
            # Загружаем текущие задачи из файла
            existing_tasks = []
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    existing_tasks = json.load(f)
            
            # Объединяем задачи (уникальные по ID)
            task_ids = {t['id'] for t in existing_tasks}
            updated_tasks = existing_tasks.copy()
            
            for task in self.tasks_storage:
                if task['id'] not in task_ids:
                    updated_tasks.append(task)
            
            # Сохраняем объединенный список
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(updated_tasks, f, ensure_ascii=False, indent=2)
                
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения задач: {e}")
            return False
        
    async def analyze_for_tasks(self, message_data: Dict[str, Any]) -> bool:
        """Анализ сообщения на наличие задач через GigaChat с обработкой ошибок"""
        try:
            if not message_data.get('text'):
                return False

            prompt = f"""Проанализируй текст сообщения на наличие задач/поручений. Ответь ТОЛЬКО в формате JSON:
            {{
                "is_task": bool,
                "task_text": str | null,
                "assignee": str | null,
                "deadline": str | null
            }}

            Данные сообщения:
            - Автор: {message_data['username']}
            - Текст: "{message_data['text']}"
            """

            response = await self.giga_client.get_summary(prompt)
            if not response:
                return False

            # Удаляем возможные некорректные символы перед парсингом
            response = response.strip()
            if not response.startswith('{') or not response.endswith('}'):
                logger.error(f"Некорректный формат ответа: {response}")
                return False

            try:
                task_data = json.loads(response)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON: {e}\nОтвет: {response}")
                return False

            if not isinstance(task_data, dict) or not task_data.get('is_task', False):
                return False

            # Создаем новую задачу
            task = {
                'id': f"task_{int(datetime.now().timestamp())}",
                'created_at': message_data['timestamp'],
                'author': message_data['username'] or 'Unknown',
                'text': task_data.get('task_text', 'Не указано'),
                'assignee': task_data.get('assignee'),
                'deadline': task_data.get('deadline'),
                'status': 'new',
                'source_msg_id': message_data['id'],
                'chat_id': message_data['chat_id'],
                'topic_id': message_data['topic_id']
            }

            # Валидация обязательных полей
            if not task['text'] or task['text'] == 'Не указано':
                return False

            self.tasks_storage.append(task)
            self.save_tasks_to_json()
            logger.info(f"Выявлена новая задача: {task}")
            return True

        except Exception as e:
            logger.error(f"Критическая ошибка анализа задачи: {e}", exc_info=True)
            return False

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
        
    async def check_task_completion(self, message_data: Dict[str, Any]) -> bool:
        """Проверяет, содержит ли сообщение явное подтверждение выполнения задачи"""
        try:
            if not self.tasks_storage or not message_data.get('text'):
                return False

            # Получаем активные незавершенные задачи
            active_tasks = [
                task for task in self.tasks_storage 
                if not task.get('is_complete', False)
            ]

            if not active_tasks:
                return False

            # Формируем строгий промпт для анализа
            tasks_list = "\n".join(
                f"{idx+1}. [ID: {task['id']}] {task['text']} (Исполнитель: {task.get('assignee', 'не назначен')})"
                for idx, task in enumerate(active_tasks)
            )
            
            prompt = f"""Анализируй сообщение на ЯВНОЕ подтверждение выполнения задачи. 
    Ответь ТОЛЬКО в JSON формате:
    {{
        "is_completion": bool,  // true ТОЛЬКО если есть явное подтверждение
        "completed_task_id": str | null,  // ID задачи
        "confidence": float  // Уверенность в выполнении (0.0-1.0)
    }}

    Правила определения выполнения:
    1. Должно быть прямое указание на выполнение ("сделал", "выполнил", "готово")
    2. Должен быть указан ID задачи или четкое описание
    3. Минимальная уверенность: 0.8

    Активные задачи:
    {tasks_list}

    Сообщение для анализа:
    "{message_data['text']}"
    Автор: {message_data['username']}
    """

            response = await self.giga_client.get_summary(prompt)
            if not response:
                return False

            # Очистка и парсинг ответа
            response = response.strip().replace('```json', '').replace('```', '').strip()
            
            try:
                result = json.loads(response)
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга ответа: {e}\nОтвет: {response}")
                return False

            # Строгая валидация результата
            if not isinstance(result, dict):
                return False

            if not result.get('is_completion', False):
                return False

            if result.get('confidence', 0) < 0.8:
                logger.info(f"Низкая уверенность в выполнении: {result['confidence']}")
                return False

            task_id = result.get('completed_task_id')
            if not task_id:
                return False

            # Находим и обновляем задачу
            for task in self.tasks_storage:
                if task['id'] == task_id:
                    task.update({
                        'is_complete': True,
                        'completed_at': message_data['timestamp'],
                        'completed_by': message_data['username'],
                        'completion_confidence': result['confidence'],
                        'status': 'completed'
                    })
                    self.save_tasks_to_json()
                    
                    logger.info(f"Задача {task_id} помечена выполненной (уверенность: {result['confidence']})")
                    return True

            return False

        except Exception as e:
            logger.error(f"Ошибка проверки выполнения: {str(e)}", exc_info=True)
            return False

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка входящих сообщений"""
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

        # Сохраняем сообщение в историю
        if chat_id not in self.messages_storage:
            self.messages_storage[chat_id] = {}
        if topic_id not in self.messages_storage[chat_id]:
            self.messages_storage[chat_id][topic_id] = []
        
        self.messages_storage[chat_id][topic_id].append(message_data)
        self.save_messages_to_json()

        # Анализируем на наличие задач
        await self.analyze_for_tasks(message_data)

        # Проверяем на выполнение существующих задач
        await self.check_task_completion(message_data)

    async def create_summary(self) -> Optional[str]:
        """Генерация детальной сводки через GigaChat"""
        try:
            # 1. Подготовка данных
            time_threshold = datetime.now(timezone.utc) - timedelta(hours=24)
            
            # Собираем задачи
            completed_tasks = [
                t for t in self.tasks_storage 
                if t.get('is_complete', False) and 
                datetime.fromisoformat(t['completed_at']).replace(tzinfo=timezone.utc) > time_threshold
            ]
            
            active_tasks = [
                t for t in self.tasks_storage 
                if not t.get('is_complete', False)
            ]
            
            # Собираем сообщения
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

            if not analysis_messages and not completed_tasks and not active_tasks:
                return None

            # 2. Формирование промпта
            prompt = self._create_summary_prompt(analysis_messages, completed_tasks, active_tasks)
            summary = await self.giga_client.get_summary(prompt)
            
            # 3. Постобработка результата
            if summary:
                # Удаляем возможные Markdown-теги если они есть
                for md_tag in ["**", "__", "```", "#"]:
                    summary = summary.replace(md_tag, "")
                
                # Добавляем смайлы к заголовкам
                summary = summary.replace("ВЫПОЛНЕННЫЕ ПОРУЧЕНИЯ", "✅ ВЫПОЛНЕННЫЕ ПОРУЧЕНИЯ")
                summary = summary.replace("ТЕКУЩИЕ ПОРУЧЕНИЯ", "🔴 ТЕКУЩИЕ ПОРУЧЕНИЯ")
                summary = summary.replace("ЗАКЛЮЧЕНИЕ", "📢 ЗАКЛЮЧЕНИЕ")
                
                return summary
            return None
            
        except Exception as e:
            logger.error(f"Ошибка создания сводки: {e}")
            return None

    def _create_summary_prompt(self, messages: List[Dict], completed_tasks: List[Dict], active_tasks: List[Dict]) -> str:
        """Формирование строгого промпта для GigaChat"""
        tasks_text = "=== ПОРУЧЕНИЯ ===\n"
        tasks_text += "Завершённые:\n" + "\n".join(
            f"- {t['text']} (исполнил: {t.get('completed_by', '?')}, {datetime.fromisoformat(t['completed_at']).strftime('%H:%M')})"
            for t in completed_tasks
        ) + "\n\nТекущие:\n" + "\n".join(
            f"- {t['text']} (ответственный: {t.get('assignee', 'не назначен')}, срок: {t.get('deadline', 'не указан')})"
            for t in active_tasks
        )
        
        messages_text = "=== ОБСУЖДЕНИЯ ===\n"
        topics = {}
        for msg in messages:
            topic = msg['topic']
            if topic not in topics:
                topics[topic] = []
            topics[topic].append(msg['text'][:100] + "...")
        
        for topic, msgs in topics.items():
            messages_text += f"\nТема: {topic} ({len(msgs)} сообщ.)\n"
            messages_text += "\n".join(f"- {m}" for m in msgs[:3]) + "\n"
        
        return f"""
    Сформируй официальную сводку за последние 24 часа на основе следующих данных:

    {tasks_text}

    {messages_text}

    Требования к сводке:
    1. Строгий официально-деловой стиль
    2. Без Markdown-разметки
    3. Используй смайлы только для визуального разделения блоков (не более 3-х)
    4. Структура:
    [Дата и период]
    [Статистика активности]
    [Выполненные поручения]
    [Текущие поручения]
    [Ключевые темы обсуждений]
    [Заключение и рекомендации]

    5. Язык: русский
    6. Объём: 15-25 предложений
    7. Важные детали:
    - Указывай конкретные сроки для задач
    - Цитируй ключевые фразы из обсуждений
    - Сохраняй нейтральный тон
    - Выделяй проблемные моменты

    Пример заголовков:
    "ОФИЦИАЛЬНАЯ СВОДКА 20.06.2025"
    "✅ ВЫПОЛНЕННЫЕ ПОРУЧЕНИЯ"
    "🔴 ТЕКУЩИЕ ЗАДАЧИ"
    "📌 ОСНОВНЫЕ ТЕМЫ"
    "📢 ВЫВОДЫ"

    Сгенерируй только текст сводки без пояснений. Будь краток и пиши по делу
    """
    
    async def cleanup_old_tasks(self):
        """Очистка старых задач (старше 24 часов)"""
        try:
            time_threshold = datetime.now(timezone.utc) - timedelta(hours=24)
            initial_count = len(self.tasks_storage)
            
            self.tasks_storage = [
                task for task in self.tasks_storage
                if datetime.fromisoformat(task['created_at']).replace(tzinfo=timezone.utc) > time_threshold
            ]
            
            removed_count = initial_count - len(self.tasks_storage)
            if removed_count > 0:
                self.save_tasks_to_json()
                logger.info(f"Удалено {removed_count} старых задач")
            return True
        except Exception as e:
            logger.error(f"Ошибка очистки задач: {e}")
            return False

    async def send_daily_summary(self):
        """Отправка ежедневной сводки только в будние дни"""
        today = datetime.now().weekday()
        if today >= 5:  # 5 и 6 - суббота и воскресенье
            logger.info("Сегодня выходной, сводка не отправляется")
            return

        # Очищаем старые задачи перед формированием сводки
        await self.cleanup_old_tasks()
        
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