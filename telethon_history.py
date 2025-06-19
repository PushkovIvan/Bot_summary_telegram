import asyncio
import json
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.types import Message, MessageService
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = 'config.yaml'
HISTORY_PATH = 'history.json'

async def main():
    # Загрузка конфига
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    telethon_cfg = config['telethon']
    group_cfg = config['groups'][0]
    chat_id = group_cfg['id']
    topics = group_cfg.get('topics', [])

    # Создаем словарь для быстрого доступа к топикам
    topics_dict = {topic['id']: topic for topic in topics}
    all_topics_ids = list(topics_dict.keys())

    api_id = int(telethon_cfg['api_id'])
    api_hash = telethon_cfg['api_hash']
    phone = telethon_cfg['phone']

    client = TelegramClient('history.session', api_id, api_hash)
    await client.start(phone=phone)
    if not await client.is_user_authorized():
        try:
            await client.send_code_request(phone)
            code = input('Enter the code you received: ')
            await client.sign_in(phone, code)
        except SessionPasswordNeededError:
            password = input('Two step verification enabled. Please enter your password: ')
            await client.sign_in(password=password)

    logger.info(f'Выгружаю все сообщения из чата {chat_id}...')
    all_data = {str(chat_id): {'0': []}}  # 0 - основной чат
    
    # Инициализируем структуру для всех топиков
    for topic_id in all_topics_ids:
        all_data[str(chat_id)][str(topic_id)] = []

    # Получаем все сообщения из чата
    total_messages = 0
    async for msg in client.iter_messages(chat_id, limit=None):
        if not isinstance(msg, Message) or isinstance(msg, MessageService):
            continue

        # Определяем topic_id для сообщения
        topic_id = 0
        if hasattr(msg, 'reply_to') and msg.reply_to:
            if hasattr(msg.reply_to, 'reply_to_msg_id'):
                # Это ответ в топике, нужно найти корневое сообщение
                try:
                    reply_msg = await client.get_messages(chat_id, ids=msg.reply_to.reply_to_msg_id)
                    if hasattr(reply_msg, 'reply_to') and reply_msg.reply_to:
                        if hasattr(reply_msg.reply_to, 'reply_to_top_id'):
                            topic_id = reply_msg.reply_to.reply_to_top_id
                except Exception as e:
                    logger.warning(f"Не удалось получить сообщение {msg.reply_to.reply_to_msg_id}: {e}")

        # Получаем информацию об отправителе
        user = await msg.get_sender()
        user_info = {
            'user_id': getattr(user, 'id', None),
            'username': getattr(user, 'username', None),
            'first_name': getattr(user, 'first_name', None)
        }

        # Формируем данные сообщения
        message_data = {
            'id': msg.id,
            'text': msg.text or '',
            **user_info,
            'timestamp': msg.date.isoformat() if msg.date else '',
            'chat_id': chat_id,
            'topic_id': topic_id,
            'topic_name': topics_dict.get(topic_id, {}).get('name', 'Основной чат')
        }

        # Распределяем сообщение по соответствующему топику
        if topic_id in all_topics_ids:
            all_data[str(chat_id)][str(topic_id)].append(message_data)
        else:
            all_data[str(chat_id)]['0'].append(message_data)

        total_messages += 1
        if total_messages % 100 == 0:
            logger.info(f'Обработано {total_messages} сообщений...')

    # Сохраняем результаты
    with open(HISTORY_PATH, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    # Выводим статистику
    logger.info(f"✅ Всего обработано сообщений: {total_messages}")
    for topic_id, messages in all_data[str(chat_id)].items():
        topic_name = 'Основной чат' if topic_id == '0' else topics_dict.get(int(topic_id), {}).get('name', 'Неизвестный топик')
        logger.info(f"  {topic_name}: {len(messages)} сообщений")

    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())