#!/usr/bin/env python3
"""
Скрипт для запуска Telegram Summary Bot
"""

import sys
import os
import asyncio
import logging
from pathlib import Path

# Добавляем текущую директорию в путь для импортов
sys.path.append(str(Path(__file__).parent))

try:
    from telegram_bot import main
    from config import CONFIG
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    print("Убедитесь, что все зависимости установлены: pip install -r requirements.txt")
    sys.exit(1)

def check_config():
    """Проверка конфигурации"""
    try:
        # Проверяем наличие токена Telegram
        if not CONFIG.get("token", {}).get("telegram") or CONFIG["token"]["telegram"] == "YOUR_TELEGRAM_BOT_TOKEN_HERE":
            print("❌ Ошибка: Не настроен токен Telegram бота")
            print("1. Получите токен у @BotFather")
            print("2. Добавьте его в config.yaml")
            return False
        
        # Проверяем наличие групп
        groups = CONFIG.get("groups", [])
        if not groups or groups == [-1234567890123, -9876543210987]:
            print("⚠️  Предупреждение: Не настроены группы для мониторинга")
            print("Добавьте ID групп в config.yaml")
            print("Для получения ID группы используйте @userinfobot")
        
        # Проверяем настройки бота
        bot_config = CONFIG.get("bot", {})
        if not bot_config:
            print("❌ Ошибка: Не найдены настройки бота в конфиге")
            return False
        
        print("✅ Конфигурация проверена")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка при проверке конфигурации: {e}")
        return False

def main_wrapper():
    """Обертка для запуска с обработкой ошибок"""
    print("🤖 Запуск Telegram Summary Bot...")
    print("=" * 50)
    
    # Проверяем конфигурацию
    if not check_config():
        sys.exit(1)
    
    # Настраиваем логирование
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('bot.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    print("🚀 Запуск бота...")
    print("Для остановки нажмите Ctrl+C")
    print("=" * 50)
    
    try:
        # Запускаем бота
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Получен сигнал остановки")
        print("Бот остановлен")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        logging.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main_wrapper() 