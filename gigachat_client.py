import asyncio
import logging
from typing import Optional
from gigachat import GigaChat

from config import CONFIG

logger = logging.getLogger(__name__)

class GigaChatClient:
    def __init__(self):
        self.config = CONFIG
        self.giga = GigaChat(
            scope='GIGACHAT_API_CORP',
            credentials=self.config["token"]["gigachat"],
            verify_ssl_certs=False,
            model="GigaChat-2-Max"
        )
    
    async def get_summary(self, prompt: str) -> Optional[str]:
        """
        Получение сводки от GigaChat
        
        Args:
            prompt: Промпт для создания сводки
            
        Returns:
            Строка со сводкой или None в случае ошибки
        """
        try:
            # Создаем задачу для асинхронного выполнения
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                self._make_request, 
                prompt
            )
            
            if response and hasattr(response, 'choices') and response.choices:
                return response.choices[0].message.content
            else:
                logger.error("Пустой ответ от GigaChat")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка при обращении к GigaChat: {e}")
            return None
    
    def _make_request(self, prompt: str):
        """
        Синхронный запрос к GigaChat
        
        Args:
            prompt: Промпт для создания сводки
            
        Returns:
            Ответ от GigaChat
        """
        try:
            # Используем метод chat с простой строкой
            response = self.giga.chat(prompt)
            return response
        except Exception as e:
            logger.error(f"Ошибка в синхронном запросе к GigaChat: {e}")
            raise 