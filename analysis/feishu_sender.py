import httpx
import json
import logging
import asyncio

logger = logging.getLogger(__name__)

class FeishuSender:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    async def send_markdown(self, title: str, content: str):
        """
        Send markdown content to Feishu.
        Splits content if it exceeds limits.
        """
        if not self.webhook_url:
            logger.warning("Feishu Webhook URL is not set.")
            return

        # Feishu limit is roughly 30k chars for interactive cards, but let's be safe with 40000 chars chunks for text or simple cards
        # We use "interactive" card with markdown element.
        
        chunks = self._split_content(content, 40000)
        
        for i, chunk in enumerate(chunks):
            card = {
                "config": {
                    "wide_screen_mode": True
                },
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"{title} ({i+1}/{len(chunks)})" if len(chunks) > 1 else title
                    },
                    "template": "blue"
                },
                "elements": [
                    {
                        "tag": "markdown",
                        "content": chunk
                    }
                ]
            }
            
            payload = {
                "msg_type": "interactive",
                "card": card
            }
            
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.post(self.webhook_url, json=payload)
                    response.raise_for_status()
                    result = response.json()
                    if result.get("code") != 0:
                        logger.error(f"Feishu send error: {result}")
                    else:
                        logger.info(f"Feishu message part {i+1} sent successfully.")
            except Exception as e:
                logger.error(f"Failed to send to Feishu: {e}")
            
            # Avoid rate limits
            await asyncio.sleep(1)

    def _split_content(self, content: str, max_length: int) -> list[str]:
        """Split content into chunks respecting markdown boundaries if possible."""
        if len(content) <= max_length:
            return [content]
        
        chunks = []
        current_chunk = ""
        
        lines = content.split('\n')
        for line in lines:
            if len(current_chunk) + len(line) + 1 > max_length:
                chunks.append(current_chunk)
                current_chunk = line + "\n"
            else:
                current_chunk += line + "\n"
        
        if current_chunk:
            chunks.append(current_chunk)
            
        return chunks
