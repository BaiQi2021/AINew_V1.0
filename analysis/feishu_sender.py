import httpx
import json
import logging
import asyncio
import re

logger = logging.getLogger(__name__)

class FeishuSender:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    async def send_to_flow(self, title: str, total_titles: str, timestamp: str, report_type: str, text: str):
        """
        Send content to Feishu Flow Webhook to create a document.
        """
        if not self.webhook_url:
            logger.warning("Feishu Webhook URL is not set.")
            return

        payload = {
            "message_type": "text",
            "content": {
                "title": title,
                "total_titles": total_titles,
                "timestamp": timestamp,
                "report_type": report_type,
                "text": text
            }
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(self.webhook_url, json=payload)
                response.raise_for_status()
                result = response.json()
                if result.get("code") != 0:
                    logger.error(f"Feishu Flow send error: {result}")
                else:
                    logger.info(f"Feishu Flow message sent successfully.")
        except Exception as e:
            logger.error(f"Failed to send to Feishu Flow: {e}")

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
        
        # Convert markdown headers to Feishu supported format
        content = self._convert_markdown_to_feishu_format(content)
        
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

    def _convert_markdown_to_feishu_format(self, content: str) -> str:
        """
        Convert standard Markdown headers to Feishu-compatible format.
        Feishu interactive cards don't support # headers.
        """
        # Remove the first H1 header if it exists, as it's usually redundant with the card title
        # Also remove following newlines to avoid gap at the top
        content = re.sub(r'^#\s+(.+)(\n+)?', '', content, count=1, flags=re.MULTILINE)
        
        # Remove existing horizontal rules (---) to avoid duplication and rendering issues
        content = re.sub(r'^\s*---\s*$', '', content, flags=re.MULTILINE)
        
        # Helper to clean existing bold markers from title text to avoid ****Title****
        def clean_bold(text):
            return text.strip().strip('*')

        # Replace remaining H1: # Title -> **Title**
        content = re.sub(r'^#\s+(.+)$', lambda m: f"**{clean_bold(m.group(1))}**\n", content, flags=re.MULTILINE)
        
        # Replace H2: ## Title -> Separator + **Title**
        # Use a unicode line for consistent rendering instead of markdown ---
        def replace_h2(m):
            text = clean_bold(m.group(1))
            # Don't add separator for "本期速览"
            if "本期速览" in text:
                return f"**{text}**\n"
            return f"\n──────────────\n**{text}**\n"

        content = re.sub(r'^##\s+(.+)$', replace_h2, content, flags=re.MULTILINE)
        
        # Replace H3: ### Title -> \n🔹 **Title**
        # Add emoji and extra newlines for H3
        content = re.sub(r'^###\s+(.+)$', lambda m: f"\n\n🔹 **{clean_bold(m.group(1))}**\n", content, flags=re.MULTILINE)
        
        # Replace H4: #### Title -> **Title**
        content = re.sub(r'^####\s+(.+)$', lambda m: f"\n**{clean_bold(m.group(1))}**\n", content, flags=re.MULTILINE)
        
        # Clean up excessive newlines (more than 2) to avoid huge gaps
        content = re.sub(r'\n{3,}', '\n\n', content)
        
        return content.strip()

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
