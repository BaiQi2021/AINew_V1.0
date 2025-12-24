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

    def _limit_reference_links(self, content: str, limit: int = 5) -> str:
        """Limit the number of links in each sub-category of '拓展阅读'."""
        if "## 拓展阅读" not in content:
            return content
            
        # Split content into main part and reference part
        parts = re.split(r'(^##\s+拓展阅读.*$)', content, flags=re.MULTILINE)
        if len(parts) < 3:
            return content
            
        main_content = "".join(parts[:-2])
        ref_header = parts[-2]
        ref_body = parts[-1]
        
        # Split ref_body into sub-sections by ###
        sub_parts = re.split(r'(^###\s+.+$)', ref_body, flags=re.MULTILINE)
        
        processed_ref_body = sub_parts[0] # Text before first ###
        
        for i in range(1, len(sub_parts), 2):
            header = sub_parts[i]
            body = sub_parts[i+1] if i+1 < len(sub_parts) else ""
            
            # Extract links (lines starting with *)
            lines = body.split('\n')
            links = []
            non_links = []
            for line in lines:
                stripped = line.strip()
                if stripped.startswith('*'):
                    links.append(line)
                elif stripped:
                    non_links.append(line)
            
            # Limit links
            limited_links = links[:limit]
            
            processed_ref_body += header + "\n" + "\n".join(limited_links) + "\n"
            if non_links:
                processed_ref_body += "\n".join(non_links) + "\n"
            processed_ref_body += "\n"
            
        return main_content + ref_header + processed_ref_body

    def _convert_markdown_to_feishu_format(self, content: str) -> str:
        """
        Convert standard Markdown headers to Feishu-compatible format.
        Feishu interactive cards don't support # headers.
        """
        # Limit the number of links in each sub-category of '拓展阅读' to 5
        content = self._limit_reference_links(content, limit=5)

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
        
        # Replace H3: ### Title -> \n❇️ **Title**
        # Add emoji and extra newlines for H3
        content = re.sub(r'^###\s+(.+)$', lambda m: f"\n\n❇️ **{clean_bold(m.group(1))}**\n", content, flags=re.MULTILINE)
        
        # Replace H4: #### Title -> **Title**
        content = re.sub(r'^####\s+(.+)$', lambda m: f"\n**{clean_bold(m.group(1))}**\n", content, flags=re.MULTILINE)
        
        # Replace blockquotes (>) with a vertical bar character (▎) that renders more consistently in Feishu cards
        # and ensure it has a blank line before it
        content = re.sub(r'([^\n])\n>', r'\1\n\n>', content)
        
        # Ensure "概要" is bolded even if LLM forgot, and use Chinese colon
        content = re.sub(r'^>\s*(?:\*\*)?概要(?:\*\*)?[:：]\s*', '> **概要**：', content, flags=re.MULTILINE)
        
        content = re.sub(r'^>\s*', '▎ ', content, flags=re.MULTILINE)

        # Remove [阅读原文] lines with forbidden domains (safety net if LLM fails)
        forbidden_patterns = [r'qq\.com', r'qbitai\.com', r'36kr\.com', r'mp\.weixin\.qq\.com', r'量子位']
        for pattern in forbidden_patterns:
            content = re.sub(rf'^\[阅读原文\]\(.*?{pattern}.*?\).*\n?', '', content, flags=re.MULTILINE | re.IGNORECASE)

        # Remove date after [阅读原文](URL)
        # Support both `YYYY-MM-DD` and `[YYYY-MM-DD]` formats
        content = re.sub(r'(\[阅读原文\]\(.*?\))\s*`\[?\d{4}-\d{2}-\d{2}\]?`', r'\1', content)

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
