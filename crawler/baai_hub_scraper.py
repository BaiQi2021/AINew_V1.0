#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BAAI Hub Scraper
Crawls articles from https://hub.baai.ac.cn/
"""

import re
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from sqlalchemy import select

from crawler.base_scraper import BaseWebScraper
from crawler import utils
from database.models import BaaiHubArticle
from database.db_session import get_session

logger = utils.setup_logger()

class BaaiHubScraper(BaseWebScraper):
    """Scraper for BAAI Hub website."""
    
    def __init__(self):
        super().__init__(
            base_url="https://hub.baai.ac.cn",
            company_name="baai_hub"
        )
        self.api_url = "https://hub-api.baai.ac.cn/api/v1/story/list"
    
    async def get_article_list(self, page: int = 1) -> List[Dict]:
        """Get list of articles from BAAI Hub API."""
        try:
            logger.info(f"Fetching BAAI Hub list page {page} from API")
            
            payload = {
                "page": page,
                "limit": 10,
                "sort": "new"
            }
            
            # Ensure session is initialized
            if not self.session:
                await self.init()
                
            response = await self.session.post(self.api_url, json=payload)
            response.raise_for_status()
            data = response.json()
            
            articles = []
            # Handle response structure: data -> data (list)
            if 'data' in data and isinstance(data['data'], list):
                stories = data['data']
                for story in stories:
                    info = story.get('story_info', {})
                    if not info:
                        continue
                        
                    article_id = str(info.get('id'))
                    title = info.get('title')
                    url = info.get('url')
                    publish_date = info.get('created_at')
                    
                    # If URL is internal /view/xxx, make it absolute
                    if url and not url.startswith('http'):
                        url = urljoin(self.base_url, url)
                    
                    # If URL is empty, construct from ID
                    if not url:
                        url = f"{self.base_url}/view/{article_id}"

                    articles.append({
                        'article_id': article_id,
                        'title': title,
                        'url': url,
                        'publish_date': publish_date,
                        'description': info.get('summary'),
                        'source_keyword': 'baai_hub'
                    })
            
            logger.info(f"Extracted {len(articles)} BAAI Hub articles from API")
            return articles
            
        except Exception as e:
            logger.error(f"Failed to fetch BAAI Hub list from API: {e}")
            return []

    def _parse_nuxt_data(self, html: str) -> List[Dict]:
        """Parse articles from window.__NUXT__ data."""
        try:
            # Extract the script content
            match = re.search(r'window\.__NUXT__=\(function\((.*?)\)\{return (.*?)\}\}\((.*?)\)\);', html, re.DOTALL)
            if not match:
                return []
            
            arg_names_str = match.group(1)
            body_str = match.group(2)
            args_values_str = match.group(3)
            
            arg_names = [x.strip() for x in arg_names_str.split(',')]
            
            # Parse argument values
            arg_values = self._parse_js_args(args_values_str)
            
            # Create a mapping
            # Handle case where args count mismatch (sometimes happens with trailing undefineds)
            var_map = {}
            for i, name in enumerate(arg_names):
                if i < len(arg_values):
                    var_map[name] = arg_values[i]
                else:
                    var_map[name] = None
            
            # Find story_info blocks in the body
            articles = []
            
            # Regex to find story_info objects
            story_matches = re.finditer(r'story_info:\{id:(\w+),title:"(.*?)",user_id:\w+,created_at:(\w+),url:"(.*?)",.*?,summary:"(.*?)"', body_str)
            
            for m in story_matches:
                id_var = m.group(1)
                title = m.group(2)
                created_at_var = m.group(3)
                url = m.group(4)
                summary = m.group(5)
                
                # Resolve ID
                article_id = var_map.get(id_var)
                if not article_id:
                    # Try to see if id_var is a literal number
                    if id_var.isdigit():
                        article_id = id_var
                    else:
                        continue
                
                # Resolve Date
                publish_date = var_map.get(created_at_var)
                if not publish_date and created_at_var.startswith('"'):
                     publish_date = created_at_var.strip('"')
                
                # Clean up unicode escapes
                if '\\u' in title:
                    try:
                        title = title.encode('utf-8').decode('unicode_escape')
                    except:
                        pass
                
                full_url = f"https://hub.baai.ac.cn/view/{article_id}"
                
                articles.append({
                    'article_id': str(article_id),
                    'title': title,
                    'url': full_url,
                    'publish_date': str(publish_date) if publish_date else None,
                    'source_keyword': None,
                    'description': summary
                })
                
            return articles
            
        except Exception as e:
            logger.error(f"Failed to parse Nuxt data: {e}")
            return []

    def _parse_nuxt_detail(self, html: str) -> Optional[Dict]:
        """Parse article detail from window.__NUXT__ data."""
        try:
            match = re.search(r'window\.__NUXT__=\(function\((.*?)\)\{return (.*?)\}\}\((.*?)\)\);', html, re.DOTALL)
            if not match:
                return None
            
            arg_names_str = match.group(1)
            body_str = match.group(2)
            args_values_str = match.group(3)
            
            arg_names = [x.strip() for x in arg_names_str.split(',')]
            arg_values = self._parse_js_args(args_values_str)
            
            var_map = {}
            for i, name in enumerate(arg_names):
                if i < len(arg_values):
                    var_map[name] = arg_values[i]
                else:
                    var_map[name] = None
            
            # Extract title
            title = None
            # Try to find title in detail object
            # detail:{id:...,title:g,...}
            # Use DOTALL and non-greedy match
            # Also try to match title: without detail prefix if that fails
            title_match = re.search(r'detail:\{.*?title:(\w+|"[^"]*")', body_str, re.DOTALL)
            if title_match:
                val = title_match.group(1)
                if val.startswith('"'):
                    title = val[1:-1]
                else:
                    title = var_map.get(val)
            else:
                # Fallback: just look for title:g in the whole body
                # This is risky but better than None
                title_match = re.search(r'title:(\w+|"[^"]*")', body_str)
                if title_match:
                    val = title_match.group(1)
                    if val.startswith('"'):
                        title = val[1:-1]
                    else:
                        title = var_map.get(val)

            # Extract content
            content = None
            # Look for content: ...
            # It could be content:variable or content:"string"
            
            # Regex for content:"string" (handling escaped quotes)
            content_str_match = re.search(r'content:"((?:[^"\\]|\\.)*)"', body_str)
            if content_str_match:
                content = content_str_match.group(1)
                # Unescape unicode and quotes
                try:
                    content = content.encode('utf-8').decode('unicode_escape')
                except:
                    pass
                
                # Fix encoding again if it was escaped mojibake (e.g. \u00E5 instead of \u5BFC)
                content = self._fix_encoding(content)
            else:
                # Regex for content:variable
                content_var_match = re.search(r'content:(\w+)[,}]', body_str)
                if content_var_match:
                    var_name = content_var_match.group(1)
                    if var_name in var_map:
                        content = var_map[var_name]
                        # Also fix encoding for variable content just in case
                        if isinstance(content, str):
                            content = self._fix_encoding(content)

            # Extract publish_date
            publish_date = None
            date_match = re.search(r'created_at:(\w+|"[^"]*")', body_str)
            if date_match:
                val = date_match.group(1)
                if val.startswith('"'):
                    publish_date = val[1:-1]
                else:
                    publish_date = var_map.get(val)

            if content:
                # Convert HTML content to text if needed, or keep HTML
                # The base scraper usually expects text, but HTML is fine if we want to preserve structure.
                # Let's convert to text to be consistent with other scrapers
                soup = BeautifulSoup(content, 'html.parser')
                
                # Extract reference links
                reference_links = []
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if href.startswith('http'):
                        reference_links.append(href)
                
                content = soup.get_text("\n", strip=True)
                
                # Extract links from text content
                text_urls = self._extract_urls_from_text(content)
                for url in text_urls:
                    if url not in reference_links:
                        reference_links.append(url)
            else:
                reference_links = []

            return {
                'title': title,
                'content': content,
                'publish_date': publish_date,
                'source_keyword': 'baai_hub',
                'reference_links': reference_links
            }
            
        except Exception as e:
            logger.error(f"Error parsing Nuxt detail: {e}")
            return None

    def _parse_js_args(self, args_str: str) -> List[Any]:
        """
        Parse a JS argument list string into a Python list.
        Handles strings, numbers, booleans, null.
        """
        args = []
        current_token = ""
        in_quote = False
        quote_char = None
        escape = False
        
        for char in args_str:
            if in_quote:
                if escape:
                    current_token += char
                    escape = False
                elif char == '\\':
                    escape = True
                    current_token += char # Keep escape for now, handle later? Or just drop it?
                    # Actually for simple parsing let's keep it
                elif char == quote_char:
                    in_quote = False
                    current_token += char
                else:
                    current_token += char
            else:
                if char == '"' or char == "'":
                    in_quote = True
                    quote_char = char
                    current_token += char
                elif char == ',':
                    args.append(self._parse_js_value(current_token.strip()))
                    current_token = ""
                else:
                    current_token += char
        
        if current_token:
            args.append(self._parse_js_value(current_token.strip()))
            
        return args

    def _parse_js_value(self, token: str) -> Any:
        if token == 'true': return True
        if token == 'false': return False
        if token == 'null': return None
        if token == 'void 0': return None
        if token.startswith('"') and token.endswith('"'):
            return token[1:-1]
        if token.startswith("'") and token.endswith("'"):
            return token[1:-1]
        try:
            return int(token)
        except:
            try:
                return float(token)
            except:
                return token # Return as string if unknown

    def _extract_urls_from_text(self, text: str) -> List[str]:
        """Extract URLs from text using regex."""
        if not text:
            return []
        
        # Regex for URL extraction
        # Matches http/https URLs, handling common punctuation at the end
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        urls = re.findall(url_pattern, text)
        
        # Clean up URLs (remove trailing punctuation that might have been matched)
        cleaned_urls = []
        for url in urls:
            # Remove trailing punctuation often found in text
            url = url.rstrip('.,;:)!>]')
            
            # Filter out WeChat links
            if 'mp.weixin.qq.com' in url:
                continue
                
            if url not in cleaned_urls:
                cleaned_urls.append(url)
                
        return cleaned_urls

    def _parse_weixin_detail(self, html: str) -> Optional[Dict]:
        """Parse WeChat article detail."""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Title
            title = ""
            title_tag = soup.find('h1', class_='rich_media_title')
            if title_tag:
                title = title_tag.get_text(strip=True)
            
            # Content
            content = ""
            content_div = soup.find('div', id='js_content')
            
            reference_links = []
            if content_div:
                # Extract links from <a> tags
                for a in content_div.find_all('a', href=True):
                    href = a['href']
                    if href.startswith('http') and 'mp.weixin.qq.com' not in href:
                        reference_links.append(href)
                
                content = content_div.get_text("\n", strip=True)
                
                # Extract links from text content
                text_urls = self._extract_urls_from_text(content)
                for url in text_urls:
                    if url not in reference_links:
                        reference_links.append(url)
            
            # Publish Date
            publish_date = None
            # Try to find publish_time in scripts
            # var ct = "1703212345";
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string:
                    match = re.search(r'ct\s*=\s*"(\d+)"', script.string)
                    if match:
                        timestamp = int(match.group(1))
                        publish_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        break
            
            return {
                'title': title,
                'content': content,
                'publish_date': publish_date,
                'source_keyword': 'baai_hub',
                'reference_links': reference_links
            }
        except Exception as e:
            logger.error(f"Error parsing WeChat detail: {e}")
            return None

    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """Get article detail."""
        logger.info(f"Fetching article detail: {url}")
        html = await self.fetch_page(url)
        if not html:
            return None
            
        # Fix encoding if needed (BAAI Hub detail pages sometimes return ISO-8859-1)
        html = self._fix_encoding(html)
        
        # Check if it is a WeChat article
        if 'weixin.qq.com' in url:
            weixin_detail = self._parse_weixin_detail(html)
            if weixin_detail and weixin_detail.get('content'):
                logger.info("Successfully parsed detail from WeChat")
                return weixin_detail
            
        # 1. Try to parse Nuxt data first
        nuxt_detail = self._parse_nuxt_detail(html)
        if nuxt_detail and nuxt_detail.get('content'):
            logger.info("Successfully parsed detail from Nuxt data")
            return nuxt_detail

        # 2. Fallback to HTML parsing
        soup = BeautifulSoup(html, 'html.parser')
        
        # Title
        title = ""
        title_tag = soup.find('h1')
        if title_tag:
            title = title_tag.get_text(strip=True)
        
        # Content
        # Try to find the main content area
        content = ""
        reference_links = []
        
        # Common classes for content
        content_classes = ['article-content', 'post-content', 'detail-content', 'content', 'main-text', 'post-content']
        content_div = soup.find('div', class_=re.compile('|'.join(content_classes), re.I))
        
        # Specific for BAAI Hub
        if not content_div:
            content_div = soup.find('div', id='post-content')

        if not content_div:
            # Fallback: find the div with the most p tags
            divs = soup.find_all('div')
            max_p_count = 0
            best_div = None
            for div in divs:
                p_count = len(div.find_all('p', recursive=False))
                if p_count > max_p_count:
                    max_p_count = p_count
                    best_div = div
            content_div = best_div
            
        if content_div:
            # Extract links
            for a in content_div.find_all('a', href=True):
                href = a['href']
                if href.startswith('http') and 'mp.weixin.qq.com' not in href:
                    reference_links.append(href)
            
            content = content_div.get_text("\n", strip=True)
            
            # Extract links from text content
            text_urls = self._extract_urls_from_text(content)
            for url in text_urls:
                if url not in reference_links:
                    reference_links.append(url)
        else:
            # Last resort: body text
            content = soup.body.get_text("\n", strip=True) if soup.body else ""

        # Meta info
        publish_date = None
        source_keyword = None
        
        # Look for meta info in the page
        text = soup.get_text(" ", strip=True)
        date_match = re.search(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})', text)
        if date_match:
            publish_date = date_match.group(1)
            
        return {
            'title': title,
            'content': content,
            'publish_date': publish_date,
            'source_keyword': 'baai_hub', # Might be None if not found
            'reference_links': reference_links
        }

    def _fix_encoding(self, text: str) -> str:
        """
        Fix common encoding issues.
        Tries to fix UTF-8 content that was interpreted as Latin-1 or CP1252.
        """
        try:
            # Try cp1252 first (common default)
            return text.encode('cp1252').decode('utf-8')
        except:
            try:
                # Try latin-1 with replace to handle edge cases
                return text.encode('latin-1').decode('utf-8', errors='replace')
            except:
                return text

async def save_article_to_db(article: Dict):
    async with get_session() as session:
        article_id = article.get('article_id')
        if 'url' in article:
            article['article_url'] = article.pop('url')
        
        stmt = select(BaaiHubArticle).where(BaaiHubArticle.article_id == article_id)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            existing.last_modify_ts = utils.get_current_timestamp()
            for key, value in article.items():
                if hasattr(existing, key) and key not in ['id', 'add_ts']:
                    setattr(existing, key, value)
            logger.info(f"Updated article: {article_id}")
        else:
            article['add_ts'] = utils.get_current_timestamp()
            article['last_modify_ts'] = utils.get_current_timestamp()
            
            # Filter keys that exist in the model
            valid_keys = {c.name for c in BaaiHubArticle.__table__.columns}
            filtered_article = {k: v for k, v in article.items() if k in valid_keys}
            
            # Handle publish_time if missing but publish_date exists
            if 'publish_time' not in filtered_article and 'publish_date' in filtered_article:
                try:
                    # Try to parse date string to timestamp
                    # Format example: 2025-12-22 13:20
                    dt_str = filtered_article['publish_date']
                    if dt_str:
                        # Remove " 分享" suffix if present
                        if " 分享" in dt_str:
                            dt_str = dt_str.replace(" 分享", "")
                        
                        dt = None
                        # Try multiple formats
                        formats = ["%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d %H:%M", "%Y/%m/%d"]
                        for fmt in formats:
                            try:
                                dt = datetime.strptime(dt_str, fmt)
                                break
                            except ValueError:
                                continue
                        
                        if dt:
                            filtered_article['publish_time'] = int(dt.timestamp())
                        else:
                            logger.warning(f"Could not parse date string: {dt_str}")
                except Exception as e:
                    logger.warning(f"Failed to parse date {filtered_article.get('publish_date')}: {e}")

            db_article = BaaiHubArticle(**filtered_article)
            session.add(db_article)
            logger.info(f"Saved new article: {article_id}")

async def run_crawler(days=3):
    """Run the crawler for the specified number of past days."""
    logger.info("=" * 60)
    logger.info("🚀 BAAI Hub Crawler Started")
    logger.info("=" * 60)
    
    # Date range
    end_date = datetime.now()
    start_date = (datetime.now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    scraper = BaaiHubScraper()
    await scraper.init()
    
    try:
        page = 1
        seen_article_ids = set()
        consecutive_old_articles = 0
        max_pages = 10
        
        while page <= max_pages:
            articles = await scraper.get_article_list(page=page)
            if not articles:
                logger.info("No more articles found.")
                break
            
            current_page_article_ids = {art['article_id'] for art in articles}
            if current_page_article_ids.issubset(seen_article_ids):
                logger.warning(f"Page {page} contains only duplicate articles. Stopping crawler.")
                break
            
            should_continue = True
            new_articles_in_page = 0
            
            for article_item in articles:
                article_id = article_item['article_id']
                
                if article_id in seen_article_ids:
                    continue
                
                seen_article_ids.add(article_id)
                
                try:
                    article = await scraper.get_article_detail(
                        article_id,
                        article_item['url']
                    )
                    
                    if not article:
                        logger.warning(f"Skipping article {article_id} - failed to fetch details")
                        continue
                    
                    # Merge list info into detail
                    article['article_id'] = article_id
                    article['url'] = article_item['url']
                    if not article.get('publish_date'):
                        article['publish_date'] = article_item.get('publish_date')
                    if not article.get('description'):
                        article['description'] = article_item.get('description')

                    article_date_str = article.get('publish_date')
                    
                    # Clean publish_date for database (VARCHAR(10))
                    if article_date_str:
                        # Remove suffixes
                        clean_date = article_date_str.replace(" 分享", "").replace(" 发布", "").strip()
                        # Truncate to 10 chars (YYYY-MM-DD)
                        if len(clean_date) >= 10:
                            article['publish_date'] = clean_date[:10]
                    
                    # Check date
                    is_old = False
                    if article_date_str:
                        try:
                            # Clean date string
                            clean_date_str = article_date_str.replace(" 分享", "")
                            # Try to parse date
                            # It might be YYYY-MM-DD HH:MM or just YYYY-MM-DD
                            if len(clean_date_str) >= 10:
                                article_dt = datetime.strptime(clean_date_str[:10], "%Y-%m-%d")
                                if article_dt.date() < start_date.date():
                                    is_old = True
                        except Exception as e:
                            logger.warning(f"Date parse error: {e}")
                    
                    if is_old:
                        logger.info(f"Article {article_id} date {article_date_str} is out of range.")
                        consecutive_old_articles += 1
                        if consecutive_old_articles >= 5:
                            logger.info(f"Found {consecutive_old_articles} consecutive old articles. Stopping.")
                            should_continue = False
                            break
                        continue
                    else:
                        consecutive_old_articles = 0
                        new_articles_in_page += 1
                    
                    await save_article_to_db(article)
                    
                    await asyncio.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing article {article_item.get('article_id', 'unknown')}: {e}")
                    continue
            
            if not should_continue:
                logger.info("Stop condition met. Exiting crawler.")
                break
            
            if new_articles_in_page == 0:
                logger.info(f"No new articles found on page {page}. Stopping.")
                break
            
            logger.info(f"Page {page} completed: {new_articles_in_page} new articles processed.")
            page += 1
            await asyncio.sleep(2)
            
    finally:
        await scraper.close()
        logger.info("Crawler finished.")

if __name__ == "__main__":
    asyncio.run(run_crawler())
