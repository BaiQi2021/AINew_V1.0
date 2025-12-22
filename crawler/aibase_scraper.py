#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIbase Scraper
Crawls articles from https://www.aibase.com/zh/daily
"""

import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from sqlalchemy import select

from crawler.base_scraper import BaseWebScraper
from database.models import AibaseArticle
from database.db_session import get_session
from crawler import utils

logger = utils.setup_logger()

class AibaseWebScraper(BaseWebScraper):
    """Scraper for AIbase website."""
    
    def __init__(self):
        super().__init__(
            base_url="https://www.aibase.com",
            company_name="aibase"
        )
    
    async def get_article_list(self, page: int = 1) -> List[Dict]:
        """Get list of articles from AIbase daily page."""
        try:
            # AIbase daily page seems to be a single page or infinite scroll?
            # Assuming standard pagination for now or just first page if unknown.
            # The URL provided is /zh/daily.
            
            url = f"{self.base_url}/zh/daily"
            if page > 1:
                # Guessing pagination parameter
                url = f"{url}?page={page}"
            
            logger.info(f"Fetching AIbase article list page {page}: {url}")
            
            html = await self.fetch_page(url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # Find daily report blocks
            # Based on snapshot, there are blocks with date headers.
            # And inside them, lists of news items.
            
            # Strategy: Find all links that look like news/article links.
            # We look for links inside the main content area.
            
            main_content = soup.find('main')
            if not main_content:
                main_content = soup
            
            # Find all links
            links = main_content.find_all('a', href=True)
            
            for link in links:
                href = link.get('href', '').strip()
                title = link.get_text(strip=True)
                
                if not href or not title:
                    continue
                
                # Filter for news/article links
                # Valid patterns: /zh/news/..., /zh/article/...
                if not any(p in href for p in ['/zh/news/', '/zh/article/', '/news/', '/article/']):
                    continue
                
                # Skip pagination or tag links
                if any(p in href for p in ['page=', 'tag', 'category']):
                    continue

                full_url = urljoin(self.base_url, href)
                
                # Extract ID
                article_id = self.extract_article_id(full_url)
                if not article_id:
                    continue
                    
                articles.append({
                    'article_id': f"aibase_{article_id}",
                    'title': title[:500],
                    'url': full_url,
                })
            
            # Deduplicate
            unique_articles = []
            seen = set()
            for art in articles:
                if art['article_id'] not in seen:
                    seen.add(art['article_id'])
                    unique_articles.append(art)
            
            logger.info(f"Page {page}: Extracted {len(unique_articles)} AIbase articles")
            return unique_articles
        
        except Exception as e:
            logger.error(f"Failed to get AIbase article list: {e}")
            return []
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """Fetch article details."""
        try:
            logger.info(f"Fetching AIbase article details: {article_id}")
            
            html = await self.fetch_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            article = {
                'article_id': article_id,
                'article_url': url,
            }
            
            # Title
            title_elem = soup.find('h1')
            if not title_elem:
                title_elem = soup.find(class_=re.compile(r'title', re.I))
            
            article['title'] = self.clean_text(title_elem.get_text()) if title_elem else ''
            if not article['title']:
                # Fallback to title tag
                title_tag = soup.find('title')
                if title_tag:
                    article['title'] = title_tag.get_text(strip=True).split('_')[0].split('-')[0].strip()

            # Source Keyword
            article['source_keyword'] = 'aibase'

            # Content
            content_elem = soup.find(class_=re.compile(r'content|article|detail', re.I))
            # Refine content selection to avoid headers/footers
            if content_elem:
                # Remove ads or related posts if possible
                for irrelevant in content_elem.find_all(class_=re.compile(r'related|share|ad|recommend', re.I)):
                    irrelevant.decompose()
            
            article['content'] = self.clean_text(content_elem.get_text()) if content_elem else ''
            
            # Reference links
            reference_links = self.extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # Publish Time
            # Look for time element
            time_elem = soup.find(['time', 'span', 'div'], class_=re.compile(r'time|date|pub', re.I))
            time_str = ''
            if time_elem:
                time_str = time_elem.get_text(strip=True)
            
            if not time_str:
                # Try meta tag
                meta_time = soup.find('meta', attrs={'property': 'article:published_time'})
                if meta_time:
                    time_str = meta_time.get('content')
            
            if time_str:
                publish_ts = self.parse_timestamp(time_str)
                if publish_ts:
                    article['publish_time'] = publish_ts
                    article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
                else:
                    # Fallback to current time if parse failed but we want to keep it?
                    # Better to log warning
                    logger.warning(f"Could not parse time: {time_str}")
                    article['publish_time'] = int(datetime.now().timestamp())
                    article['publish_date'] = datetime.now().strftime('%Y-%m-%d')
            else:
                # No time found, default to now
                article['publish_time'] = int(datetime.now().timestamp())
                article['publish_date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Description
            desc_elem = soup.find('meta', attrs={'name': 'description'})
            if desc_elem:
                article['description'] = desc_elem.get('content', '')
            else:
                article['description'] = article['content'][:200]
            
            # Author
            author_elem = soup.find(class_=re.compile(r'author|user', re.I))
            article['author'] = self.clean_text(author_elem.get_text()) if author_elem else 'AIbase'
            
            # Cover Image
            img_elem = soup.find(class_=re.compile(r'cover|thumb', re.I))
            if img_elem and img_elem.name == 'img':
                 article['cover_image'] = img_elem.get('src', '')
            elif img_elem:
                img = img_elem.find('img')
                if img:
                    article['cover_image'] = img.get('src', '')
            
            if 'cover_image' not in article or not article['cover_image']:
                 # Try og:image
                 og_img = soup.find('meta', attrs={'property': 'og:image'})
                 if og_img:
                     article['cover_image'] = og_img.get('content', '')

            # Category/Tags
            tags = []
            for tag in soup.find_all(class_=re.compile(r'tag|label', re.I)):
                t = self.clean_text(tag.get_text())
                if t and t not in tags:
                    tags.append(t)
            article['tags'] = json.dumps(tags, ensure_ascii=False) if tags else ''
            
            # Defaults
            article['read_count'] = 0
            article['like_count'] = 0
            article['comment_count'] = 0
            article['share_count'] = 0
            article['collect_count'] = 0
            article['is_original'] = 0
            
            return article
            
        except Exception as e:
            logger.error(f"Failed to get article details {article_id}: {e}")
            return None

async def save_article_to_db(article: Dict):
    """Save article to AibaseArticle table."""
    async with get_session() as session:
        article_id = article.get('article_id')
        
        stmt = select(AibaseArticle).where(AibaseArticle.article_id == article_id)
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
            
            valid_keys = {c.name for c in AibaseArticle.__table__.columns}
            filtered_article = {k: v for k, v in article.items() if k in valid_keys}
            
            db_article = AibaseArticle(**filtered_article)
            session.add(db_article)
            logger.info(f"Saved new article: {article_id}")

async def run_crawler(days=3):
    """Run the AIbase crawler."""
    logger.info("=" * 60)
    logger.info("🚀 AIbase Crawler Started")
    logger.info("=" * 60)
    
    start_date = (datetime.now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Date range: {start_date.date()} to {datetime.now().date()}")
    
    scraper = AibaseWebScraper()
    await scraper.init()
    
    try:
        # Assuming single page or simple pagination
        # AIbase daily page might contain multiple days of news
        articles = await scraper.get_article_list(page=1)
        
        for article_item in articles:
            try:
                # Check if we already processed this URL? 
                # Ideally we check DB first, but save_article_to_db handles upsert/check.
                # However to save requests, we could check.
                # For now, just fetch details.
                
                article = await scraper.get_article_detail(
                    article_item['article_id'],
                    article_item['url']
                )
                
                if not article:
                    continue
                
                # Date check
                if article.get('publish_date', '') < str(start_date.date()):
                    logger.info(f"Article {article['article_id']} too old ({article.get('publish_date')})")
                    continue
                    
                await save_article_to_db(article)
                await asyncio.sleep(2)  # Polite delay
                
            except Exception as e:
                logger.error(f"Error processing {article_item['article_id']}: {e}")
                continue
                
    finally:
        await scraper.close()
        logger.info("AIbase Crawler finished.")

if __name__ == "__main__":
    asyncio.run(run_crawler())

