#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Companies Scraper
AI公司官网爬虫（保留可用的爬虫：NVIDIA）
"""

import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from sqlalchemy import select

from crawler.base_scraper import BaseWebScraper
from database.models import CompanyArticle
from database.db_session import get_session
from crawler import utils

logger = utils.setup_logger()


class NVIDIAScraper(BaseWebScraper):
    """NVIDIA新闻爬虫"""
    
    def __init__(self):
        super().__init__(
            base_url="https://nvidianews.nvidia.com/",
            company_name="nvidia"
        )
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """获取文章详情 (NVIDIA专用)"""
        try:
            logger.info(f"Fetching NVIDIA article details: {article_id}")
            html = await self.fetch_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            article = {
                'article_id': article_id,
                'article_url': url,
                'source_keyword': 'nvidia',
                'company': 'nvidia',
            }
            
            # Title
            title_elem = soup.find('h1')
            article['title'] = self.clean_text(title_elem.get_text()) if title_elem else ''
            
            # Content
            content_elem = soup.find('div', class_='article-content')
            if not content_elem:
                content_elem = soup.find('div', class_='news-body')
            if not content_elem:
                content_elem = soup.find('main')
            
            article['content'] = self.clean_text(content_elem.get_text()) if content_elem else ''
            
            # Reference Links
            reference_links = self.extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # Publish Time
            # NVIDIA news usually has date in a div/span with class 'date' or 'timestamp'
            # Format: "May 21, 2025" or "Wednesday, May 21, 2025"
            time_str = ''
            
            # 1. Try JSON-LD
            ld_scripts = soup.find_all('script', type='application/ld+json')
            for script in ld_scripts:
                try:
                    data = json.loads(script.string)
                    if 'datePublished' in data:
                        time_str = data['datePublished']
                        break
                except:
                    pass
            
            # 2. Try meta tags
            if not time_str:
                time_elem = soup.find('meta', attrs={'property': 'article:published_time'})
                if time_elem:
                    time_str = time_elem.get('content')
            
            # 3. Try HTML elements
            if not time_str:
                # Look for date in header or specific classes
                date_elem = soup.find(class_=lambda x: x and any(c in str(x).lower() for c in ['date', 'timestamp', 'published']))
                if date_elem:
                    time_str = date_elem.get_text(strip=True)
            
            if not time_str:
                # Fallback: try to find date pattern in the first few lines of text
                text = soup.get_text()[:1000]
                # Match: Month DD, YYYY
                match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}', text, re.IGNORECASE)
                if match:
                    time_str = match.group(0)
            
            if not time_str:
                logger.warning(f"Skip article {article_id}: missing publish time.")
                return None
                
            publish_ts = self.parse_timestamp(time_str)
            if publish_ts is None:
                logger.warning(f"Skip article {article_id}: cannot parse publish time: {time_str}")
                return None
                
            article['publish_time'] = publish_ts
            article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
            
            # Other fields
            article['description'] = article['content'][:200]
            article['author'] = 'NVIDIA'
            article['category'] = 'News'
            article['tags'] = ''
            article['cover_image'] = ''
            article['is_original'] = 1
            
            return article
            
        except Exception as e:
            logger.error(f"Failed to get NVIDIA article details {article_id}: {e}")
            return None
    
    async def get_article_list(self, page: int = 1) -> List[Dict]:
        """获取文章列表"""
        try:
            logger.info(f"Fetching NVIDIA news list from {self.base_url}...")
            
            html = await self.fetch_page(self.base_url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            article_elements = soup.find_all(['article', 'div'], class_=lambda x: x and any(
                keyword in str(x).lower() for keyword in ['news', 'article', 'post', 'item']
            ))
            
            logger.info(f"Found {len(article_elements)} potential article elements")
            
            for elem in article_elements[:20]:
                try:
                    link_elem = elem.find('a', href=True)
                    if not link_elem:
                        continue
                    
                    url = link_elem.get('href', '')
                    if url.startswith('/'):
                        url = 'https://nvidianews.nvidia.com' + url
                    elif not url.startswith('http'):
                        continue
                    
                    article_id = self.extract_article_id(url)
                    if not article_id:
                        continue
                    
                    title_elem = elem.find(['h1', 'h2', 'h3', 'h4'])
                    if not title_elem:
                        title_elem = link_elem
                    title = self.clean_text(title_elem.get_text())
                    
                    if not title or len(title) < 5:
                        continue
                    
                    articles.append({
                        'article_id': f"nvidia_{article_id}",
                        'title': title[:500],
                        'url': url,
                    })
                    
                except Exception as e:
                    logger.debug(f"Failed to parse element: {e}")
                    continue
            
            logger.info(f"Extracted {len(articles)} NVIDIA articles")
            return articles
        
        except Exception as e:
            logger.error(f"Failed to get NVIDIA article list: {e}")
            return []


# 通用文章详情获取函数
async def get_generic_article_detail(scraper: BaseWebScraper, article_id: str, url: str) -> Optional[Dict]:
    """通用文章详情获取"""
    try:
        logger.info(f"Fetching article details: {article_id}")
        
        html = await scraper.fetch_page(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        article = {
            'article_id': article_id,
            'article_url': url,
            'source_keyword': scraper.company_name,
            'company': scraper.company_name,
        }
        
        # 标题
        title_elem = soup.find('h1')
        if not title_elem:
            title_elem = soup.find('title')
            article['title'] = title_elem.get_text(strip=True).split('|')[0].strip() if title_elem else ''
        else:
            article['title'] = scraper.clean_text(title_elem.get_text())
        
        # 内容
        content_elem = soup.find('article')
        if not content_elem:
            content_elem = soup.find(['div', 'main'], class_=lambda x: x and 'content' in str(x).lower())
        if not content_elem:
            content_elem = soup.find('main')
        
        article['content'] = scraper.clean_text(content_elem.get_text()) if content_elem else ''
        
        # 提取参考链接
        reference_links = scraper.extract_reference_links(soup, content_elem)
        article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
        
        # 描述
        desc_elem = soup.find('meta', attrs={'name': 'description'})
        if not desc_elem:
            desc_elem = soup.find('meta', attrs={'property': 'og:description'})
        article['description'] = desc_elem.get('content', '')[:500] if desc_elem else article['content'][:200]
        
        # 作者
        author_elem = soup.find(class_=lambda x: x and 'author' in str(x).lower())
        if not author_elem:
            author_elem = soup.find('meta', attrs={'name': 'author'})
            author_text = author_elem.get('content', scraper.company_name) if author_elem else scraper.company_name
        else:
            author_text = scraper.clean_text(author_elem.get_text())
        
        # 严格限制作者字段长度，避免提取到整段内容
        if len(author_text) > 100:  # 如果太长，说明提取错误
            author_text = scraper.company_name
        article['author'] = author_text[:255]  # 最终限制
        
        # 发布时间
        time_elem = soup.find('time')
        if not time_elem:
            time_elem = soup.find('meta', attrs={'property': 'article:published_time'})
            time_str = time_elem.get('content') if time_elem else ''
        else:
            time_str = time_elem.get('datetime', '') or time_elem.get_text(strip=True)
        
        if not time_str:
            logger.warning(f"Skip article {article_id}: missing publish time.")
            return None
        publish_ts = scraper.parse_timestamp(time_str)
        if publish_ts is None:
            logger.warning(f"Skip article {article_id}: cannot parse publish time: {time_str}")
            return None
        article['publish_time'] = publish_ts
        article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
        
        # 其他字段
        article['category'] = 'AI资讯'
        article['tags'] = ''
        article['cover_image'] = ''
        article['is_original'] = 1
        
        return article
    
    except Exception as e:
        logger.error(f"Failed to get article details {article_id}: {e}")
        return None


async def save_company_article_to_db(article: Dict):
    """保存文章到数据库"""
    async with get_session() as session:
        article_id = article.get('article_id')
        
        stmt = select(CompanyArticle).where(CompanyArticle.article_id == article_id)
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
            
            valid_keys = {c.name for c in CompanyArticle.__table__.columns}
            filtered_article = {k: v for k, v in article.items() if k in valid_keys}
            
            db_article = CompanyArticle(**filtered_article)
            session.add(db_article)
            logger.info(f"Saved new article: {article_id}")


async def run_nvidia_crawler(days: int = 7):
    """运行NVIDIA爬虫"""
    logger.info(f"Starting NVIDIA Crawler (days={days})...")
    scraper = NVIDIAScraper()
    await scraper.init()
    
    try:
        articles = await scraper.get_article_list()
        logger.info(f"Found {len(articles)} articles")
        
        for article_item in articles:
            try:
                article = await scraper.get_article_detail(
                    article_item['article_id'],
                    article_item['url']
                )
                
                if article:
                    # 检查日期
                    if days > 0:
                        article_ts = article['publish_time']
                        now_ts = datetime.now().timestamp()
                        if article_ts > now_ts + 86400:
                             logger.warning(f"Skip article {article['title']}: future date ({article['publish_date']})")
                             continue
                        if now_ts - article_ts > days * 86400:
                             logger.info(f"Skip article {article['title']}: too old ({article['publish_date']})")
                             continue

                    await save_company_article_to_db(article)
                
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Error processing NVIDIA article: {e}")
                continue
                
    finally:
        await scraper.close()
        logger.info("NVIDIA Crawler finished.")


if __name__ == "__main__":
    async def test():
        scraper = NVIDIAScraper()
        await scraper.init()
        
        articles = await scraper.get_article_list()
        logger.info(f"Found {len(articles)} articles")
        
        if articles:
            detail = await get_generic_article_detail(scraper, articles[0]['article_id'], articles[0]['url'])
            if detail:
                logger.info(f"Article: {detail['title']}")
        
        await scraper.close()
    
    asyncio.run(test())
