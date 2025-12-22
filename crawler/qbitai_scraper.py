#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QbitAI Scraper
Crawls articles from https://www.qbitai.com and stores them in the database.
"""

import asyncio
import json
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select

from database.models import QbitaiArticle
from database.db_session import get_session
from crawler import utils

# Initialize logger
logger = utils.setup_logger()

class QbitaiWebScraper:
    """Direct scraper for QbitAI website."""
    
    def __init__(self):
        self.base_url = "https://www.qbitai.com"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        self.session = None
    
    async def init(self):
        """Initialize HTTP client."""
        # Note: verify=False to bypass SSL certificate permission issues on macOS
        self.session = httpx.AsyncClient(headers=self.headers, timeout=30, verify=False)
    
    async def close(self):
        """Close HTTP client."""
        if self.session:
            await self.session.aclose()
    
    async def fetch_page(self, url: str, **kwargs) -> Optional[str]:
        """Fetch page content."""
        try:
            response = await self.session.get(url, **kwargs)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Failed to fetch page {url}: {e}")
            return None
    
    async def get_article_list(self, page: int = 1) -> List[Dict]:
        """Get list of articles from a page."""
        try:
            logger.info(f"Fetching article list page {page}...")
            
            if page == 1:
                url = f"{self.base_url}/"
            else:
                url = f"{self.base_url}/?page={page}"
            
            html = await self.fetch_page(url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # Find article elements (div.picture_text is common on QbitAI)
            article_elements = soup.find_all('div', class_='picture_text')
            
            if not article_elements:
                article_elements = soup.find_all(class_=re.compile(r'article|news|post|item', re.I))
            
            if not article_elements:
                article_elements = soup.select('a[href*="/article"], a[href*="/news"]')
            
            logger.info(f"Found {len(article_elements)} potential article elements")
            
            for elem in article_elements[:20]:
                try:
                    if 'picture_text' in elem.get('class', []):
                        title_elem = elem.select_one('.text_box h4 a')
                        if title_elem:
                            title = title_elem.get_text(strip=True)
                            url = title_elem.get('href', '')
                        else:
                            continue
                    elif elem.name == 'a':
                        title = elem.get_text(strip=True)
                        url = elem.get('href', '')
                    else:
                        title_elem = elem.find(['h2', 'h3', 'h4', 'a'])
                        title = title_elem.get_text(strip=True) if title_elem else ''
                        
                        link_elem = elem.find('a', href=re.compile(r'article|news'))
                        url = link_elem.get('href', '') if link_elem else ''
                    
                    if not url or not title:
                        continue
                    
                    if not url.startswith('http'):
                        url = urljoin(self.base_url, url)
                    
                    article_id = self._extract_article_id(url)
                    if not article_id:
                        continue
                    
                    articles.append({
                        'article_id': article_id,
                        'title': title[:500],
                        'url': url,
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to parse article element: {e}")
                    continue
            
            logger.info(f"Page {page}: Extracted {len(articles)} articles")
            return articles
        
        except Exception as e:
            logger.error(f"Failed to get article list: {e}")
            return []
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """Fetch article details."""
        try:
            logger.info(f"Fetching details for: {article_id}")
            
            html = await self.fetch_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            article = {
                'article_id': article_id,
                'url': url,
            }
            
            # Title - 优先查找第一个h1标签（量子位的标题没有特定class）
            title_elem = soup.find('h1')
            if not title_elem:
                # 后备方案：查找包含title类的标题
                title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'title', re.I))
            if not title_elem:
                # 最后尝试从title标签提取
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # 移除网站名称（如" | 量子位"）
                    article['title'] = title_text.split('|')[0].strip()
                else:
                    article['title'] = ''
            else:
                article['title'] = title_elem.get_text(strip=True)
            
            # Content
            content_elem = soup.find(class_=re.compile(r'content|article-body|main', re.I))
            article['content'] = content_elem.get_text(strip=True) if content_elem else ''
            
            # Extract reference links from article content
            reference_links = self._extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # Description
            desc_elem = soup.find(class_=re.compile(r'desc|summary|intro', re.I))
            article['description'] = desc_elem.get_text(strip=True) if desc_elem else article['content'][:200]
            
            # Author
            author_elem = soup.find(class_=re.compile(r'author', re.I))
            article['author'] = author_elem.get_text(strip=True) if author_elem else ''
            
            # Publish Time
            time_elem = soup.find(['time', 'span'], class_=re.compile(r'time|date|pub', re.I))
            if not time_elem:
                time_elem = soup.find('meta', attrs={'property': 'article:published_time'})
                time_str = time_elem.get('content') if time_elem else datetime.now().isoformat()
            else:
                time_str = time_elem.get_text(strip=True) if time_elem.name != 'meta' else time_elem.get('content')
            
            publish_ts = self._parse_timestamp(time_str)
            if publish_ts is None:
                logger.warning(f"Skip article {article_id} due to missing/invalid publish time.")
                return None
            article['publish_time'] = publish_ts
            article['publish_date'] = datetime.fromtimestamp(article['publish_time']).strftime('%Y-%m-%d')
            
            # Category
            cat_elem = soup.find(class_=re.compile(r'category|cat', re.I))
            article['category'] = cat_elem.get_text(strip=True) if cat_elem else ''
            
            # Tags
            tags = []
            for tag_elem in soup.find_all(class_=re.compile(r'tag', re.I)):
                tag_text = tag_elem.get_text(strip=True)
                if tag_text:
                    tags.append(tag_text)
            article['tags'] = json.dumps(tags, ensure_ascii=False) if tags else ''
            
            # Cover Image
            img_elem = soup.find('img', class_=re.compile(r'cover|featured', re.I))
            article['cover_image'] = img_elem.get('src') if img_elem else ''
            
            # Source Keyword
            article['source_keyword'] = 'qbitai'
            
            # Metrics (default 0)
            article['read_count'] = 0
            article['like_count'] = 0
            article['comment_count'] = 0
            article['share_count'] = 0
            article['collect_count'] = 0
            article['is_original'] = 1
            
            return article
        
        except Exception as e:
            logger.error(f"Failed to get article details {article_id}: {e}")
            return None

    def _extract_reference_links(self, soup: BeautifulSoup, content_elem: Optional[BeautifulSoup]) -> List[Dict]:
        """提取文章中的参考链接（论文、GitHub、官方网站等）
        修改：同时扫描文本内容中的URL和<a>标签链接
        """
        reference_links = []
        
        if not content_elem:
            return reference_links
            
        candidates = [] 

        # 1. 提取<a>标签中的链接
        for link in content_elem.find_all('a', href=True):
            href = link.get('href', '').strip()
            text = link.get_text(strip=True)
            if href:
                candidates.append((href, text or href))

        # 2. 提取文本内容中的链接 (处理非超链接形式的URL)
        text_content = content_elem.get_text()
        # 匹配http/https开头，直到遇到空白、括号、引号或中文字符
        url_pattern = re.compile(r'https?://[^\s<>\[\]"\'\u4e00-\u9fa5]+')
        text_urls = url_pattern.findall(text_content)
        
        for url in text_urls:
            # 清理可能的末尾标点
            url = url.rstrip('.,;:。，；：')
            candidates.append((url, url))
        
        # 去重和过滤
        seen_urls = set()
        unique_links = []
        
        for href, text in candidates:
            if not href:
                continue
                
            # 补全相对路径 (主要针对<a>标签)
            if not href.startswith('http'):
                href = urljoin(self.base_url, href)
            
            if href in seen_urls:
                continue
            
            # 过滤掉量子位自身的链接
            if 'qbitai.com' in href.lower():
                continue
                
            # 识别参考来源
            is_reference = False
            ref_type = 'other'
            href_lower = href.lower()
            
            # 论文相关
            if any(domain in href_lower for domain in ['arxiv.org', 'paperswithcode.com', 'semanticscholar.org', 'acm.org', 'ieee.org']):
                is_reference = True
                ref_type = 'paper'
            # GitHub/代码仓库
            elif any(domain in href_lower for domain in ['github.com', 'gitlab.com', 'huggingface.co']):
                is_reference = True
                ref_type = 'code'
            # 官方博客/文档/科技巨头
            elif any(domain in href_lower for domain in ['blog.', 'medium.com', 'openai.com', 'google.com', 'microsoft.com', 'meta.com', 'nvidia.com', 'apple.com', 'aws.amazon.com']):
                is_reference = True
                ref_type = 'official'
            # 社交媒体/内容平台
            elif any(domain in href_lower for domain in ['twitter.com', 'x.com', 'zhihu.com', 'youtube.com', 'bilibili.com']):
                # 排除分享意图的链接
                if not any(k in href_lower for k in ['share', 'intent/tweet', 'sharer']):
                    is_reference = True
                    ref_type = 'social'
            
            # 其他外部链接
            elif href.startswith('http') and not any(social in href_lower for social in ['facebook.com', 'weibo.com', 'qzone.qq.com', 'douban.com']):
                is_reference = True
                ref_type = 'external'
            
            if is_reference:
                seen_urls.add(href)
                unique_links.append({
                    'title': text[:200],
                    'url': href,
                    'type': ref_type
                })
        
        logger.info(f"Extracted {len(unique_links)} reference links")
        return unique_links

    def _extract_article_id(self, url: str) -> Optional[str]:
        patterns = [
            r'/article/(\d+)',
            r'/news/(\d+)',
            r'/(\d+)\.html',
            r'/article/([^/]+)',
            r'article[=/]([^&/?]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return url.split('/')[-1].split('.')[0] if url else None

    def _parse_timestamp(self, time_str: str) -> Optional[int]:
        try:
            if not time_str:
                logger.warning("Publish time missing, skip article.")
                return None
            
            time_str = time_str.strip()
            now = datetime.now()
            
            if '刚刚' in time_str:
                return int(now.timestamp())
            elif '分钟前' in time_str:
                match = re.search(r'(\d+)', time_str)
                minutes = int(match.group(1)) if match else 0
                return int((now - timedelta(minutes=minutes)).timestamp())
            elif '小时前' in time_str:
                match = re.search(r'(\d+)', time_str)
                hours = int(match.group(1)) if match else 0
                return int((now - timedelta(hours=hours)).timestamp())
            elif '天前' in time_str:
                match = re.search(r'(\d+)', time_str)
                days = int(match.group(1)) if match else 0
                return int((now - timedelta(days=days)).timestamp())
            elif '昨天' in time_str:
                return int((now - timedelta(days=1)).timestamp())
            elif '前天' in time_str:
                return int((now - timedelta(days=2)).timestamp())
            
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M:%S',
                '%Y年%m月%d日 %H:%M:%S',
                '%Y年%m月%d日',
                '%Y-%m-%d',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(time_str[:19], fmt)
                    return int(dt.timestamp())
                except:
                    pass
            
            logger.warning(f"Unrecognized publish time format: {time_str}")
            return None
        except:
            logger.warning(f"Failed to parse publish time: {time_str}")
            return None

async def save_article_to_db(article: Dict):
    async with get_session() as session:
        article_id = article.get('article_id')
        if 'url' in article:
            article['article_url'] = article.pop('url')
        
        stmt = select(QbitaiArticle).where(QbitaiArticle.article_id == article_id)
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
            
            valid_keys = {c.name for c in QbitaiArticle.__table__.columns}
            filtered_article = {k: v for k, v in article.items() if k in valid_keys}
            
            db_article = QbitaiArticle(**filtered_article)
            session.add(db_article)
            logger.info(f"Saved new article: {article_id}")
        
        # 移除手动 commit，让上下文管理器自动处理

async def run_crawler(days=3):
    """Run the crawler for the specified number of past days."""
    logger.info("=" * 60)
    logger.info("🚀 QbitAI Crawler Started")
    logger.info("=" * 60)
    
    # 设置日期范围：从 N 天前的零点到现在
    end_date = datetime.now()
    start_date = (datetime.now() - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    
    scraper = QbitaiWebScraper()
    await scraper.init()
    
    try:
        page = 1
        seen_article_ids = set()  # 跟踪已处理的文章ID，避免重复
        consecutive_old_articles = 0  # 连续遇到过期文章的计数
        max_pages = 10  # 最大页数限制，防止无限循环
        
        while page <= max_pages:
            articles = await scraper.get_article_list(page=page)
            if not articles:
                logger.info("No more articles found.")
                break
            
            # 检测重复页面：如果当前页所有文章都已处理过，说明遇到了重复页面
            current_page_article_ids = {art['article_id'] for art in articles}
            if current_page_article_ids.issubset(seen_article_ids):
                logger.warning(f"Page {page} contains only duplicate articles. Stopping crawler.")
                break
            
            should_continue = True
            new_articles_in_page = 0
            
            for article_item in articles:
                article_id = article_item['article_id']
                
                # 跳过已处理的文章
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
                    
                    article_date = article.get('publish_date')
                    
                    # 检查日期是否在范围内
                    if article_date < str(start_date.date()):
                        logger.info(f"Article {article_id} date {article_date} is out of range.")
                        consecutive_old_articles += 1
                        # 如果连续遇到5篇过期文章，停止爬取
                        if consecutive_old_articles >= 5:
                            logger.info(f"Found {consecutive_old_articles} consecutive old articles. Stopping.")
                            should_continue = False
                            break
                        continue
                    else:
                        # 重置计数器
                        consecutive_old_articles = 0
                        new_articles_in_page += 1
                    
                    await save_article_to_db(article)
                    
                    await asyncio.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing article {article_item.get('article_id', 'unknown')}: {e}")
                    continue  # Continue with next article
            
            if not should_continue:
                logger.info("Stop condition met. Exiting crawler.")
                break
            
            # 如果这一页没有新文章，也停止
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

