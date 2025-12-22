#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI Tools Blog Scraper
通用AI工具博客爬虫
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


class GenericBlogScraper(BaseWebScraper):
    """通用博客爬虫，适用于大多数博客网站"""
    
    def __init__(self, base_url: str, company_name: str):
        super().__init__(base_url=base_url, company_name=company_name)
    
    async def get_article_list(self, page: int = 1) -> List[Dict]:
        """获取文章列表"""
        try:
            url = self.base_url
            if page > 1:
                # 尝试常见的分页模式
                for pattern in [f'?page={page}', f'/page/{page}', f'?p={page}']:
                    test_url = self.base_url.rstrip('/') + pattern
                    html = await self.fetch_page(test_url)
                    if html:
                        url = test_url
                        break
            else:
                html = await self.fetch_page(url)
            
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            # 尝试多种常见的文章容器选择器
            article_elements = []
            
            # 1. 查找 article 标签
            article_elements = soup.find_all('article')
            
            # 2. 如果没找到，查找常见的博客容器类名
            if not article_elements:
                article_elements = soup.find_all(['div', 'li'], class_=lambda x: x and any(
                    keyword in str(x).lower() for keyword in 
                    ['post', 'article', 'blog', 'card', 'item', 'entry', 'content']
                ))
            
            # 3. 如果还是没找到，查找包含链接的容器
            if not article_elements:
                article_elements = soup.find_all('a', href=lambda x: x and any(
                    pattern in x for pattern in ['/blog/', '/post/', '/article/', '/news/']
                ))
            
            logger.info(f"Found {len(article_elements)} potential article elements")
            
            for elem in article_elements[:30]:
                try:
                    # 查找链接
                    if elem.name == 'a':
                        link_elem = elem
                    else:
                        link_elem = elem.find('a', href=True)
                    
                    if not link_elem:
                        continue
                    
                    url = link_elem.get('href', '')
                    if not url:
                        continue
                    
                    # 处理相对路径
                    if url.startswith('/'):
                        # 从 base_url 提取域名
                        from urllib.parse import urlparse
                        parsed = urlparse(self.base_url)
                        base_domain = f"{parsed.scheme}://{parsed.netloc}"
                        url = base_domain + url
                    elif not url.startswith('http'):
                        # 如果是相对路径但不以 / 开头，拼接到 base_url
                        url = self.base_url.rstrip('/') + '/' + url
                    
                    # 过滤非内容链接
                    if any(skip in url.lower() for skip in ['#', 'javascript:', 'mailto:', '.pdf', '.jpg', '.png']):
                        continue
                    
                    article_id = self.extract_article_id(url)
                    if not article_id:
                        continue
                    
                    # 查找标题
                    title_elem = elem.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                    if not title_elem:
                        title_elem = link_elem
                    
                    title = self.clean_text(title_elem.get_text())
                    if not title:
                        title = link_elem.get('title', '')
                    
                    if not title or len(title) < 5:
                        continue
                    
                    articles.append({
                        'article_id': f"{self.company_name}_{article_id}",
                        'title': title[:500],
                        'url': url,
                    })
                    
                except Exception as e:
                    logger.debug(f"Failed to parse article element: {e}")
                    continue
            
            logger.info(f"Extracted {len(articles)} articles from {self.company_name}")
            return articles
        
        except Exception as e:
            logger.error(f"Failed to get article list from {self.company_name}: {e}")
            return []
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """获取文章详情"""
        try:
            logger.info(f"Fetching article details: {article_id}")
            
            html = await self.fetch_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            article = {
                'article_id': article_id,
                'article_url': url,
                'source_keyword': self.company_name,
                'company': self.company_name,
            }
            
            # 标题
            title_elem = soup.find('h1')
            if not title_elem:
                title_elem = soup.find('title')
                if title_elem:
                    article['title'] = title_elem.get_text(strip=True).split('|')[0].strip()
                else:
                    article['title'] = ''
            else:
                article['title'] = self.clean_text(title_elem.get_text())
            
            # 内容 - 尝试多种选择器
            content_elem = None
            for selector in [
                {'name': 'article'},
                {'name': 'div', 'class_': lambda x: x and 'content' in str(x).lower()},
                {'name': 'div', 'class_': lambda x: x and 'post' in str(x).lower()},
                {'name': 'main'},
            ]:
                content_elem = soup.find(**selector)
                if content_elem:
                    break
            
            article['content'] = self.clean_text(content_elem.get_text()) if content_elem else ''
            
            # 提取参考链接
            reference_links = self.extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # 描述
            desc_elem = soup.find('meta', attrs={'name': 'description'})
            if not desc_elem:
                desc_elem = soup.find('meta', attrs={'property': 'og:description'})
            if desc_elem:
                article['description'] = desc_elem.get('content', '')[:500]
            else:
                article['description'] = article['content'][:200]
            
            # 作者
            author_elem = soup.find(['span', 'div', 'a'], class_=lambda x: x and 'author' in str(x).lower())
            if not author_elem:
                author_elem = soup.find('meta', attrs={'name': 'author'})
                article['author'] = author_elem.get('content', self.company_name) if author_elem else self.company_name
            else:
                article['author'] = self.clean_text(author_elem.get_text()) or self.company_name
            
            # 发布时间
            time_elem = soup.find('time')
            if not time_elem:
                time_elem = soup.find(['span', 'div'], class_=lambda x: x and any(
                    t in str(x).lower() for t in ['time', 'date', 'publish']
                ))
            
            publish_ts = None
            if time_elem:
                time_str = time_elem.get('datetime', '') or time_elem.get_text(strip=True)
                publish_ts = self.parse_timestamp(time_str) if time_str else None
            else:
                # 尝试从meta标签获取
                time_meta = soup.find('meta', attrs={'property': 'article:published_time'})
                if time_meta:
                    time_str = time_meta.get('content', '')
                    publish_ts = self.parse_timestamp(time_str) if time_str else None
            
            if publish_ts is None:
                logger.warning(f"Skip article {article_id}: missing/invalid publish time.")
                return None
            
            article['publish_time'] = publish_ts
            article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
            
            # 分类
            cat_elem = soup.find(['span', 'a'], class_=lambda x: x and 'categor' in str(x).lower())
            article['category'] = self.clean_text(cat_elem.get_text()) if cat_elem else 'AI资讯'
            
            # 标签
            tags = []
            for tag_elem in soup.find_all(['a', 'span'], class_=lambda x: x and 'tag' in str(x).lower()):
                tag_text = self.clean_text(tag_elem.get_text())
                if tag_text and len(tag_text) < 50:
                    tags.append(tag_text)
            article['tags'] = json.dumps(tags, ensure_ascii=False) if tags else ''
            
            # 封面图片
            img_elem = soup.find('meta', attrs={'property': 'og:image'})
            if img_elem:
                article['cover_image'] = img_elem.get('content', '')
            else:
                img_elem = soup.find('img')
                article['cover_image'] = img_elem.get('src', '') if img_elem else ''
            
            # 其他字段
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


if __name__ == "__main__":
    async def test():
        scraper = GenericBlogScraper("https://www.heygen.com/blog", "heygen")
        await scraper.init()
        
        articles = await scraper.get_article_list()
        logger.info(f"Found {len(articles)} articles")
        
        if articles:
            detail = await scraper.get_article_detail(articles[0]['article_id'], articles[0]['url'])
            if detail:
                logger.info(f"Article: {detail['title']}")
        
        await scraper.close()
    
    asyncio.run(test())

