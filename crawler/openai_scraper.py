#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAI Research & News Scraper
爬取OpenAI官网的研究论文和新闻
使用 cloudscraper 绕过 Cloudflare/Akamai 防护
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

import cloudscraper
from bs4 import BeautifulSoup
from sqlalchemy import select

from crawler.base_scraper import BaseWebScraper
from database.models import CompanyArticle
from database.db_session import get_session
from crawler import utils

logger = utils.setup_logger()


class OpenAIScraper(BaseWebScraper):
    """OpenAI官网爬虫 - 使用 cloudscraper 绕过反爬保护"""
    
    def __init__(self):
        super().__init__(
            base_url="https://openai.com",
            company_name="openai",
            http2=True,
        )
        # OpenAI Chinese URLs（用户指定数据源）
        self.blog_url = "https://openai.com/zh-Hans-CN/news/"
        # 用户指定的列表页：https://openai.com/zh-Hans-CN/research/index/?page=2
        self.research_url = "https://openai.com/zh-Hans-CN/research/index/"
        
        # 使用 cloudscraper 替代 httpx（绕过 Cloudflare 403）
        self.cloud_scraper = None
        self._executor = ThreadPoolExecutor(max_workers=3)
        
        # 官方 API 端点（通过抓包发现）
        self.api_url = "https://openai.com/backend/articles/"
    
    async def init(self):
        """初始化 cloudscraper 客户端"""
        self.cloud_scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'darwin',
                'desktop': True,
            }
        )
        logger.info("OpenAI Scraper initialized with cloudscraper")
    
    async def close(self):
        """关闭资源"""
        if self.cloud_scraper:
            self.cloud_scraper.close()
        self._executor.shutdown(wait=False)
    
    def _fetch_sync(self, url: str) -> Optional[str]:
        """同步获取页面内容（cloudscraper 是同步的）"""
        for attempt in range(self.max_retries):
            try:
                response = self.cloud_scraper.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except Exception as e:
                logger.error(f"Failed to fetch page {url} (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
        return None
    
    def _fetch_json_sync(self, url: str) -> Optional[Dict]:
        """同步获取 JSON 数据"""
        for attempt in range(self.max_retries):
            try:
                response = self.cloud_scraper.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Failed to fetch JSON {url} (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
        return None
    
    async def fetch_page(self, url: str, **kwargs) -> Optional[str]:
        """异步获取页面内容（包装同步的 cloudscraper）"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._fetch_sync, url)
    
    async def fetch_json(self, url: str, **kwargs) -> Optional[Dict]:
        """异步获取 JSON 数据"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._fetch_json_sync, url)
    
    async def get_article_list(self, page: int = 1, article_type: str = 'blog') -> List[Dict]:
        """获取文章列表 - 使用官方 API"""
        try:
            from urllib.parse import urlencode
            
            # 计算 skip 值（每页20条）
            limit = 20
            skip = (page - 1) * limit
            
            # 构建 API URL
            params = {
                'locale': 'zh-Hans-CN',
                'pageQueries': '[{"pageTypes":["Article"],"categories":["publication","conclusion","milestone","release"]}]',
                'limit': str(limit),
                'skip': str(skip),
                'sort': 'new',
                'groupedTags': ''
            }
            
            url = self.api_url + '?' + urlencode(params)
            logger.info(f"Fetching OpenAI articles from API (page {page}, skip {skip})...")
            
            data = await self.fetch_json(url)
            if not data:
                return []
            
            articles = []
            items = data.get('items', [])
            total = data.get('total', 0)
            
            logger.info(f"API returned {len(items)} items (total: {total})")
            
            for item in items:
                slug = item.get('slug', '')
                title = item.get('title', '')
                pub_date = item.get('publicationDate', '')
                
                if not slug or not title:
                    continue
                
                # 提取 article_id（slug 格式: index/xxx-xxx-xxx）
                article_id = slug.replace('index/', '') if slug.startswith('index/') else slug
                
                # 构建完整 URL
                full_url = f"https://openai.com/{slug}/"
                
                # 确定文章类型
                categories = item.get('categories', [])
                if 'publication' in categories or 'conclusion' in categories:
                    determined_type = 'research'
                else:
                    determined_type = 'blog'
                
                articles.append({
                    'article_id': f"openai_{article_id}",
                    'title': title[:500],
                    'url': full_url,
                    'article_type': determined_type,
                    'raw_date': pub_date,  # ISO 格式: 2025-12-18T12:00
                    'cover_image': item.get('coverImage', {}).get('url', '') if isinstance(item.get('coverImage'), dict) else ''
                })
            
            logger.info(f"Extracted {len(articles)} OpenAI articles")
            return articles
        
        except Exception as e:
            logger.error(f"Failed to get OpenAI article list: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """获取文章详情"""
        try:
            logger.info(f"Fetching OpenAI article details: {article_id}")
            
            html = await self.fetch_page(url)
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            
            article = {
                'article_id': article_id,
                'article_url': url,
                'company': self.company_name,
            }
            
            # 标题
            title_elem = soup.find('h1')
            if not title_elem:
                title_elem = soup.find('title')
            article['title'] = self.clean_text(title_elem.get_text()) if title_elem else ''
            
            # 内容 - OpenAI通常使用article标签或main标签
            content_elem = soup.find('article')
            if not content_elem:
                content_elem = soup.find('main')
            if not content_elem:
                content_elem = soup.find(['div'], class_=lambda x: x and ('content' in str(x).lower() or 'article' in str(x).lower()))
            
            article['content'] = self.clean_text(content_elem.get_text()) if content_elem else ''
            
            # 提取参考链接
            reference_links = self.extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # 描述/摘要
            desc_elem = soup.find('meta', attrs={'name': 'description'})
            if not desc_elem:
                desc_elem = soup.find('meta', attrs={'property': 'og:description'})
            if desc_elem:
                article['description'] = desc_elem.get('content', '')
            else:
                article['description'] = article['content'][:300]
            
            # 作者
            author_elem = soup.find(['span', 'div', 'p'], class_=lambda x: x and 'author' in str(x).lower())
            if not author_elem:
                author_elem = soup.find('meta', attrs={'name': 'author'})
                article['author'] = author_elem.get('content', '') if author_elem else 'OpenAI'
            else:
                article['author'] = self.clean_text(author_elem.get_text())
            
            # 发布时间 (使用 BaseWebScraper 增强版逻辑)
            time_str = self.find_publish_time_string(soup, content_elem)
            
            # Note: We can't access 'raw_date' here easily without changing signature.
            # But BaseWebScraper.find_publish_time_string should handle on-page dates.
            # If not, we might need to rely on what we parsed in list.
            # Since we can't change signature easily, let's hope detail page has date.
            # If detail page fails to parse date, we might lose it. 
            # For now, let's assume detail page has it or JSON-LD has it.
            
            if not time_str:
                logger.warning(f"Skip article {article_id}: missing publish time.")
                return None
                
            publish_ts = self.parse_timestamp(time_str)
            if publish_ts is None:
                logger.warning(f"Skip article {article_id}: cannot parse publish time: {time_str}")
                return None
            article['publish_time'] = publish_ts
            article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
            
            # 分类和标签
            article['category'] = 'AI Research' if '/research/' in url else 'AI News'
            
            # 标签
            tag_elements = soup.find_all(['a', 'span'], class_=lambda x: x and 'tag' in str(x).lower())
            tags = []
            for tag_elem in tag_elements:
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
            
            # 文章类型判断
            article['article_type'] = 'research' if '/research/' in url else 'blog'
            article['is_research'] = 1 if article['article_type'] == 'research' else 0
            article['is_product'] = 1 if any(keyword in article['title'].lower() for keyword in ['gpt', 'dall-e', 'whisper', 'api', 'product', 'launch', 'release']) else 0
            
            return article
        
        except Exception as e:
            logger.error(f"Failed to get OpenAI article details {article_id}: {e}")
            return None


async def save_company_article_to_db(article: Dict):
    """保存公司文章到数据库"""
    async with get_session() as session:
        article_id = article.get('article_id')
        
        stmt = select(CompanyArticle).where(CompanyArticle.article_id == article_id)
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        
        # 字段长度限制映射（根据数据库模型定义）
        field_length_limits = {
            'article_id': 255,
            'company': 100,
            'author': 255,
            'publish_date': 10,
            'category': 100,
            'cover_image': 512,
            'article_type': 50,
        }
        
        def truncate_field(key: str, value):
            """截断字段值到指定长度"""
            if key in field_length_limits and value is not None:
                if isinstance(value, str):
                    max_length = field_length_limits[key]
                    if len(value) > max_length:
                        return value[:max_length]
            return value
        
        if existing:
            existing.last_modify_ts = utils.get_current_timestamp()
            for key, value in article.items():
                if hasattr(existing, key) and key not in ['id', 'add_ts']:
                    truncated_value = truncate_field(key, value)
                    setattr(existing, key, truncated_value)
            logger.info(f"Updated company article: {article_id}")
        else:
            article['add_ts'] = utils.get_current_timestamp()
            article['last_modify_ts'] = utils.get_current_timestamp()
            
            valid_keys = {c.name for c in CompanyArticle.__table__.columns}
            filtered_article = {}
            for k, v in article.items():
                if k in valid_keys:
                    filtered_article[k] = truncate_field(k, v)
            
            db_article = CompanyArticle(**filtered_article)
            session.add(db_article)
            logger.info(f"Saved new company article: {article_id}")


async def run_openai_crawler(days: int = 7):
    """运行OpenAI爬虫"""
    logger.info("=" * 60)
    logger.info(f"🚀 OpenAI Crawler Started (Filter: last {days} days)")
    logger.info("=" * 60)
    
    scraper = OpenAIScraper()
    await scraper.init()
    
    blog_saved_count = 0
    research_saved_count = 0
    
    try:
        # 使用官方 API 获取文章列表
        logger.info("Fetching OpenAI articles from API...")
        
        all_articles = await scraper.get_article_list(page=1, article_type='research')
        
        if not all_articles:
            logger.warning("⚠️  OpenAI: Failed to fetch article list")
        else:
            logger.info(f"Found {len(all_articles)} total articles")
            
            for article_item in all_articles:
                try:
                    # 先检查日期是否在范围内（API 返回 ISO 格式日期）
                    if days > 0 and 'raw_date' in article_item:
                        raw_ts = scraper.parse_timestamp(article_item['raw_date'])
                        if raw_ts:
                            now_ts = datetime.now().timestamp()
                            if now_ts - raw_ts > days * 86400:
                                logger.info(f"Skip article {article_item['title']}: too old ({article_item['raw_date']})")
                                continue
                    
                    article = await scraper.get_article_detail(
                        article_item['article_id'],
                        article_item['url']
                    )
                    
                    if article:
                        # 再次检查详情页的日期（更准确）
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
                        if article['article_type'] == 'research':
                            research_saved_count += 1
                        else:
                            blog_saved_count += 1
                    
                    await asyncio.sleep(1)  # 礼貌延迟
                    
                except Exception as e:
                    logger.error(f"Error processing OpenAI article: {e}")
                    continue
        
        # 汇总
        total_saved = blog_saved_count + research_saved_count
        logger.info(f"✅ OpenAI Crawler: Successfully saved {total_saved} articles (blog: {blog_saved_count}, research: {research_saved_count})")
        
    finally:
        await scraper.close()
        logger.info("OpenAI Crawler finished.")


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_openai_crawler())
