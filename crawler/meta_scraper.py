#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Meta AI Scraper
爬取 Meta AI 官网的研究和博客
"""

import asyncio
import json
import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from crawler.base_scraper import BaseWebScraper
from crawler.openai_scraper import save_company_article_to_db
from crawler import utils

logger = utils.setup_logger()


class MetaAIScraper(BaseWebScraper):
    """Meta AI官网爬虫"""
    
    def __init__(self):
        super().__init__(
            base_url="https://ai.meta.com",
            company_name="meta"
        )
        self.blog_url = "https://ai.meta.com/blog/"
        self.research_url = "https://ai.meta.com/research/"
    
    async def get_article_list(self, page: int = 1, article_type: str = 'blog') -> List[Dict]:
        """获取文章列表"""
        try:
            if article_type == 'research':
                url = self.research_url
            else:
                url = self.blog_url
            
            logger.info(f"Fetching Meta AI {article_type} list from {url}...")
            
            html = await self.fetch_page(url)
            if not html:
                return []
            
            soup = BeautifulSoup(html, 'html.parser')
            articles = []
            
            article_elements = soup.find_all(['article', 'div'], class_=lambda x: x and any(keyword in str(x).lower() for keyword in ['post', 'card', 'item', 'article']))
            
            if not article_elements:
                article_elements = soup.select('a[href*="/blog/"], a[href*="/research/"]')
            
            logger.info(f"Found {len(article_elements)} potential article elements")
            
            for elem in article_elements[:30]:
                try:
                    if elem.name == 'a':
                        link_elem = elem
                    else:
                        link_elem = elem.find('a', href=True)
                    
                    if not link_elem:
                        continue
                    
                    url = link_elem.get('href', '')
                    if not url:
                        continue
                    
                    # 过滤掉非文章页面
                    if url.rstrip('/') in [self.base_url, self.blog_url.rstrip('/'), self.research_url.rstrip('/'), 
                                         'https://ai.meta.com/about', 'https://ai.meta.com/meta-ai', 
                                         '/about', '/research', '/meta-ai']:
                        continue
                    
                    if url.startswith('/'):
                        url = self.base_url + url
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
                    
                    if '/research/' in url:
                        determined_type = 'research'
                    else:
                        determined_type = 'blog'
                    
                    articles.append({
                        'article_id': f"meta_{article_id}",
                        'title': title[:500],
                        'url': url,
                        'article_type': determined_type,
                    })
                    
                except Exception as e:
                    logger.warning(f"Failed to parse article element: {e}")
                    continue
            
            logger.info(f"Extracted {len(articles)} Meta AI articles")
            return articles
        
        except Exception as e:
            logger.error(f"Failed to get Meta AI article list: {e}")
            return []
    
    async def get_article_detail(self, article_id: str, url: str) -> Optional[Dict]:
        """获取文章详情"""
        try:
            logger.info(f"Fetching Meta AI article details: {article_id}")
            
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
            
            # 内容
            content_elem = soup.find('article')
            if not content_elem:
                content_elem = soup.find('main')
            if not content_elem:
                content_elem = soup.find(['div'], class_=lambda x: x and ('content' in str(x).lower() or 'article' in str(x).lower()))
            
            article['content'] = self.clean_text(content_elem.get_text()) if content_elem else ''
            
            # 提取参考链接
            reference_links = self.extract_reference_links(soup, content_elem)
            article['reference_links'] = json.dumps(reference_links, ensure_ascii=False) if reference_links else ''
            
            # 描述
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
                article['author'] = author_elem.get('content', '') if author_elem else 'Meta AI'
            else:
                article['author'] = self.clean_text(author_elem.get_text())
            
            # 发布时间 (使用 BaseWebScraper 增强版逻辑)
            time_str = self.find_publish_time_string(soup, content_elem)
            
            if not time_str:
                logger.warning(f"Skip article {article_id}: missing publish time.")
                return None
                
            publish_ts = self.parse_timestamp(time_str)
            if publish_ts is None:
                logger.warning(f"Skip article {article_id}: cannot parse publish time: {time_str}")
                return None
            article['publish_time'] = publish_ts
            article['publish_date'] = datetime.fromtimestamp(publish_ts).strftime('%Y-%m-%d')
            
            # 分类
            article['category'] = 'AI Research' if '/research/' in url else 'AI Blog'
            
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
            article['is_product'] = 1 if any(keyword in article['title'].lower() for keyword in ['llama', 'pytorch', 'release', 'launch', 'announce']) else 0
            
            return article
        
        except Exception as e:
            logger.error(f"Failed to get Meta AI article details {article_id}: {e}")
            return None


async def run_meta_microsoft_crawler(days: int = 7):
    """运行 Meta 爬虫"""
    logger.info("=" * 60)
    logger.info(f"🚀 Meta AI Crawler Started (Filter: last {days} days)")
    logger.info("=" * 60)
    
    # Meta AI
    meta_scraper = MetaAIScraper()
    await meta_scraper.init()
    
    try:
        # Meta AI Blog
        logger.info("Fetching Meta AI blog articles...")
        blog_articles = await meta_scraper.get_article_list(article_type='blog')
        
        for article_item in blog_articles[:15]:
            try:
                article = await meta_scraper.get_article_detail(
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
                logger.error(f"Error processing Meta AI blog article: {e}")
                continue
        
        # Meta AI Research
        logger.info("Fetching Meta AI research articles...")
        research_articles = await meta_scraper.get_article_list(article_type='research')
        
        for article_item in research_articles[:15]:
            try:
                article = await meta_scraper.get_article_detail(
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
                logger.error(f"Error processing Meta AI research article: {e}")
                continue
        
    finally:
        await meta_scraper.close()
        logger.info("Meta AI Crawler finished.")


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_meta_microsoft_crawler())
