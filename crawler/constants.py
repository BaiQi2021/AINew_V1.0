#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawler Constants
爬虫配置常量
"""

from typing import Dict, List

# 爬虫默认配置
DEFAULT_CRAWLER_CONFIG = {
    'days': 7,
    'max_articles_per_source': 20,
    'request_delay': 2,
    'timeout': 30,
    'retry_times': 3,
    'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# 调度器配置
SCHEDULER_CONFIG = {
    'max_concurrent': 3,  # 最大并发数
    'use_incremental': True,  # 是否使用增量更新
    'crawler_delay': 2,  # 爬虫之间的延迟（秒）
    'incremental_threshold': 3600,  # 增量更新阈值（秒，1小时）
}

# 数据库表映射
DB_TABLE_MAP = {
    'company': 'company_article',  # 公司文章表
    'news': 'qbitai_article',  # 新闻文章表
    'tools': 'qbitai_article',  # AI工具文章表
    'aibase': 'aibase_article',  # AIbase文章表
    'baai_hub': 'baai_hub_article',  # BAAI Hub文章表
}

# 爬虫配置信息（用于注册）
CRAWLER_CONFIGS: List[Dict] = [
    # AI公司爬虫
    {
        'key': 'anthropic',
        'name': 'Anthropic',
        'module': 'crawler.anthropic_scraper',
        'class': 'AnthropicScraper',
        'runner': 'run_anthropic_crawler',
        'type': 'company',
        'enabled': True,
        'priority': 1,
        'description': 'Anthropic官方新闻和研究',
        'db_table': 'company_article',
    },
    {
        'key': 'google_deepmind',
        'name': 'Google DeepMind',
        'module': 'crawler.google_ai_scraper',
        'class': 'GoogleAIScraper',
        'runner': 'run_google_ai_crawler',
        'type': 'company',
        'enabled': True,
        'priority': 1,
        'description': 'Google DeepMind官方博客',
        'db_table': 'company_article',
    },
    {
        'key': 'meta',
        'name': 'Meta AI',
        'module': 'crawler.meta_microsoft_scraper',
        'class': 'MetaAIScraper',
        'runner': 'run_meta_microsoft_crawler',
        'type': 'company',
        'enabled': True,
        'priority': 1,
        'description': 'Meta AI研究论文和博客',
        'db_table': 'company_article',
    },
    {
        'key': 'nvidia',
        'name': 'NVIDIA',
        'module': 'crawler.ai_companies_scraper',
        'class': 'NVIDIAScraper',
        'runner': 'run_nvidia_crawler',
        'type': 'company',
        'enabled': True,
        'priority': 2,
        'description': 'NVIDIA AI新闻',
        'db_table': 'company_article',
    },
    {
        'key': 'openai',
        'name': 'OpenAI',
        'module': 'crawler.openai_scraper',
        'class': 'OpenAIScraper',
        'runner': 'run_openai_crawler',
        'type': 'company',
        'enabled': True,
        'priority': 1,
        'description': 'OpenAI官方新闻',
        'db_table': 'company_article',
    },
    
    # 新闻媒体爬虫
    {
        'key': 'aibase',
        'name': 'AIbase',
        'module': 'crawler.aibase_scraper',
        'class': 'AibaseWebScraper',
        'runner': 'run_crawler',
        'type': 'news',
        'enabled': True,
        'priority': 1,
        'description': 'AIbase AI Daily News',
        'db_table': 'aibase_article',
    },
    {
        'key': 'baai_hub',
        'name': 'BAAI Hub',
        'module': 'crawler.baai_hub_scraper',
        'class': 'BaaiHubScraper',
        'runner': 'run_crawler',
        'type': 'news',
        'enabled': True,
        'priority': 1,
        'description': 'Beijing Academy of Artificial Intelligence Hub',
        'db_table': 'baai_hub_article',
    },
    {
        'key': 'qbitai',
        'name': '量子位',
        'module': 'crawler.qbitai_scraper',
        'class': 'QbitaiWebScraper',
        'runner': 'run_crawler',
        'type': 'news',
        'enabled': True,
        'priority': 1,
        'description': '国内主要AI科技新闻媒体',
        'db_table': 'qbitai_article',
    },
    
    # AI工具爬虫（示例配置，可扩展）
    {
        'key': 'ai_tools',
        'name': 'AI Tools',
        'module': 'crawler.ai_tools_scraper',
        'class': 'AIToolsScraper',
        'runner': None,
        'type': 'tools',
        'enabled': False,  # 默认禁用
        'priority': 3,
        'description': 'AI工具博客聚合',
        'db_table': 'qbitai_article',
    },
]

# HTTP请求头
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}

