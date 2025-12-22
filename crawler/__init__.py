#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawler Module
爬虫模块统一接口
"""

from crawler.base_scraper import BaseWebScraper
from crawler.crawler_registry import CrawlerRegistry, CrawlerType, get_global_registry
from crawler.proxy_pool import ProxyPool, get_global_proxy_pool, init_proxy_pool
from crawler.utils import setup_logger, get_current_timestamp

__all__ = [
    # 基础类
    'BaseWebScraper',
    
    # 注册中心
    'CrawlerRegistry',
    'CrawlerType',
    'get_global_registry',
    
    # 代理池
    'ProxyPool',
    'get_global_proxy_pool',
    'init_proxy_pool',
    
    # 工具函数
    'setup_logger',
    'get_current_timestamp',
]

__version__ = '1.0.0'

