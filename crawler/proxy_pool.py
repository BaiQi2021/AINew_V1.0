#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Proxy Pool Module
代理池模块，支持HTTP/HTTPS代理轮换
"""

import asyncio
import random
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from crawler import utils

logger = utils.setup_logger()


class ProxyPool:
    """代理池管理"""
    
    def __init__(self, proxies: List[str] = None):
        """
        初始化代理池
        Args:
            proxies: 代理列表，格式如 ["http://ip:port", "socks5://ip:port"]
        """
        self.proxies = proxies or []
        self.failed_proxies = {}  # 失败的代理及其失败时间
        self.retry_after = 300  # 失败后多久可以重试（秒）
    
    def add_proxy(self, proxy: str):
        """添加代理"""
        if proxy not in self.proxies:
            self.proxies.append(proxy)
            logger.info(f"Added proxy: {proxy}")
    
    def add_proxies(self, proxies: List[str]):
        """批量添加代理"""
        for proxy in proxies:
            self.add_proxy(proxy)
    
    def remove_proxy(self, proxy: str):
        """移除代理"""
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            logger.info(f"Removed proxy: {proxy}")
    
    def mark_failed(self, proxy: str):
        """标记代理失败"""
        self.failed_proxies[proxy] = datetime.now()
        logger.warning(f"Marked proxy as failed: {proxy}")
    
    def is_available(self, proxy: str) -> bool:
        """检查代理是否可用"""
        if proxy not in self.failed_proxies:
            return True
        
        failed_time = self.failed_proxies[proxy]
        if datetime.now() - failed_time > timedelta(seconds=self.retry_after):
            # 超过重试时间，从失败列表中移除
            del self.failed_proxies[proxy]
            return True
        
        return False
    
    def get_proxy(self) -> Optional[str]:
        """获取一个可用的代理"""
        available_proxies = [p for p in self.proxies if self.is_available(p)]
        
        if not available_proxies:
            logger.warning("No available proxies in pool")
            return None
        
        return random.choice(available_proxies)
    
    def get_proxy_dict(self) -> Optional[Dict[str, str]]:
        """获取代理配置字典（用于httpx）"""
        proxy = self.get_proxy()
        if not proxy:
            return None
        
        return {
            "http://": proxy,
            "https://": proxy,
        }
    
    def size(self) -> int:
        """获取代理池大小"""
        return len(self.proxies)
    
    def available_size(self) -> int:
        """获取可用代理数量"""
        return len([p for p in self.proxies if self.is_available(p)])


# 全局代理池实例
_global_proxy_pool = None


def get_global_proxy_pool() -> ProxyPool:
    """获取全局代理池"""
    global _global_proxy_pool
    if _global_proxy_pool is None:
        _global_proxy_pool = ProxyPool()
    return _global_proxy_pool


def init_proxy_pool(proxies: List[str]):
    """初始化全局代理池"""
    global _global_proxy_pool
    _global_proxy_pool = ProxyPool(proxies)
    logger.info(f"Initialized proxy pool with {len(proxies)} proxies")


# 示例：从文件加载代理
async def load_proxies_from_file(filepath: str) -> List[str]:
    """从文件加载代理列表"""
    try:
        with open(filepath, 'r') as f:
            proxies = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logger.info(f"Loaded {len(proxies)} proxies from {filepath}")
        return proxies
    except Exception as e:
        logger.error(f"Failed to load proxies from file: {e}")
        return []


# 示例：从API获取代理
async def fetch_proxies_from_api(api_url: str) -> List[str]:
    """从API获取代理列表"""
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            response = await client.get(api_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # 假设API返回格式为 {"proxies": ["ip:port", ...]}
            proxies = data.get('proxies', [])
            # 转换为完整格式
            proxies = [f"http://{p}" if not p.startswith('http') else p for p in proxies]
            
            logger.info(f"Fetched {len(proxies)} proxies from API")
            return proxies
    except Exception as e:
        logger.error(f"Failed to fetch proxies from API: {e}")
        return []


# 代理测试
async def test_proxy(proxy: str, test_url: str = "https://www.google.com") -> bool:
    """测试代理是否可用"""
    try:
        import httpx
        proxies = {
            "http://": proxy,
            "https://": proxy,
        }
        async with httpx.AsyncClient(proxies=proxies, timeout=10) as client:
            response = await client.get(test_url)
            return response.status_code == 200
    except Exception as e:
        logger.debug(f"Proxy {proxy} test failed: {e}")
        return False


async def test_all_proxies(proxy_pool: ProxyPool, test_url: str = "https://www.google.com"):
    """测试代理池中的所有代理"""
    logger.info(f"Testing {proxy_pool.size()} proxies...")
    
    tasks = [test_proxy(proxy, test_url) for proxy in proxy_pool.proxies]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    for proxy, result in zip(proxy_pool.proxies, results):
        if isinstance(result, bool) and result:
            logger.info(f"✅ Proxy {proxy} is working")
        else:
            proxy_pool.mark_failed(proxy)
            logger.warning(f"❌ Proxy {proxy} failed")
    
    logger.info(f"Available proxies: {proxy_pool.available_size()}/{proxy_pool.size()}")


if __name__ == "__main__":
    # 使用示例
    async def example():
        # 创建代理池
        proxies = [
            "http://proxy1.example.com:8080",
            "http://proxy2.example.com:8080",
            "socks5://proxy3.example.com:1080",
        ]
        
        pool = ProxyPool(proxies)
        
        # 测试代理
        await test_all_proxies(pool)
        
        # 获取可用代理
        proxy = pool.get_proxy()
        logger.info(f"Got proxy: {proxy}")
        
        # 标记失败
        if proxy:
            pool.mark_failed(proxy)
        
        # 再次获取
        proxy = pool.get_proxy()
        logger.info(f"Got another proxy: {proxy}")
    
    asyncio.run(example())

