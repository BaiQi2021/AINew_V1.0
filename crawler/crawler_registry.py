#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬虫注册中心 (Crawler Registry)
统一管理和注册所有爬虫，提供统一的访问接口
"""

import importlib
from typing import Dict, List, Type, Optional, Callable, Any
from enum import Enum

from crawler import utils
from crawler.constants import CRAWLER_CONFIGS

logger = utils.setup_logger()


class CrawlerType(Enum):
    """爬虫类型枚举"""
    COMPANY = "company"  # AI公司官网
    NEWS = "news"  # 新闻媒体
    TOOLS = "tools"  # AI工具博客


class CrawlerRegistry:
    """爬虫注册中心"""
    
    def __init__(self):
        self._crawlers: Dict[str, Dict] = {}
    
    def register(
        self, 
        key: str, 
        name: str, 
        crawler_class: Type = None,
        crawler_type: CrawlerType = CrawlerType.COMPANY,
        enabled: bool = True,
        priority: int = 5,
        description: str = "",
        db_table: str = "company_article",
        module_path: str = None,
        class_name: str = None,
        runner_name: str = None,
    ):
        """
        注册爬虫
        
        Args:
            key: 爬虫唯一标识
            name: 爬虫显示名称
            crawler_class: 爬虫类（可选，如果提供module_path和class_name则动态导入）
            crawler_type: 爬虫类型
            enabled: 是否启用
            priority: 优先级（1-10，数字越小优先级越高）
            description: 描述信息
            db_table: 数据库表名
            module_path: 模块路径（用于动态导入）
            class_name: 类名（用于动态导入）
            runner_name: runner函数名（用于动态导入）
        """
        self._crawlers[key] = {
            'key': key,
            'name': name,
            'class': crawler_class,
            'type': crawler_type,
            'enabled': enabled,
            'priority': priority,
            'description': description,
            'db_table': db_table,
            'module_path': module_path,
            'class_name': class_name,
            'runner_name': runner_name,
        }
        logger.debug(f"Registered crawler: {name} ({key})")
    
    def get_crawler(self, key: str) -> Optional[Dict]:
        """获取指定的爬虫配置"""
        return self._crawlers.get(key)
    
    def get_all_crawlers(self, enabled_only: bool = True) -> List[Dict]:
        """获取所有爬虫"""
        crawlers = list(self._crawlers.values())
        if enabled_only:
            crawlers = [c for c in crawlers if c.get('enabled', True)]
        # 按优先级排序
        crawlers.sort(key=lambda x: x.get('priority', 999))
        return crawlers
    
    def get_crawlers_by_type(self, crawler_type: CrawlerType, enabled_only: bool = True) -> List[Dict]:
        """根据类型获取爬虫"""
        crawlers = [c for c in self._crawlers.values() if c.get('type') == crawler_type]
        if enabled_only:
            crawlers = [c for c in crawlers if c.get('enabled', True)]
        crawlers.sort(key=lambda x: x.get('priority', 999))
        return crawlers
    
    def get_crawler_class(self, key: str) -> Optional[Type]:
        """获取爬虫类（动态导入）"""
        crawler_info = self.get_crawler(key)
        if not crawler_info:
            logger.error(f"Crawler {key} not found")
            return None
        
        # 如果已经有类对象，直接返回
        if crawler_info.get('class'):
            return crawler_info['class']
        
        # 否则尝试动态导入
        module_path = crawler_info.get('module_path')
        class_name = crawler_info.get('class_name')
        
        if not module_path or not class_name:
            logger.error(f"Crawler {key} missing module_path or class_name")
            return None
        
        try:
            module = importlib.import_module(module_path)
            crawler_class = getattr(module, class_name)
            # 缓存类对象
            crawler_info['class'] = crawler_class
            return crawler_class
        except Exception as e:
            logger.error(f"Failed to import crawler {key} from {module_path}.{class_name}: {e}")
            return None
    
    def get_crawler_runner(self, key: str) -> Optional[Callable]:
        """获取爬虫runner函数（动态导入）"""
        crawler_info = self.get_crawler(key)
        if not crawler_info:
            logger.error(f"Crawler {key} not found")
            return None
        
        runner_name = crawler_info.get('runner_name')
        if not runner_name:
            logger.warning(f"Crawler {key} has no runner function")
            return None
        
        module_path = crawler_info.get('module_path')
        if not module_path:
            logger.error(f"Crawler {key} missing module_path")
            return None
        
        try:
            module = importlib.import_module(module_path)
            runner_func = getattr(module, runner_name)
            return runner_func
        except Exception as e:
            logger.error(f"Failed to import runner {runner_name} from {module_path}: {e}")
            return None
    
    def list_crawlers(self):
        """打印所有已注册的爬虫"""
        logger.info("=" * 80)
        logger.info("📋 已注册的爬虫列表")
        logger.info("=" * 80)
        
        for crawler_type in CrawlerType:
            crawlers = self.get_crawlers_by_type(crawler_type, enabled_only=False)
            if crawlers:
                logger.info(f"\n🔹 {crawler_type.value.upper()} 类型:")
                for c in crawlers:
                    status = "✅" if c.get('enabled') else "❌"
                    logger.info(f"  {status} {c['name']:20} ({c['key']:15}) - Priority: {c['priority']}")
        
        logger.info("\n" + "=" * 80)
        logger.info(f"总计: {len(self._crawlers)} 个爬虫")
        enabled_count = len([c for c in self._crawlers.values() if c.get('enabled')])
        logger.info(f"启用: {enabled_count} 个")
        logger.info("=" * 80)


# 全局爬虫注册中心实例
_global_registry = None


def get_global_registry() -> CrawlerRegistry:
    """获取全局爬虫注册中心实例"""
    global _global_registry
    if _global_registry is None:
        _global_registry = CrawlerRegistry()
        _register_all_crawlers(_global_registry)
    return _global_registry


def _register_all_crawlers(registry: CrawlerRegistry):
    """从配置自动注册所有爬虫"""
    try:
        for config in CRAWLER_CONFIGS:
            # 转换type字符串为枚举
            crawler_type_str = config.get('type', 'company')
            crawler_type = CrawlerType(crawler_type_str)
            
            registry.register(
                key=config['key'],
                name=config['name'],
                crawler_type=crawler_type,
                enabled=config.get('enabled', True),
                priority=config.get('priority', 5),
                description=config.get('description', ''),
                db_table=config.get('db_table', 'company_article'),
                module_path=config.get('module'),
                class_name=config.get('class'),
                runner_name=config.get('runner'),
            )
        
        logger.info(f"✅ 成功注册 {len(registry.get_all_crawlers(enabled_only=False))} 个爬虫")
        
    except Exception as e:
        logger.error(f"注册爬虫失败: {e}")
        raise


if __name__ == "__main__":
    # 测试注册中心
    registry = get_global_registry()
    registry.list_crawlers()
    
    # 测试动态导入
    print("\n测试动态导入:")
    qbitai_class = registry.get_crawler_class('qbitai')
    print(f"Qbitai Class: {qbitai_class}")
    
    qbitai_runner = registry.get_crawler_runner('qbitai')
    print(f"Qbitai Runner: {qbitai_runner}")
