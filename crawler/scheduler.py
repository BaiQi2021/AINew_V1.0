#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一的爬虫调度器 (Unified Crawler Scheduler)
支持并发执行、增量更新和动态爬虫加载
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Optional
from sqlalchemy import select, func

from crawler import utils
from crawler.crawler_registry import get_global_registry, CrawlerType
from crawler.constants import SCHEDULER_CONFIG
from database.models import CompanyArticle, QbitaiArticle
from database.db_session import get_session

logger = utils.setup_logger()


class IncrementalUpdateManager:
    """增量更新管理器"""
    
    @staticmethod
    async def get_latest_article_time(company: str = None) -> datetime:
        """获取某公司最新文章的时间"""
        try:
            async with get_session() as session:
                if company:
                    stmt = select(func.max(CompanyArticle.publish_time)).where(
                        CompanyArticle.company == company
                    )
                else:
                    stmt = select(func.max(CompanyArticle.publish_time))
                
                result = await session.execute(stmt)
                max_timestamp = result.scalar()
                
                if max_timestamp:
                    return datetime.fromtimestamp(max_timestamp)
                else:
                    # 如果没有数据，返回7天前
                    return datetime.now() - timedelta(days=7)
        except Exception as e:
            logger.error(f"Failed to get latest article time: {e}")
            return datetime.now() - timedelta(days=7)
    
    @staticmethod
    async def get_latest_news_time(source: str = None) -> datetime:
        """获取某新闻源最新文章的时间"""
        try:
            async with get_session() as session:
                if source:
                    # 根据article_id前缀判断来源
                    stmt = select(func.max(QbitaiArticle.publish_time)).where(
                        QbitaiArticle.article_id.like(f"{source}_%")
                    )
                else:
                    stmt = select(func.max(QbitaiArticle.publish_time))
                
                result = await session.execute(stmt)
                max_timestamp = result.scalar()
                
                if max_timestamp:
                    return datetime.fromtimestamp(max_timestamp)
                else:
                    return datetime.now() - timedelta(days=7)
        except Exception as e:
            logger.error(f"Failed to get latest news time: {e}")
            return datetime.now() - timedelta(days=7)
    
    @staticmethod
    async def should_crawl(source: str, source_type: str = 'company', threshold_hours: int = 1) -> bool:
        """
        判断是否需要爬取（基于最后更新时间）
        
        Args:
            source: 数据源标识
            source_type: 数据源类型 (company/news/tools)
            threshold_hours: 阈值小时数
        """
        try:
            if source_type == 'company':
                latest_time = await IncrementalUpdateManager.get_latest_article_time(source)
            else:
                latest_time = await IncrementalUpdateManager.get_latest_news_time(source)
            
            # 如果最新文章超过阈值，则需要爬取
            time_diff = datetime.now() - latest_time
            should_crawl = time_diff > timedelta(hours=threshold_hours)
            
            if should_crawl:
                logger.info(f"Source {source} needs crawling (last update: {time_diff.total_seconds()/3600:.1f}h ago)")
            else:
                logger.info(f"Source {source} is up to date (last update: {time_diff.total_seconds()/60:.1f}m ago)")
            
            return should_crawl
        except Exception as e:
            logger.error(f"Error checking if should crawl {source}: {e}")
            return True  # 出错时默认爬取


class CrawlerScheduler:
    """爬虫调度器"""
    
    def __init__(
        self,
        days: int = 7,
        max_concurrent: int = None,
        use_incremental: bool = None,
        crawler_delay: int = None,
    ):
        """
        Args:
            days: 爬取天数
            max_concurrent: 最大并发数（默认从配置读取）
            use_incremental: 是否使用增量更新（默认从配置读取）
            crawler_delay: 爬虫之间的延迟（秒）（默认从配置读取）
        """
        self.days = days
        self.max_concurrent = max_concurrent or SCHEDULER_CONFIG['max_concurrent']
        self.use_incremental = use_incremental if use_incremental is not None else SCHEDULER_CONFIG['use_incremental']
        self.crawler_delay = crawler_delay or SCHEDULER_CONFIG['crawler_delay']
        
        self.results = {
            'total_crawlers': 0,
            'success_crawlers': 0,
            'failed_crawlers': 0,
            'skipped_crawlers': 0,
            'crawlers': []
        }
        
        self.incremental_manager = IncrementalUpdateManager()
        self.registry = get_global_registry()
    
    async def run_crawler_with_tracking(
        self,
        crawler_key: str,
        crawler_name: str,
        crawler_runner: Callable,
        crawler_type: str = 'company'
    ):
        """运行单个爬虫并跟踪结果"""
        self.results['total_crawlers'] += 1
        
        try:
            # 增量更新检查
            if self.use_incremental:
                threshold_hours = SCHEDULER_CONFIG['incremental_threshold'] / 3600
                should_crawl = await self.incremental_manager.should_crawl(
                    crawler_key, 
                    crawler_type,
                    threshold_hours=threshold_hours
                )
                if not should_crawl:
                    logger.info(f"⏭️  Skipping {crawler_name} (up to date)")
                    self.results['skipped_crawlers'] += 1
                    self.results['crawlers'].append({
                        'name': crawler_name,
                        'key': crawler_key,
                        'status': 'skipped',
                        'reason': 'up_to_date'
                    })
                    return
            
            logger.info(f"🎯 Starting {crawler_name}...")
            start_time = datetime.now()
            
            # 运行爬虫
            await crawler_runner(days=self.days)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.results['success_crawlers'] += 1
            self.results['crawlers'].append({
                'name': crawler_name,
                'key': crawler_key,
                'status': 'success',
                'duration': duration
            })
            
            logger.info(f"✅ {crawler_name} completed in {duration:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ {crawler_name} failed: {e}")
            self.results['failed_crawlers'] += 1
            self.results['crawlers'].append({
                'name': crawler_name,
                'key': crawler_key,
                'status': 'failed',
                'error': str(e)
            })
    
    async def run_crawlers_by_type(self, crawler_type: CrawlerType):
        """并发运行指定类型的爬虫"""
        crawlers = self.registry.get_crawlers_by_type(crawler_type, enabled_only=True)
        
        logger.info("=" * 80)
        logger.info(f"📊 Running {len(crawlers)} {crawler_type.value} crawlers (max concurrent: {self.max_concurrent})")
        logger.info("=" * 80)
        
        # 创建任务
        tasks = []
        for crawler_info in crawlers:
            crawler_key = crawler_info['key']
            crawler_name = crawler_info['name']
            
            # 获取runner函数
            runner_func = self.registry.get_crawler_runner(crawler_key)
            if not runner_func:
                logger.warning(f"No runner function found for {crawler_key}, skipping")
                continue
            
            task = self.run_crawler_with_tracking(
                crawler_key,
                crawler_name,
                runner_func,
                crawler_type.value
            )
            tasks.append(task)
        
        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def run_with_semaphore(task):
            async with semaphore:
                await task
                await asyncio.sleep(self.crawler_delay)  # 爬虫间延迟
        
        # 并发执行
        if tasks:
            await asyncio.gather(*[run_with_semaphore(task) for task in tasks])
        else:
            logger.warning(f"No enabled crawlers found for type: {crawler_type.value}")
    
    async def run_all(self):
        """运行所有爬虫"""
        logger.info("🚀" * 40)
        logger.info("   AI REPORT - UNIFIED CRAWLER SCHEDULER")
        logger.info("🚀" * 40)
        logger.info(f"📅 Date Range: Last {self.days} days")
        logger.info(f"⚡ Max Concurrent: {self.max_concurrent}")
        logger.info(f"🔄 Incremental Update: {'Enabled' if self.use_incremental else 'Disabled'}")
        logger.info(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("")
        
        overall_start = datetime.now()
        
        # 运行所有类型的爬虫
        for crawler_type in CrawlerType:
            await self.run_crawlers_by_type(crawler_type)
        
        overall_end = datetime.now()
        overall_duration = (overall_end - overall_start).total_seconds()
        
        # 打印总结
        self.print_summary(overall_duration)
        
        return self.results
    
    def print_summary(self, total_duration: float):
        """打印执行摘要"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 CRAWLER EXECUTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Crawlers: {self.results['total_crawlers']}")
        logger.info(f"✅ Success: {self.results['success_crawlers']}")
        logger.info(f"⏭️  Skipped: {self.results['skipped_crawlers']}")
        logger.info(f"❌ Failed: {self.results['failed_crawlers']}")
        logger.info(f"⏱️  Total Duration: {total_duration:.2f}s ({total_duration/60:.2f} minutes)")
        
        if self.use_incremental and self.results['skipped_crawlers'] > 0:
            time_saved = self.results['skipped_crawlers'] * 30  # 假设每个爬虫平均30秒
            logger.info(f"⚡ Time Saved (Incremental): ~{time_saved}s")
        
        logger.info("")
        
        if self.results['crawlers']:
            logger.info("Crawler Details:")
            logger.info("-" * 80)
            for crawler in self.results['crawlers']:
                status_icon = {
                    'success': '✅',
                    'failed': '❌',
                    'skipped': '⏭️'
                }.get(crawler['status'], '❓')
                
                if crawler['status'] == 'success':
                    logger.info(f"{status_icon} {crawler['name']:25} | Duration: {crawler.get('duration', 0):.2f}s")
                elif crawler['status'] == 'skipped':
                    logger.info(f"{status_icon} {crawler['name']:25} | Reason: {crawler.get('reason', 'unknown')}")
                else:
                    logger.info(f"{status_icon} {crawler['name']:25} | Error: {crawler.get('error', 'Unknown')[:50]}")
        
        logger.info("=" * 80)
        logger.info(f"⏰ End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("🎉 All crawlers completed!")
        logger.info("=" * 80)


async def run_all_crawlers(
    days: int = 7,
    max_concurrent: int = None,
    use_incremental: bool = None
) -> Dict:
    """
    运行所有爬虫的便捷函数
    
    Args:
        days: 爬取天数
        max_concurrent: 最大并发数
        use_incremental: 是否使用增量更新
        
    Returns:
        执行结果字典
    """
    scheduler = CrawlerScheduler(
        days=days,
        max_concurrent=max_concurrent,
        use_incremental=use_incremental
    )
    results = await scheduler.run_all()
    return results


if __name__ == "__main__":
    # 运行示例
    asyncio.run(run_all_crawlers(days=7, max_concurrent=3, use_incremental=True))

