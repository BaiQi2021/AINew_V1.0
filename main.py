#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIReport Main Entry Point
1. Crawl AI company websites and news sites
2. Generate Weekly/Daily Report
"""

import asyncio
import argparse
from crawler.scheduler import run_all_crawlers, CrawlerScheduler
from crawler import get_global_registry, CrawlerType
from analysis.gemini_agent import GeminiAIReportAgent
from crawler import utils
from database.db_session import init_db

logger = utils.setup_logger()

async def main():
    parser = argparse.ArgumentParser(description="AIReport Crawler & Generator")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back for data")
    parser.add_argument("--skip-crawl", action="store_true", help="Skip crawling, only generate report")
    parser.add_argument("--skip-report", action="store_true", help="Skip report generation, only crawl")
    parser.add_argument("--crawler", type=str, default="all", 
                        help="Specify crawler: all, qbitai, openai, anthropic, google, china, meta, microsoft, news, company")
    parser.add_argument("--concurrent", action="store_true", help="Enable concurrent crawling (faster)")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Maximum concurrent crawlers (default: 3)")
    parser.add_argument("--no-incremental", action="store_true", help="Disable incremental updates")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy pool")
    parser.add_argument("--use-agent", action="store_true", help="Deprecated: Agent is now enabled by default")
    parser.add_argument("--save-intermediate", action="store_true", help="Save intermediate results during agent processing")
    parser.add_argument("--report-count", type=int, default=10, help="Number of report entries (quick view items and report titles, default: 10)")
    
    args = parser.parse_args()

    # Initialize Database
    logger.info("Initializing Database...")
    await init_db()
    
    # Initialize Proxy Pool if enabled
    if args.use_proxy:
        logger.info("Proxy support enabled (configure proxies in proxy_pool.py)")
        # TODO: Load proxies from config file or API

    if not args.skip_crawl:
        logger.info("Starting Crawler Phase...")
        
        # 使用统一的调度器（支持并发和增量更新）
        if args.crawler == "all":
            # 并发模式默认启用
            max_concurrent = args.max_concurrent if args.concurrent else 3
            logger.info(f"🚀 Running all crawlers (max concurrent: {max_concurrent})")
            await run_all_crawlers(
                days=args.days,
                max_concurrent=max_concurrent,
                use_incremental=not args.no_incremental
            )
        else:
            # 运行单个爬虫
            await run_single_crawler(args.crawler, args.days)
    
    if not args.skip_report:
        logger.info("Starting Analysis Phase...")
        
        # 使用智能 Agent 生成报告
        logger.info("🤖 使用 GeminiAIReportAgent 进行智能分析...")
        try:
            agent = GeminiAIReportAgent(max_retries=2)
            await agent.run(days=args.days, save_intermediate=args.save_intermediate, report_count=args.report_count)
        except Exception as e:
            logger.error(f"Agent 运行失败: {e}")


async def run_single_crawler(crawler_name: str, days: int):
    """运行单个爬虫（使用动态加载）"""
    registry = get_global_registry()
    
    # 特殊处理：按类型运行
    if crawler_name == "company":
        scheduler = CrawlerScheduler(days=days)
        await scheduler.run_crawlers_by_type(CrawlerType.COMPANY)
        return
    elif crawler_name == "news":
        scheduler = CrawlerScheduler(days=days)
        await scheduler.run_crawlers_by_type(CrawlerType.NEWS)
        return
    elif crawler_name == "tools":
        scheduler = CrawlerScheduler(days=days)
        await scheduler.run_crawlers_by_type(CrawlerType.TOOLS)
        return
    
    # 运行指定的单个爬虫
    crawler_info = registry.get_crawler(crawler_name)
    if not crawler_info:
        logger.error(f"Unknown crawler: {crawler_name}")
        logger.info("Available crawlers:")
        registry.list_crawlers()
        return
    
    # 动态加载并运行爬虫
    runner_func = registry.get_crawler_runner(crawler_name)
    if runner_func:
        logger.info(f"Running crawler: {crawler_info['name']}")
        await runner_func(days=days)
    else:
        logger.error(f"No runner function found for crawler: {crawler_name}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")

