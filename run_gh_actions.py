#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Actions Entry Point
1. Import data from JSON
2. Run crawlers for last 2 days
3. Generate report
4. Send to Feishu
5. Export data to JSON
"""

import asyncio
import os
import logging
from datetime import datetime
from database.json_bridge import import_from_json, export_to_json
from crawler.scheduler import run_all_crawlers
from analysis.gemini_agent import GeminiAIReportAgent
from analysis.feishu_sender import FeishuSender
import config
from crawler import utils

# Setup logging
logger = utils.setup_logger()
settings = config.settings

async def run_pipeline():
    # 1. Import from JSON
    logger.info("Step 1: Importing data from JSON...")
    await import_from_json("data/db_export.json")

    # 2. Run Crawlers (last 2 days)
    days = 2
    logger.info(f"Step 2: Running crawlers for last {days} days...")
    await run_all_crawlers(days=days, max_concurrent=3, use_incremental=True)

    # 3. Generate Report
    logger.info("Step 3: Generating AI Report...")
    agent = GeminiAIReportAgent(max_retries=3)
    report_content = await agent.run(days=days, save_intermediate=True, report_count=10)

    if not report_content:
        logger.error("Failed to generate report.")
    else:
        # 4. Send to Feishu
        logger.info("Step 4: Sending to Feishu...")
        webhooks = []
        if settings.FEISHU_WEBHOOK_URL:
            webhooks.append(settings.FEISHU_WEBHOOK_URL)
        if settings.FEISHU_WEBHOOK_URLS:
            webhooks.extend([w.strip() for w in settings.FEISHU_WEBHOOK_URLS.split(",") if w.strip()])
        
        if not webhooks:
            logger.warning("No Feishu webhooks configured.")
        else:
            for webhook in webhooks:
                try:
                    sender = FeishuSender(webhook)
                    title = f"AI 前沿动态速报 ({datetime.now().strftime('%Y-%m-%d')})"
                    await sender.send_markdown(title, report_content)
                    logger.info(f"Sent to Feishu: {webhook[:30]}...")
                except Exception as e:
                    logger.error(f"Failed to send to Feishu {webhook[:30]}: {e}")

    # 5. Export to JSON
    logger.info("Step 5: Exporting data to JSON...")
    await export_to_json("data/db_export.json")
    logger.info("Pipeline completed.")

if __name__ == "__main__":
    # Set DB to SQLite for GH Actions if not specified
    if os.environ.get("GITHUB_ACTIONS") == "true":
        os.environ["DB_DIALECT"] = "sqlite"
        os.environ["DB_NAME"] = "ai_report_gh"
    
    asyncio.run(run_pipeline())
