import asyncio
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime

from crawler.scheduler import run_all_crawlers
from analysis.gemini_agent import GeminiAIReportAgent
from analysis.feishu_sender import FeishuSender
from database.db_session import init_db

logger = logging.getLogger(__name__)

class SchedulerManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.job_id = "daily_report_job"
        self.feishu_webhook = None
        self.days_to_crawl = 1

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    def update_schedule(self, time_str: str, webhook_url: str, days: int = 1):
        """
        Update the schedule time and webhook URL.
        time_str: "HH:MM" format
        """
        self.feishu_webhook = webhook_url
        self.days_to_crawl = days
        
        # Remove existing job if any
        if self.scheduler.get_job(self.job_id):
            self.scheduler.remove_job(self.job_id)
            
        if not time_str:
            return

        try:
            hour, minute = map(int, time_str.split(':'))
            trigger = CronTrigger(hour=hour, minute=minute)
            
            self.scheduler.add_job(
                self.run_pipeline_sync,
                trigger=trigger,
                id=self.job_id,
                replace_existing=True
            )
            logger.info(f"Scheduled job set for {time_str}")
        except ValueError:
            logger.error(f"Invalid time format: {time_str}")

    def run_pipeline_sync(self):
        """Synchronous wrapper for the async pipeline"""
        asyncio.run(self.run_pipeline())

    async def run_pipeline(self):
        logger.info("Starting scheduled pipeline...")
        try:
            # 1. Init DB
            await init_db()
            
            # 2. Run Crawlers
            logger.info("Running crawlers...")
            await run_all_crawlers(days=self.days_to_crawl, max_concurrent=3, use_incremental=True)
            
            # 3. Generate Report
            logger.info("Generating report...")
            agent = GeminiAIReportAgent()
            report_content = await agent.run(days=self.days_to_crawl) 
            
            # 4. Send to Feishu
            if self.feishu_webhook and report_content:
                logger.info(f"Sending report to Feishu...")
                sender = FeishuSender(self.feishu_webhook)
                await sender.send_markdown(f"AI News Report {datetime.now().strftime('%Y-%m-%d')}", report_content)
                
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            # Try to send error notification
            if self.feishu_webhook:
                sender = FeishuSender(self.feishu_webhook)
                await sender.send_markdown("AI News Report - Error", f"Pipeline failed: {str(e)}")
