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
        self.is_running = False
        self.current_status = "Idle"

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")

    def get_status(self):
        job = self.scheduler.get_job(self.job_id)
        next_run = job.next_run_time if job else None
        return {
            "is_running": self.is_running,
            "current_status": self.current_status,
            "next_run_time": next_run,
            "webhook_configured": bool(self.feishu_webhook),
            "schedule_time": job.trigger.fields[3].name if job and hasattr(job.trigger, 'fields') else None # This is tricky for CronTrigger
        }

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
        if self.is_running:
            logger.warning("Pipeline already running, skipping...")
            return

        self.is_running = True
        self.current_status = "Starting pipeline..."
        logger.info("Starting scheduled pipeline...")
        try:
            # 1. Init DB
            self.current_status = "Initializing Database..."
            await init_db()
            
            # 2. Run Crawlers
            self.current_status = "Running Crawlers..."
            logger.info("Running crawlers...")
            await run_all_crawlers(days=self.days_to_crawl, max_concurrent=3, use_incremental=True)
            
            # 3. Generate Report
            self.current_status = "Generating Report..."
            logger.info("Generating report...")
            agent = GeminiAIReportAgent()
            report_content = await agent.run(days=self.days_to_crawl) 
            
            # 4. Send to Feishu
            if self.feishu_webhook and report_content:
                self.current_status = "Sending to Feishu..."
                logger.info(f"Sending report to Feishu...")
                sender = FeishuSender(self.feishu_webhook)
                
                if "flow/api/trigger-webhook" in self.feishu_webhook:
                    # Calculate parameters for Flow
                    # Count titles (assuming ## Title format or similar)
                    title_count = report_content.count("## ") 
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    report_type = "Daily Report" if self.days_to_crawl == 1 else f"{self.days_to_crawl}-Day Report"
                    doc_title = f"AI News {report_type} - {datetime.now().strftime('%Y-%m-%d')}"

                    await sender.send_to_flow(
                        title=doc_title,
                        total_titles=str(title_count),
                        timestamp=timestamp,
                        report_type=report_type,
                        text=report_content
                    )
                else:
                    await sender.send_markdown(f"AI 前沿动态速报 ({datetime.now().strftime('%Y-%m-%d')})", report_content)
                
            logger.info("Pipeline completed successfully")
            self.current_status = "Completed"
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.current_status = f"Error: {str(e)}"
            # Try to send error notification
            if self.feishu_webhook:
                sender = FeishuSender(self.feishu_webhook)
                if "flow/api/trigger-webhook" in self.feishu_webhook:
                     await sender.send_to_flow(
                        title=f"AI News Error Report - {datetime.now().strftime('%Y-%m-%d')}",
                        total_titles="0",
                        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        report_type="Error Report",
                        text=f"Pipeline failed: {str(e)}"
                    )
                else:
                    await sender.send_markdown("AI News Report - Error", f"Pipeline failed: {str(e)}")
        finally:
            self.is_running = False
            # Reset status after a delay or keep it? 
            # For now, we keep "Completed" or "Error" until next run, or maybe reset to "Idle"
            # But if we reset immediately, the user might miss it.
            # Let's set to Idle after a short while? No, just leave it or set to Idle.
            # If is_running is False, status implies Idle.
            self.current_status = "Idle"
