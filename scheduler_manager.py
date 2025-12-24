import asyncio
import logging
import os
import json
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
        self.config_file = "scheduler_config.json"
        self.feishu_webhooks = []
        self.schedule_times = []
        self.days_to_crawl = 1
        self.is_running = False
        self.current_status = "Idle"
        self.pipeline_steps = []  # 存储详细步骤供前端展示
        self.load_config()

    def start(self):
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("Scheduler started")
            self._apply_schedule()

    def _add_step(self, content, type="text", label=None):
        """添加进度步骤"""
        self.pipeline_steps.append({
            "type": type,
            "content": content,
            "label": label,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })

    def load_config(self):
        """从文件加载配置"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.feishu_webhooks = config.get("feishu_webhooks", [])
                    # Compatibility with old single webhook
                    if not self.feishu_webhooks and config.get("feishu_webhook"):
                        self.feishu_webhooks = [config.get("feishu_webhook")]
                    
                    self.schedule_times = config.get("schedule_times", [])
                    # Compatibility with old single time
                    if not self.schedule_times and config.get("schedule_time"):
                        self.schedule_times = [config.get("schedule_time")]
                        
                    self.days_to_crawl = config.get("days_to_crawl", 1)
                logger.info("Config loaded from file")
            except Exception as e:
                logger.error(f"Failed to load config: {e}")

    def save_config(self):
        """保存配置到文件"""
        config = {
            "feishu_webhooks": self.feishu_webhooks,
            "schedule_times": self.schedule_times,
            "days_to_crawl": self.days_to_crawl
        }
        try:
            with open(self.config_file, 'w') as f:
                json.dump(config, f, indent=4)
            logger.info("Config saved to file")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")

    def get_status(self):
        jobs = self.scheduler.get_jobs()
        next_run = min([job.next_run_time for job in jobs if job.next_run_time]) if jobs else None
        
        return {
            "is_running": self.is_running,
            "current_status": self.current_status,
            "next_run_time": next_run,
            "webhook_configured": len(self.feishu_webhooks) > 0,
            "feishu_webhooks": self.feishu_webhooks,
            "schedule_times": self.schedule_times,
            "pipeline_steps": self.pipeline_steps
        }

    def stop(self):
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

    def _apply_schedule(self):
        """根据当前配置应用调度任务"""
        # Remove all existing jobs
        for job in self.scheduler.get_jobs():
            self.scheduler.remove_job(job.id)
            
        for i, time_str in enumerate(self.schedule_times):
            if not time_str:
                continue
            try:
                hour, minute = map(int, time_str.split(':'))
                trigger = CronTrigger(hour=hour, minute=minute)
                
                self.scheduler.add_job(
                    self.run_pipeline_sync,
                    trigger=trigger,
                    id=f"daily_report_job_{i}",
                    replace_existing=True
                )
                logger.info(f"Scheduled job {i} set for {time_str}")
            except ValueError:
                logger.error(f"Invalid time format: {time_str}")

    def update_schedule(self, times: list, webhook_urls: list, days: int = 1):
        """
        Update the schedule times and webhook URLs.
        times: List of "HH:MM" format strings
        webhook_urls: List of webhook URLs
        """
        self.feishu_webhooks = [w.strip() for w in webhook_urls if w.strip()]
        self.schedule_times = [t.strip() for t in times if t.strip()]
        self.days_to_crawl = days
        
        self.save_config()
        self._apply_schedule()

    def run_pipeline_sync(self):
        """Synchronous wrapper for the async pipeline"""
        asyncio.run(self.run_pipeline())

    async def run_pipeline(self):
        if self.is_running:
            logger.warning("Pipeline already running, skipping...")
            return

        self.is_running = True
        self.pipeline_steps = []
        self.current_status = "Starting pipeline..."
        self._add_step("🚀 开始执行定时任务流水线...", type="info")
        
        logger.info("Starting scheduled pipeline...")
        try:
            # 1. Init DB
            self.current_status = "Initializing Database..."
            self._add_step("📥 正在初始化数据库...", type="text")
            await init_db()
            
            # 2. Run Crawlers
            self.current_status = "Running Crawlers..."
            self._add_step("🕷️ 正在运行爬虫采集数据...", type="text")
            logger.info("Running crawlers...")
            await run_all_crawlers(days=self.days_to_crawl, max_concurrent=3, use_incremental=True)
            self._add_step("✅ 数据采集完成", type="success")
            
            # 3. Generate Report
            self.current_status = "Generating Report..."
            self._add_step("🤖 正在生成智能报告...", type="text")
            logger.info("Generating report...")
            agent = GeminiAIReportAgent()
            
            # Step-by-step generation to capture intermediate data
            self._add_step("📥 正在从数据库获取数据...", type="text")
            news_items = await agent.fetch_articles_from_db(days=self.days_to_crawl)
            if not news_items:
                self._add_step("❌ 未找到数据！", type="error")
                return
            self._add_step(f"✅ 获取到 {len(news_items)} 条原始数据", type="info")
            
            # Source distribution for chart
            sources = [item.source for item in news_items]
            source_counts = {}
            for s in sources:
                source_counts[s] = source_counts.get(s, 0) + 1
            self._add_step(source_counts, type="chart", label="数据来源分布")

            self._add_step("🔍 正在进行智能过滤 (Filtering)...", type="text")
            filtered_items = await agent.step1_filter(news_items)
            self._add_step(f"✅ 过滤后剩余: {len(filtered_items)} 条 (剔除 {len(news_items) - len(filtered_items)} 条)", type="info")
            
            self._add_step("🧩 正在进行归类 (Clustering)...", type="text")
            clustered_items = await agent.step2_cluster(filtered_items)
            self._add_step("✅ 归类完成", type="info")

            self._add_step("🧹 正在进行去重 (Deduplication)...", type="text")
            deduped_items = await agent.step3_deduplicate(clustered_items)
            self._add_step(f"✅ 去重后剩余: {len(deduped_items)} 条", type="info")

            self._add_step("🏆 正在进行评分排序 (Ranking)...", type="text")
            ranked_items = await agent.step4_rank(deduped_items)
            self._add_step("✅ 排序完成", type="info")
            
            # Funnel data
            funnel_data = {
                "Stage": ["Raw", "Filtered", "Deduplicated"],
                "Count": [len(news_items), len(filtered_items), len(deduped_items)]
            }
            self._add_step(funnel_data, type="dataframe", label="处理漏斗数据")

            self._add_step("📄 正在获取 arXiv 论文...", type="text")
            arxiv_papers = await agent.step5_fetch_arxiv_papers(ranked_items)
            self._add_step(f"✅ 获取到 {len(arxiv_papers)} 篇相关论文", type="info")

            self._add_step("✍️ 正在撰写最终报告...", type="text")
            report_content = await agent.generate_final_report(ranked_items, arxiv_papers=arxiv_papers, days=self.days_to_crawl, target_count=10)
            
            if report_content:
                self._add_step("💾 正在保存报告并更新数据库...", type="text")
                file_path = agent.save_report_to_file(report_content)
                await agent.mark_articles_as_reported(ranked_items, file_path)
                self._add_step(f"✅ 报告已保存至: {file_path}", type="success")
            
            # 4. Send to Feishu
            if self.feishu_webhooks and report_content:
                self.current_status = "Sending to Feishu..."
                self._add_step(f"📤 正在发送至 {len(self.feishu_webhooks)} 个飞书群...", type="text")
                logger.info(f"Sending report to {len(self.feishu_webhooks)} Feishu webhooks...")
                
                for webhook in self.feishu_webhooks:
                    try:
                        sender = FeishuSender(webhook)
                        if "flow/api/trigger-webhook" in webhook:
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
                        logger.info(f"Sent to webhook: {webhook[:20]}...")
                    except Exception as send_error:
                        logger.error(f"Failed to send to webhook {webhook[:20]}: {send_error}")
                        self._add_step(f"⚠️ 发送至某个飞书群失败: {str(send_error)}", type="error")

                self._add_step("✅ 飞书推送完成", type="success")
                
            logger.info("Pipeline completed successfully")
            self.current_status = "Completed"
            self._add_step("🎉 定时任务全部执行完成！", type="success")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.current_status = f"Error: {str(e)}"
            self._add_step(f"❌ 任务失败: {str(e)}", type="error")
            # Try to send error notification to all webhooks
            if self.feishu_webhooks:
                for webhook in self.feishu_webhooks:
                    try:
                        sender = FeishuSender(webhook)
                        if "flow/api/trigger-webhook" in webhook:
                             await sender.send_to_flow(
                                title=f"AI News Error Report - {datetime.now().strftime('%Y-%m-%d')}",
                                total_titles="0",
                                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                report_type="Error Report",
                                text=f"Pipeline failed: {str(e)}"
                            )
                        else:
                            await sender.send_markdown("AI News Report - Error", f"Pipeline failed: {str(e)}")
                    except:
                        pass
        finally:
            self.is_running = False
            # Keep the steps for a while so the UI can show them
            # We don't reset current_status to Idle immediately here so the UI can show "Completed"
            # But we should probably have a way to clear it or it will show forever.
            # For now, let's just leave it.

