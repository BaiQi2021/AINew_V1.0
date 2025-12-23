import asyncio
import argparse
import os
from datetime import datetime
from analysis.feishu_sender import FeishuSender

async def main():
    parser = argparse.ArgumentParser(description="Test Feishu Sender")
    parser.add_argument("webhook_url", nargs="?", help="Feishu Webhook URL")
    parser.add_argument("--file", default="final_reports/AI_Report_2025-12-23_174829.md", help="Path to the report file")
    parser.add_argument("--title", help="Custom title for the report")
    args = parser.parse_args()

    webhook_url = args.webhook_url or os.environ.get("FEISHU_WEBHOOK_URL")
    
    if not webhook_url:
        print("Error: Webhook URL not provided. Pass it as an argument or set FEISHU_WEBHOOK_URL env var.")
        return

    if not os.path.exists(args.file):
        print(f"Error: File {args.file} not found.")
        return

    print(f"Reading report from {args.file}...")
    with open(args.file, "r", encoding="utf-8") as f:
        report_content = f.read()

    print(f"Initializing FeishuSender with URL: {webhook_url}")
    sender = FeishuSender(webhook_url)

    if "flow/api/trigger-webhook" in webhook_url:
        print("Detected Feishu Flow Webhook.")
        # Calculate parameters for Flow
        title_count = report_content.count("## ") 
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Assuming it's a daily report for this test
        report_type = "Daily Report" 
        
        # Use custom title if provided, otherwise generate a short one
        if args.title:
            doc_title = args.title
        else:
            # Shorten title to avoid "Title character count exceeds limit" error
            doc_title = f"AI News {datetime.now().strftime('%Y-%m-%d')}"

        print(f"Sending to Flow: {doc_title}")
        print(f"Note: If you get 'Title character count exceeds limit', check your Feishu Flow configuration.")
        print(f"Ensure you are mapping the 'title' field (not 'text') to the Document Title.")
        
        await sender.send_to_flow(
            title=doc_title,
            total_titles=str(title_count),
            timestamp=timestamp,
            report_type=report_type,
            text=report_content
        )
    else:
        print("Detected Standard Webhook.")
        title = f"AI 前沿动态速报 ({datetime.now().strftime('%Y-%m-%d')})"
        print(f"Sending Markdown: {title}")
        await sender.send_markdown(title, report_content)

    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
