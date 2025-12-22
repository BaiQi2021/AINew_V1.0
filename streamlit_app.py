import streamlit as st
import asyncio
import pandas as pd
import json
from datetime import datetime, timedelta
import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from crawler.scheduler import run_all_crawlers, CrawlerScheduler
from crawler import get_global_registry, CrawlerType
from analysis.gemini_agent import GeminiAIReportAgent
from database.db_session import init_db, get_session
from database.models import QbitaiArticle, CompanyArticle, AibaseArticle, BaaiHubArticle
from sqlalchemy import select, func, desc

# Page Config
st.set_page_config(
    page_title="AI小报",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for "Natural and Beautiful" look
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stButton>button {
        width: 100%;
        border-radius: 20px;
        height: 3em;
        background-color: #4CAF50;
        color: white;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        text-align: center;
    }
    h1, h2, h3 {
        color: #2c3e50;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Session State
if 'report_content' not in st.session_state:
    st.session_state.report_content = None
if 'logs' not in st.session_state:
    st.session_state.logs = []

# Helper Functions
async def run_crawler_task(crawler_options, days, max_concurrent):
    await init_db()
    
    registry = get_global_registry()
    
    # If "all" is selected or passed
    if "all" in crawler_options:
         await run_all_crawlers(days=days, max_concurrent=max_concurrent, use_incremental=True)
         return

    # Run selected crawlers
    progress_text = "Operation in progress. Please wait."
    my_bar = st.progress(0, text=progress_text)
    total = len(crawler_options)
    
    for i, crawler_key in enumerate(crawler_options):
        my_bar.progress((i / total), text=f"Running crawler: {crawler_key}")
        
        runner = registry.get_crawler_runner(crawler_key)
        if runner:
            try:
                await runner(days=days)
            except Exception as e:
                st.error(f"Error running {crawler_key}: {e}")
        else:
            st.error(f"Could not load crawler runner: {crawler_key}")
            
    my_bar.empty()

async def get_article_count_in_range(days):
    await init_db()
    cutoff_time = int((datetime.now() - timedelta(days=days)).timestamp())
    
    async with get_session() as session:
        q_count = await session.scalar(select(func.count(QbitaiArticle.id)).where(QbitaiArticle.publish_time >= cutoff_time))
        c_count = await session.scalar(select(func.count(CompanyArticle.id)).where(CompanyArticle.publish_time >= cutoff_time))
        a_count = await session.scalar(select(func.count(AibaseArticle.id)).where(AibaseArticle.publish_time >= cutoff_time))
        b_count = await session.scalar(select(func.count(BaaiHubArticle.id)).where(BaaiHubArticle.publish_time >= cutoff_time))
        
        return q_count + c_count + a_count + b_count

async def get_db_stats():
    await init_db()
    async with get_session() as session:
        # Count articles
        qbitai_count = await session.scalar(select(func.count(QbitaiArticle.id)))
        company_count = await session.scalar(select(func.count(CompanyArticle.id)))
        aibase_count = await session.scalar(select(func.count(AibaseArticle.id)))
        baai_count = await session.scalar(select(func.count(BaaiHubArticle.id)))
        
        return {
            "QbitAI": qbitai_count,
            "Company Blogs": company_count,
            "Aibase": aibase_count,
            "BAAI Hub": baai_count,
            "Total": qbitai_count + company_count + aibase_count + baai_count
        }

def update_data_preview(container, items, stage):
    """Helper to update the data preview section dynamically"""
    if not items:
        return
        
    with container.container():
        st.subheader(f"📊 数据预览: {stage}")
        
        data = []
        for item in items:
            row = {
                "Title": item.title,
                "Source": item.source,
                "Date": datetime.fromtimestamp(item.publish_time).strftime('%Y-%m-%d'),
                "Link": item.url
            }
            
            if stage == "Filtering":
                row["Decision"] = item.filter_decision
                row["Reason"] = item.filter_reason
            elif stage == "Clustering":
                row["Event ID"] = item.event_id
            elif stage == "Deduplication":
                row["Event ID"] = item.event_id
                row["Decision"] = item.dedup_decision
                row["Reason"] = item.dedup_reason
            elif stage == "Ranking":
                row["Score"] = f"{item.final_score:.2f}" if item.final_score else "0.00"
                row["Tech"] = item.tech_impact
                row["Ind"] = item.industry_scope
                row["Hype"] = item.hype_score
            
            data.append(row)
        
        df = pd.DataFrame(data)
        
        # Configure columns based on stage
        column_config = {
            "Link": st.column_config.LinkColumn("Link"),
        }
        
        if stage == "Ranking":
            column_config["Score"] = st.column_config.NumberColumn("Score", format="%.2f")
            
        st.dataframe(
            df,
            column_config=column_config,
            use_container_width=True,
            hide_index=True
        )

async def generate_report_step_by_step(days, report_count, custom_instructions=""):
    await init_db()
    agent = GeminiAIReportAgent()
    
    status_container = st.status("正在生成报告...", expanded=True)
    preview_placeholder = st.empty()
    
    with status_container:
        st.write("📥 正在从数据库获取数据...")
        news_items = await agent.fetch_articles_from_db(days=days)
        if not news_items:
            st.error("未找到数据！")
            return None
        st.info(f"✅ 获取到 {len(news_items)} 条原始数据")
        
        # Update Preview: Raw Data
        update_data_preview(preview_placeholder, news_items, "Raw Data")
        
        # Visualization: Raw Data Distribution
        sources = [item.source for item in news_items]
        source_counts = pd.Series(sources).value_counts()
        st.bar_chart(source_counts)

        st.write("🔍 正在进行智能过滤 (Filtering)...")
        filtered_items = await agent.step1_filter(news_items)
        st.info(f"✅ 过滤后剩余: {len(filtered_items)} 条 (剔除 {len(news_items) - len(filtered_items)} 条)")
        
        # Update Preview: Filtered Data
        update_data_preview(preview_placeholder, news_items, "Filtering") # Show all items to see what was filtered
        
        st.write("🧩 正在进行归类 (Clustering)...")
        clustered_items = await agent.step2_cluster(filtered_items)
        st.info(f"✅ 归类完成")

        # Update Preview: Clustered Data
        update_data_preview(preview_placeholder, clustered_items, "Clustering")

        st.write("🧹 正在进行去重 (Deduplication)...")
        deduped_items = await agent.step3_deduplicate(clustered_items)
        st.info(f"✅ 去重后剩余: {len(deduped_items)} 条")

        # Update Preview: Deduplicated Data
        update_data_preview(preview_placeholder, clustered_items, "Deduplication") # Show clustered items to see what was deduped

        st.write("🏆 正在进行评分排序 (Ranking)...")
        ranked_items = await agent.step4_rank(deduped_items)
        st.info(f"✅ 排序完成")
        
        # Update Preview: Ranked Data
        update_data_preview(preview_placeholder, ranked_items, "Ranking")
        
        # Visualization: Funnel
        funnel_data = {
            "Stage": ["Raw", "Filtered", "Deduplicated"],
            "Count": [len(news_items), len(filtered_items), len(deduped_items)]
        }
        st.dataframe(pd.DataFrame(funnel_data))

        st.write("📄 正在获取 arXiv 论文...")
        arxiv_papers = await agent.step5_fetch_arxiv_papers(ranked_items)
        st.info(f"✅ 获取到 {len(arxiv_papers)} 篇相关论文")

        st.write("✍️ 正在撰写最终报告...")
        report = await agent.generate_final_report(ranked_items, arxiv_papers=arxiv_papers, days=days, target_count=report_count, custom_instructions=custom_instructions)
        
        status_container.update(label="报告生成完成！", state="complete", expanded=False)
        return report

# Sidebar
st.sidebar.title("⚙️ 控制面板")

st.sidebar.subheader("1. 数据采集设置")
days_lookback = st.sidebar.slider("回溯天数 (Days)", 1, 30, 3)

# Specific list of crawlers as requested
target_crawlers = {
    "Anthropic": "anthropic",
    "OpenAI": "openai",
    "Meta AI": "meta",
    "NVIDIA": "nvidia",
    "Google DeepMind": "google_deepmind",
    "BAAI Hub": "baai_hub",
    "量子位": "qbitai",
    "AIbase": "aibase"
}

selected_crawlers_labels = st.sidebar.multiselect(
    "选择爬虫 (Select Crawlers)",
    options=list(target_crawlers.keys()),
    default=list(target_crawlers.keys())
)

selected_crawler_keys = [target_crawlers[label] for label in selected_crawlers_labels]

if st.sidebar.button("🚀 开始采集 (Start Crawling)"):
    if not selected_crawler_keys:
        st.sidebar.error("请至少选择一个爬虫！")
    else:
        with st.spinner(f"正在运行爬虫..."):
            asyncio.run(run_crawler_task(selected_crawler_keys, days_lookback, 3))
        st.sidebar.success("采集完成！")

st.sidebar.markdown("---")

st.sidebar.subheader("2. 报告生成设置")

# Get available article count
try:
    available_count = asyncio.run(get_article_count_in_range(days_lookback))
    st.sidebar.caption(f"📅 过去 {days_lookback} 天内共有 {available_count} 篇文章")
    max_report_count = min(50, max(5, available_count))
except Exception:
    available_count = 0
    max_report_count = 50

report_count = st.sidebar.number_input(
    "报告条目数量", 
    min_value=1, 
    max_value=max_report_count, 
    value=min(10, max_report_count),
    help=f"基于当前数据量，建议不超过 {available_count} 条"
)

template_file = st.sidebar.file_uploader("上传报告模版/指令 (可选)", type=["md", "txt"])
custom_instructions = ""
if template_file:
    custom_instructions = template_file.read().decode("utf-8")

if st.sidebar.button("✨ 生成报告 (Generate Report)"):
    report = asyncio.run(generate_report_step_by_step(days_lookback, report_count, custom_instructions))
    if report:
        st.session_state.report_content = report

st.sidebar.markdown("---")
st.sidebar.info("Designed for AIReport Project")

# Main Content
col1, col2, col3, col4, col5 = st.columns(5)

# Load stats
try:
    stats = asyncio.run(get_db_stats())
    col1.metric("总文章数", stats["Total"])
    col2.metric("QbitAI", stats["QbitAI"])
    col3.metric("公司博客", stats["Company Blogs"])
    col4.metric("Aibase", stats["Aibase"])
    col5.metric("BAAI Hub", stats["BAAI Hub"])
except Exception as e:
    st.error(f"无法连接数据库: {e}")

st.markdown("---")

# Report Display
if st.session_state.report_content:
    st.subheader("📝 生成的报告 (Generated Report)")
    
    tab1, tab2 = st.tabs(["预览 (Preview)", "源码 (Source)"])
    
    with tab1:
        st.markdown(st.session_state.report_content)
    
    with tab2:
        st.code(st.session_state.report_content, language="markdown")
    
    # Download Button
    st.download_button(
        label="📥 下载报告 (Download Markdown)",
        data=st.session_state.report_content,
        file_name=f"AI_Report_{datetime.now().strftime('%Y-%m-%d')}.md",
        mime="text/markdown"
    )
else:
    st.info("👈 请在左侧侧边栏点击 '生成报告' 按钮开始。")
    
    # Show recent data preview if no report
    st.subheader("📊 最近采集的数据预览")
    
    async def get_recent_articles():
        async with get_session() as session:
            # Fetch a few from each table
            q_stmt = select(QbitaiArticle.title, QbitaiArticle.publish_date, QbitaiArticle.article_url).order_by(desc(QbitaiArticle.publish_time)).limit(5)
            c_stmt = select(CompanyArticle.title, CompanyArticle.publish_date, CompanyArticle.article_url).order_by(desc(CompanyArticle.publish_time)).limit(5)
            b_stmt = select(BaaiHubArticle.title, BaaiHubArticle.publish_date, BaaiHubArticle.article_url).order_by(desc(BaaiHubArticle.publish_time)).limit(5)
            
            q_res = await session.execute(q_stmt)
            c_res = await session.execute(c_stmt)
            b_res = await session.execute(b_stmt)
            
            data = []
            for row in q_res:
                data.append({"Title": row.title, "Date": row.publish_date, "Source": "QbitAI", "URL": row.article_url})
            for row in c_res:
                data.append({"Title": row.title, "Date": row.publish_date, "Source": "Company Blog", "URL": row.article_url})
            for row in b_res:
                data.append({"Title": row.title, "Date": row.publish_date, "Source": "BAAI Hub", "URL": row.article_url})
                
            return pd.DataFrame(data)

    try:
        df = asyncio.run(get_recent_articles())
        if not df.empty:
            st.dataframe(
                df,
                column_config={
                    "URL": st.column_config.LinkColumn("Link")
                },
                use_container_width=True
            )
        else:
            st.write("暂无数据，请先进行采集。")
    except Exception as e:
        st.error(f"加载预览数据失败: {e}")
