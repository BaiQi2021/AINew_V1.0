#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AIReport 数据库初始化脚本
使用 SQLAlchemy 2.x 异步引擎创建数据库表
支持 MySQL 和 PostgreSQL
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus
from loguru import logger

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text

# 添加项目根目录到路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# 导入配置
from config import settings

# 导入数据库模型
from database.models import Base, QbitaiArticle, QbitaiArticleComment, BaaiHubArticle


def _build_database_url() -> str:
    """
    根据配置构建数据库连接 URL
    支持 MySQL 和 PostgreSQL
    """
    dialect = (settings.DB_DIALECT or "mysql").lower()
    host = settings.DB_HOST or "localhost"
    port = str(settings.DB_PORT or ("3306" if dialect == "mysql" else "5432"))
    user = settings.DB_USER or "root"
    password = quote_plus(settings.DB_PASSWORD or "")
    db_name = settings.DB_NAME or "ai_report"

    if dialect in ("postgresql", "postgres"):
        # PostgreSQL 使用 asyncpg 驱动
        logger.info(f"使用 PostgreSQL 数据库: {db_name}")
        return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}"
    else:
        # MySQL 使用 aiomysql 驱动
        logger.info(f"使用 MySQL 数据库: {db_name}")
        return f"mysql+aiomysql://{user}:{password}@{host}:{port}/{db_name}"


async def check_database_connection(engine: AsyncEngine) -> bool:
    """
    检查数据库连接是否正常
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        logger.info("✅ 数据库连接成功")
        return True
    except Exception as e:
        logger.error(f"❌ 数据库连接失败: {e}")
        return False


async def create_tables(engine: AsyncEngine) -> bool:
    """
    创建所有数据库表
    """
    try:
        logger.info("开始创建数据库表...")
        
        async with engine.begin() as conn:
            # 使用 SQLAlchemy 的 metadata.create_all 创建所有表
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("✅ 数据库表创建成功")
        logger.info(f"   已创建表: {', '.join([table.name for table in Base.metadata.sorted_tables])}")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表失败: {e}")
        return False


async def verify_tables(engine: AsyncEngine) -> bool:
    """
    验证所有表是否已创建
    """
    try:
        expected_tables = [table.name for table in Base.metadata.sorted_tables]
        logger.info(f"验证数据库表: {', '.join(expected_tables)}")
        
        async with engine.connect() as conn:
            # 根据数据库类型执行不同的查询
            dialect = engine.url.get_backend_name()
            
            if dialect == "postgresql":
                result = await conn.execute(
                    text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                )
            else:  # MySQL
                result = await conn.execute(
                    text(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{settings.DB_NAME}'")
                )
            
            existing_tables = [row[0] for row in result]
            
            missing_tables = set(expected_tables) - set(existing_tables)
            
            if missing_tables:
                logger.warning(f"⚠️  缺少表: {', '.join(missing_tables)}")
                return False
            else:
                logger.info("✅ 所有表已存在并验证通过")
                return True
                
    except Exception as e:
        logger.error(f"❌ 验证数据库表失败: {e}")
        return False


async def main() -> None:
    """
    主函数：初始化数据库
    """
    logger.info("=" * 60)
    logger.info("AIReport 数据库初始化")
    logger.info("=" * 60)
    
    try:
        # 1. 构建数据库连接 URL
        database_url = _build_database_url()
        logger.info(f"数据库类型: {settings.DB_DIALECT}")
        logger.info(f"数据库主机: {settings.DB_HOST}:{settings.DB_PORT}")
        logger.info(f"数据库名称: {settings.DB_NAME}")
        
        # 2. 创建异步引擎
        engine = create_async_engine(
            database_url,
            pool_pre_ping=True,  # 连接前检查连接是否有效
            pool_recycle=1800,   # 30分钟后回收连接
            echo=False            # 不输出 SQL 语句（调试时可设为 True）
        )
        
        # 3. 检查数据库连接
        if not await check_database_connection(engine):
            logger.error("数据库连接失败，请检查配置")
            await engine.dispose()
            sys.exit(1)
        
        # 4. 创建数据库表
        if not await create_tables(engine):
            logger.error("创建数据库表失败")
            await engine.dispose()
            sys.exit(1)
        
        # 5. 验证数据库表
        if not await verify_tables(engine):
            logger.warning("数据库表验证未通过，但表已创建")
        
        # 6. 清理资源
        await engine.dispose()
        
        logger.info("=" * 60)
        logger.info("✅ 数据库初始化完成！")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.exception(f"❌ 数据库初始化失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

