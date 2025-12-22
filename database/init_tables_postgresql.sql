-- ===============================
-- AIReport 数据库表结构 (PostgreSQL版本)
-- 量子位(QbitAI)爬虫相关表
-- ===============================

-- ----------------------------
-- Table structure for qbitai_article
-- 量子位文章表：存储从量子位网站爬取的AI相关文章
-- ----------------------------
DROP TABLE IF EXISTS qbitai_article CASCADE;

CREATE TABLE qbitai_article (
    id SERIAL PRIMARY KEY,
    article_id VARCHAR(255) NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    content TEXT,
    article_url TEXT NOT NULL,
    author VARCHAR(255),
    publish_time BIGINT,
    publish_date VARCHAR(10),
    read_count INTEGER DEFAULT 0,
    like_count INTEGER DEFAULT 0,
    comment_count INTEGER DEFAULT 0,
    share_count INTEGER DEFAULT 0,
    collect_count INTEGER DEFAULT 0,
    category VARCHAR(100),
    tags TEXT,
    cover_image VARCHAR(512),
    source_keyword VARCHAR(255) DEFAULT '',
    is_original INTEGER DEFAULT 1,
    reference_links TEXT,
    add_ts BIGINT NOT NULL,
    last_modify_ts BIGINT NOT NULL
);

-- 创建唯一索引
CREATE UNIQUE INDEX idx_qbitai_article_unique ON qbitai_article(article_id);
CREATE INDEX idx_qbitai_article_date ON qbitai_article(publish_date);
CREATE INDEX idx_qbitai_article_time ON qbitai_article(publish_time);

-- 表注释
COMMENT ON TABLE qbitai_article IS '量子位文章表';
COMMENT ON COLUMN qbitai_article.id IS '自增ID';
COMMENT ON COLUMN qbitai_article.article_id IS '文章唯一ID';
COMMENT ON COLUMN qbitai_article.title IS '文章标题';
COMMENT ON COLUMN qbitai_article.description IS '文章描述/摘要';
COMMENT ON COLUMN qbitai_article.content IS '文章内容';
COMMENT ON COLUMN qbitai_article.article_url IS '文章链接';
COMMENT ON COLUMN qbitai_article.author IS '文章作者';
COMMENT ON COLUMN qbitai_article.publish_time IS '发布时间戳';
COMMENT ON COLUMN qbitai_article.publish_date IS '发布日期(YYYY-MM-DD)';
COMMENT ON COLUMN qbitai_article.read_count IS '阅读数';
COMMENT ON COLUMN qbitai_article.like_count IS '点赞数';
COMMENT ON COLUMN qbitai_article.comment_count IS '评论数';
COMMENT ON COLUMN qbitai_article.share_count IS '分享数';
COMMENT ON COLUMN qbitai_article.collect_count IS '收藏数';
COMMENT ON COLUMN qbitai_article.category IS '文章分类';
COMMENT ON COLUMN qbitai_article.tags IS '文章标签(JSON格式)';
COMMENT ON COLUMN qbitai_article.cover_image IS '文章封面图片URL';
COMMENT ON COLUMN qbitai_article.source_keyword IS '来源关键词';
COMMENT ON COLUMN qbitai_article.is_original IS '是否原创(1-是, 0-否)';
COMMENT ON COLUMN qbitai_article.reference_links IS '文章中的参考链接(JSON格式)';
COMMENT ON COLUMN qbitai_article.add_ts IS '记录添加时间戳';
COMMENT ON COLUMN qbitai_article.last_modify_ts IS '记录最后修改时间戳';

-- ===============================
-- 索引优化
-- ===============================
-- 为常用查询优化添加复合索引
CREATE INDEX idx_article_date_category ON qbitai_article(publish_date, category);

-- ===============================
-- 数据库配置说明
-- ===============================
-- 
-- 本SQL文件支持PostgreSQL数据库。
-- 
-- 使用方法：
-- 1. 确保已创建数据库（如: CREATE DATABASE ai_report ENCODING 'UTF8';）
-- 2. 连接数据库：\c ai_report;
-- 3. 执行本SQL文件：\i /path/to/init_tables_postgresql.sql;
--
-- 或使用命令行：
-- psql -U postgres -d ai_report -f database/init_tables_postgresql.sql
--
-- 注意：
-- - 表结构与 SQLAlchemy 模型 (database/models.py) 保持一致
-- - 使用 SERIAL 类型作为自增主键
-- - 外键约束确保数据完整性
-- 

