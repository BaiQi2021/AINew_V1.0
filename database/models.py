from sqlalchemy import Column, Integer, Text, String, BigInteger
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class QbitaiArticle(Base):
    __tablename__ = 'qbitai_article'
    id = Column(Integer, primary_key=True)
    article_id = Column(String(255), nullable=False, index=True, unique=True)
    title = Column(Text, nullable=False)
    description = Column(Text)
    content = Column(Text)
    article_url = Column(Text, nullable=False)
    author = Column(String(255))
    publish_time = Column(BigInteger, index=True)
    publish_date = Column(String(10), index=True)
    read_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    share_count = Column(Integer, default=0)
    collect_count = Column(Integer, default=0)
    category = Column(String(100))
    tags = Column(Text)
    cover_image = Column(String(512))
    source_keyword = Column(String(255), default='')
    is_original = Column(Integer, default=1)
    reference_links = Column(Text)  # JSON格式存储文章中的参考链接
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)

class CompanyArticle(Base):
    """统一的AI公司官网文章表"""
    __tablename__ = 'company_article'
    id = Column(Integer, primary_key=True)
    article_id = Column(String(255), nullable=False, index=True, unique=True)
    company = Column(String(100), nullable=False, index=True)  # openai, anthropic, google, baidu等
    title = Column(Text, nullable=False)
    description = Column(Text)
    content = Column(Text)
    article_url = Column(Text, nullable=False)
    author = Column(String(255))
    publish_time = Column(BigInteger, index=True)
    publish_date = Column(String(10), index=True)
    category = Column(String(100))
    tags = Column(Text)  # JSON格式
    cover_image = Column(String(512))
    article_type = Column(String(50))  # research, product, news, blog等
    is_research = Column(Integer, default=0)  # 是否为研究论文
    is_product = Column(Integer, default=0)  # 是否为产品发布
    reference_links = Column(Text)  # JSON格式存储参考链接
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)

class AibaseArticle(Base):
    __tablename__ = 'aibase_article'
    id = Column(Integer, primary_key=True)
    article_id = Column(String(255), nullable=False, index=True, unique=True)
    title = Column(Text, nullable=False)
    description = Column(Text)
    content = Column(Text)
    article_url = Column(Text, nullable=False)
    author = Column(String(255))
    publish_time = Column(BigInteger, index=True)
    publish_date = Column(String(10), index=True)
    read_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    share_count = Column(Integer, default=0)
    collect_count = Column(Integer, default=0)
    category = Column(String(100))
    tags = Column(Text)
    cover_image = Column(String(512))
    source_keyword = Column(String(255), default='')
    is_original = Column(Integer, default=1)
    reference_links = Column(Text)  # JSON格式存储文章中的参考链接
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)

class BaaiHubArticle(Base):
    __tablename__ = 'baai_hub_article'
    id = Column(Integer, primary_key=True)
    article_id = Column(String(255), nullable=False, index=True, unique=True)
    title = Column(Text, nullable=False)
    description = Column(Text)
    content = Column(Text)
    article_url = Column(Text, nullable=False)
    author = Column(String(255))
    publish_time = Column(BigInteger, index=True)
    publish_date = Column(String(10), index=True)
    read_count = Column(Integer, default=0)
    like_count = Column(Integer, default=0)
    comment_count = Column(Integer, default=0)
    share_count = Column(Integer, default=0)
    collect_count = Column(Integer, default=0)
    category = Column(String(100))
    tags = Column(Text)
    cover_image = Column(String(512))
    source_keyword = Column(String(255), default='')
    is_original = Column(Integer, default=1)
    reference_links = Column(Text)  # JSON格式存储文章中的参考链接
    add_ts = Column(BigInteger)
    last_modify_ts = Column(BigInteger)

