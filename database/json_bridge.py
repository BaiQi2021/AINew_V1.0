import json
import asyncio
from sqlalchemy import select, insert
from database.db_session import get_session, init_db
from database.models import QbitaiArticle, CompanyArticle, AibaseArticle, BaaiHubArticle, ReportedArticle, Base
import logging

logger = logging.getLogger(__name__)

MODELS = {
    "qbitai_article": QbitaiArticle,
    "company_article": CompanyArticle,
    "aibase_article": AibaseArticle,
    "baai_hub_article": BaaiHubArticle,
    "reported_article": ReportedArticle
}

async def export_to_json(file_path="data/db_export.json"):
    """Export all tables to a single JSON file."""
    data = {}
    async with get_session() as session:
        for table_name, model in MODELS.items():
            try:
                stmt = select(model)
                result = await session.execute(stmt)
                rows = result.scalars().all()
                
                # Convert to dict
                table_data = []
                for row in rows:
                    row_dict = {}
                    for column in model.__table__.columns:
                        val = getattr(row, column.name)
                        # Basic serialization for JSON
                        if val is not None:
                            row_dict[column.name] = val
                    table_data.append(row_dict)
                
                data[table_name] = table_data
            except Exception as e:
                logger.error(f"Failed to export table {table_name}: {e}")
            
    import os
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Exported database to {file_path}")

async def import_from_json(file_path="data/db_export.json"):
    """Import data from JSON file into the database."""
    import os
    
    # Always initialize DB first to ensure tables exist
    await init_db()
    
    if not os.path.exists(file_path):
        logger.warning(f"JSON file {file_path} not found, skipping import.")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    async with get_session() as session:
        for table_name, rows in data.items():
            if table_name not in MODELS:
                continue
            
            model = MODELS[table_name]
            if not rows:
                continue
            
            # To avoid duplicates, we check by article_id if it exists
            # For simplicity in this bridge, we'll just try to insert and ignore duplicates if possible,
            # or better, check one by one.
            
            for row in rows:
                # Check if exists
                article_id = row.get('article_id')
                if article_id:
                    stmt = select(model).where(model.article_id == article_id)
                    result = await session.execute(stmt)
                    if result.scalar():
                        continue
                
                # Insert
                new_row = model(**row)
                session.add(new_row)
        
        await session.commit()
    logger.info(f"Imported data from {file_path}")

if __name__ == "__main__":
    # Quick test
    logging.basicConfig(level=logging.INFO)
    # asyncio.run(export_to_json())
