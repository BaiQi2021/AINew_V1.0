# -*- coding: utf-8 -*-
"""
Global Configuration
"""

from pydantic_settings import BaseSettings
from typing import Optional
from pydantic import Field
from pathlib import Path

# Environment file handling
PROJECT_ROOT: Path = Path(__file__).resolve().parent
ENV_FILE: Path = PROJECT_ROOT / ".env"

class Settings(BaseSettings):
    """
    Global settings for AIReport.
    Loads from environment variables or .env file.
    """
    # Database Configuration
    DB_DIALECT: str = Field("mysql", description="Database type: 'mysql' or 'postgresql'")
    DB_HOST: str = Field("localhost", description="Database host")
    DB_PORT: int = Field(3306, description="Database port")
    DB_USER: str = Field("root", description="Database user")
    DB_PASSWORD: str = Field("", description="Database password")
    DB_NAME: str = Field("ai_report", description="Database name")
    
    # Analysis API Configuration
    # Using 'REPORT_ENGINE' prefix to match previous setup, or simplify to just API_KEY
    REPORT_ENGINE_API_KEY: Optional[str] = Field(None, description="API Key for the LLM")
    REPORT_ENGINE_BASE_URL: Optional[str] = Field("http://litellm.aviagames.net", description="Base URL for the LLM API")
    REPORT_ENGINE_MODEL_NAME: Optional[str] = Field("gpt-3.5-turbo", description="Model name to use")
    
    # Gemini API Configuration (向后兼容)
    GEMINI_API_KEY: Optional[str] = Field(None, description="Gemini API Key")
    GEMINI_BASE_URL: Optional[str] = Field("https://generativelanguage.googleapis.com/v1beta/openai/", description="Gemini Base URL")
    GEMINI_MODEL_NAME: Optional[str] = Field("gemini-3-pro-preview", description="Gemini Model name")

    class Config:
        env_file = str(ENV_FILE)
        env_prefix = ""
        case_sensitive = False
        extra = "allow"

settings = Settings()
