from typing import Optional
from pydantic_settings import BaseSettings,SettingsConfigDict
from functools import lru_cache

class Settings(BaseSettings):
    # OLLAMA Configs
    GROQ_API_KEY: Optional[str] = None
    GROQ_MODEL: str = "llama-3.3-70b-specdec" 
    # OLLAMA Configs
    OLLAMA_MODEL: str ='llama3.1:8b-instruct-q8_0'
    OLLAMA_BASE_URL: str ='http://192.168.1.8:11434'
    OLLAMA_TEMPERATURE: float = 0
    
    MISTRAL_MODEL:str="mistral-nemo:12b-instruct-2407-q4_K_M"
    MISTRAL_BASE_URL:str='http://192.168.1.8:11434'
    
    MAX_TOKENS: int = 10000
    PROJECT_NAME: str = "Data Vault Assistant"
    API_VERSION: str = "v1"
    
    DEFAULT_TEMPERATURE: float = 0
    # Memory Settings
    MEMORY_TYPE: str = "buffer"  # buffer, file, redis
    MEMORY_KEY: str = "chat_history"
    
    model_config = SettingsConfigDict(env_file='.env', case_sensitive=True)
    
    DB_HOST: Optional[str] = None
    DB_PORT: Optional[int] = None
    DB_USER: Optional[str] = None
    DB_PASSWORD: Optional[str] = None
    DB_NAME: Optional[str] = None
    DB_SCHEMA: Optional[str] = None
    
    def get_postgres_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

from dataclasses import dataclass
@dataclass
class ParserConfig:
    version: str = "1.0.0"
    default_varchar_length: int = 255
    enable_detailed_logging: bool = True
    validation_level: str = "strict"
    target_schema: str = "integration"
    collision_code: str="mdm"
    
@lru_cache()
def get_settings() -> Settings:
    return Settings()

# Initialize settings
settings = get_settings()
