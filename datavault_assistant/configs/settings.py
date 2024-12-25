from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache

class Settings(BaseSettings):
    # OLLAMA Configs
    GROQ_API_KEY: Optional[str] = None
    GROQ_MODEL: str = "llama3-8b-8192"
    # OLLAMA Configs
    OLLAMA_MODEL: str ='llama3.1:8b-instruct-q8_0'
    OLLAMA_BASE_URL: str= 'http://192.168.1.8:11434'
    OLLAMA_TEMPERATURE: float = 0
    MAX_TOKENS: int = 10000
    
    DEFAULT_TEMPERATURE: float = 0
    # Memory Settings
    MEMORY_TYPE: str = "buffer"  # buffer, file, redis
    MEMORY_KEY: str = "chat_history"

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    return Settings()

# Initialize settings
settings = get_settings()