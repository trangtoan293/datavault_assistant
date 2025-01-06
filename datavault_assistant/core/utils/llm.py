from langchain_ollama import ChatOllama
from langchain_groq import ChatGroq
from typing import Optional
from datavault_assistant.configs.settings import settings

class LLMFactory:
    """Factory class để khởi tạo các LLM models"""
    
    @staticmethod
    def init_llm(
        provider: str = "ollama",
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) :
        """
        Khởi tạo LLM model dựa trên provider
        
        Args:
            provider: Tên nhà cung cấp LLM ollama hoặc groq
            model: Model specific name
            temperature: Temperature cho model
            
        Returns:
            LLM model instance
        """
        
        temperature = temperature or settings.DEFAULT_TEMPERATURE
        
        if provider == "ollama":
            return ChatOllama(
                base_url=settings.OLLAMA_BASE_URL,
                model=model or settings.OLLAMA_MODEL,
                temperature=settings.OLLAMA_TEMPERATURE,
                num_ctx=settings.MAX_TOKENS
            )

        elif provider == "groq":
            return ChatGroq( 
                model=model or settings.GROQ_MODEL,
                api_key=settings.GROQ_API_KEY,
                temperature=temperature,
            )
            
            
        else:
            raise ValueError(f"Provider {provider} không được hỗ trợ")

def init_llm(provider: str = "ollama", **kwargs) :
    """Helper function để lấy LLM instance"""
    return LLMFactory.init_llm(provider, **kwargs)

if __name__ == "__main__":
    # llm = init_llm(provider="ollama")
    # print(llm)
    print(settings.GROQ_API_KEY)