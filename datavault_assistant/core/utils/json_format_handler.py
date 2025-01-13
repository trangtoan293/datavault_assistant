from pydantic import BaseModel, Field
from typing import List, Optional
import json
from functools import wraps
from time import sleep
from typing import Callable, TypeVar, Any
import logging

class HubComponent(BaseModel):
    name: str = Field(..., pattern="^HUB_[A-Z0-9_]+$")
    business_keys: List[str]
    source_tables: List[str]
    description: str

class LinkComponent(BaseModel):
    name: str = Field(..., pattern="^LNK_[A-Z0-9_]+$")
    related_hubs: List[str]
    business_keys: List[str]
    source_tables: List[str]
    description: str

class SatelliteComponent(BaseModel):
    name: str = Field(..., pattern="^SAT_[A-Z0-9_]+$")
    hub: str
    business_keys: List[str]
    source_table: str
    descriptive_attrs: List[str]

class LinkSatelliteComponent(BaseModel):
    name: str = Field(..., pattern="^LSAT_[A-Z0-9_]+$")
    link: str
    business_keys: List[str]
    source_table: str
    descriptive_attrs: List[str]

class DataVaultOutput(BaseModel):
    hubs: List[HubComponent]
    links: List[LinkComponent]
    satellites: List[SatelliteComponent]
    link_satellites: List[LinkSatelliteComponent]

def validate_llm_output(json_data: dict) -> DataVaultOutput:
    """
    Validates the JSON output from LLM against the defined schema.
    
    Args:
        json_data (dict): JSON data from LLM
        
    Returns:
        DataVaultOutput: Validated data object
        
    Raises:
        ValidationError: If data doesn't match schema
    """
    return DataVaultOutput(**json_data)

from typing import Dict, Any
import re

class JSONPostProcessor:
    @staticmethod
    def clean_component_name(name: str) -> str:
        """
        Chuẩn hóa tên component theo convention.
        """
        # Convert to uppercase and replace spaces with underscores
        name = re.sub(r'\s+', '_', name.upper())
        # Remove special characters except underscores
        name = re.sub(r'[^A-Z0-9_]', '', name)
        return name

    @staticmethod
    def standardize_keys(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Chuẩn hóa keys trong JSON.
        """
        # Convert all keys to snake_case
        new_data = {}
        for key, value in data.items():
            new_key = re.sub(r'(?<!^)(?=[A-Z])', '_', key).lower()
            
            if isinstance(value, dict):
                value = JSONPostProcessor.standardize_keys(value)
            elif isinstance(value, list):
                value = [
                    JSONPostProcessor.standardize_keys(item)
                    if isinstance(item, dict) else item
                    for item in value
                ]
                
            new_data[new_key] = value
        return new_data

    @staticmethod
    def process_output(json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process và clean JSON output từ LLM.
        """
        # Clean component names
        for component_type in ['hubs', 'links', 'satellites', 'link_satellites']:
            if component_type in json_data:
                for component in json_data[component_type]:
                    if 'name' in component:
                        component['name'] = JSONPostProcessor.clean_component_name(
                            component['name']
                        )

        # Standardize all keys
        json_data = JSONPostProcessor.standardize_keys(json_data)

        return json_data




T = TypeVar('T')

class RetryException(Exception):
    pass

def retry_on_invalid_json(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: tuple = (json.JSONDecodeError, ValueError)
) -> Callable:
    """
    Decorator để retry khi gặp lỗi JSON invalid từ LLM.
    
    Args:
        max_attempts (int): Số lần thử tối đa
        delay (float): Thời gian chờ giữa các lần thử (seconds)
        backoff_factor (float): Hệ số tăng thời gian chờ
        exceptions (tuple): Các exception cần retry
        
    Returns:
        Callable: Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            current_delay = delay

            for attempt in range(max_attempts):
                try:
                    result = func(*args, **kwargs)
                    # Validate JSON structure
                    if isinstance(result, str):
                        json.loads(result)
                    return result
                except exceptions as e:
                    last_exception = e
                    logging.warning(
                        f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}"
                    )
                    if attempt < max_attempts - 1:
                        sleep(current_delay)
                        current_delay *= backoff_factor
                    continue

            raise RetryException(
                f"Failed after {max_attempts} attempts. Last error: {str(last_exception)}"
            )

        return wrapper
    return decorator