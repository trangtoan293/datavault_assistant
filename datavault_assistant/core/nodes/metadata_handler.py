import pandas as pd
from typing import Dict, Union, Optional
from pathlib import Path
import tempfile
import os
from fastapi import UploadFile
import logging
from pydantic import BaseModel
from core.nodes.data_vault_builder import DataVaultAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetadataConfig(BaseModel):
    """Configuration for metadata handling"""
    required_columns: list = [
        'SCHEMA_NAME', 'TABLE_NAME', 'COLUMN_NAME', 
        'DATA_TYPE', 'LENGTH', 'NULLABLE', 'DESCRIPTION'
    ]
    allowed_extensions: list = ['.xlsx', '.xls', '.xlsm', '.csv']

class MetadataHandler:
    """
    Unified class for handling metadata operations including parsing and processing
    """
    def __init__(self,llm: Optional[str], config: Optional[MetadataConfig] =None):
        self.llm=llm
        self.config = config or MetadataConfig()
        self.analyzer = DataVaultAnalyzer(self.llm)
    
    def validate_columns(self, df: pd.DataFrame) -> bool:
        """Validate if DataFrame has required columns"""
        try:
            return all(col in df.columns for col in self.config.required_columns)
        except Exception as e:
            logger.error(f"Column validation failed: {str(e)}")
            raise ValueError(f"Column validation error: {str(e)}")

    def validate_file_extension(self, file_path: Union[str, Path]) -> bool:
        """Validate if file has allowed extension"""
        return Path(file_path).suffix.lower() in self.config.allowed_extensions

    def read_file(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """Read file into DataFrame based on extension"""
        file_path = Path(file_path)
        try:
            if file_path.suffix.lower() in ['.xlsx', '.xls', '.xlsm']:
                return pd.read_excel(file_path)
            elif file_path.suffix.lower() == '.csv':
                return pd.read_csv(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
        except Exception as e:
            logger.error(f"File reading failed: {str(e)}")
            raise

    def process_metadata(self, df: pd.DataFrame) -> str:
        """Process DataFrame into metadata string"""
        try:
            df = df.fillna('')
            return df.to_string(index=False)
        except Exception as e:
            logger.error(f"Metadata processing failed: {str(e)}")
            raise

    def read_metadata_source(self, file_path: Union[str, Path]) -> str:
        """Main method to read and process metadata from file"""
        try:
            if not self.validate_file_extension(file_path):
                raise ValueError(f"Invalid file format. Allowed formats: {self.config.allowed_extensions}")
            
            df = self.read_file(file_path)
            
            if not self.validate_columns(df):
                raise ValueError(f"Missing required columns. Required: {self.config.required_columns}")
            
            return self.process_metadata(df)
            
        except Exception as e:
            logger.error(f"Metadata source processing failed: {str(e)}")
            raise

    async def _save_upload_file(self, file: UploadFile) -> str:
        """Save uploaded file to temporary location"""
        try:
            suffix = Path(file.filename).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                content = await file.read()
                temp_file.write(content)
                return temp_file.name
        except Exception as e:
            logger.error(f"Failed to save upload file: {str(e)}")
            raise
    
    def analyze_local_file(self,file: Path)-> Dict:
        try:
            if not self.validate_file_extension(file.name):
                raise ValueError(f"Invalid file format. Allowed formats: {self.config.allowed_extensions}")
            result = self.read_metadata_source(file)
            analyzed_result = self.analyzer.analyze(result)
            return {
                    "metadata": result,
                    "analysis": analyzed_result
                }
            
        except Exception as e:
            logger.error(f"processing failed local file: {str(e)}")
            raise
        
    async def analyze_upload_file(self, file: UploadFile) -> Dict:
        """Process uploaded file and return metadata"""
        temp_path = None
        try:
            # if not self.validate_file_extension(file.filename):
            #     raise ValueError(f"Invalid file format. Allowed formats: {self.config.allowed_extensions}")
            
            temp_path = await self._save_upload_file(file)
            result = self.read_metadata_source(temp_path)
            # Optional: Process with analyzer if needed
            analyzed_result = self.analyzer.analyze(result)
            
            return {
                "metadata": result,
                "analysis": analyzed_result
            }
            
        except Exception as e:
            logger.error(f"Upload processing failed: {str(e)}")
            raise
            
        finally:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file: {str(e)}")

# Usage example
if __name__ == "__main__":
    handler = MetadataHandler()
    # Example synchronous usage
    try:
        result = handler.read_metadata_source("path/to/file.xlsx")
        print("Processed metadata:", result)
    except Exception as e:
        print(f"Error processing file: {str(e)}")

    # Example async usage with FastAPI
    """
    @app.post("/upload")
    async def upload_file(file: UploadFile):
        handler = MetadataHandler()
        result = await handler.process_upload(file)
        return result
    """