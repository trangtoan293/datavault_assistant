from fastapi import UploadFile, HTTPException
from typing import Dict,Union,Any
from pathlib import Path
from datavault_assistant.core.nodes.metadata_handler import MetadataHandler,YAMLDownloadHandler
from datavault_assistant.core.utils.llm import init_llm


class MetadataService:
    def __init__(self):
        self.llm=init_llm('ollama')
        self.metadata_service = MetadataHandler(self.llm)
        self.yaml_handler = YAMLDownloadHandler()
        
    def _config_llm(self,model_name:str='ollama'):
        return init_llm(model_name)
        
        
    async def process_upload_file(self, file: UploadFile,llm:str) -> Dict:
        self.llm = self._config_llm(model_name=llm)
        self.metadata_service = MetadataHandler(self.llm)
        """Process metadata file"""
        try:
            result = await self.metadata_service.analyze_upload_file(file)
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error processing file: {str(e)}"
            )

    async def read_upload_file(self, file: UploadFile) -> str:
        """read metadata file"""
        try:
            temp_path= await self.metadata_service._save_upload_file(file)
            result = self.metadata_service.read_metadata_source(temp_path)
            return result 
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error analyzing metadata: {str(e)}"
            )
    async def generate_yaml(self,data: Dict[str, Any], filename: str = "output"):
        """
        API endpoint để generate và download YAML file
        """
        return await self.yaml_handler.process_and_save_yaml(data, filename)