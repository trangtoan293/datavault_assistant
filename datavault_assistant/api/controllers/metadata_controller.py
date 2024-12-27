from fastapi import UploadFile, HTTPException
from typing import Dict,Union
from pathlib import Path
from core.nodes.metadata_reader import MetadataService
from core.nodes.metadata_analyzer import LLMMetadataAnalyzer
from core.nodes.data_vault_builder import LLMDataVaultBuilder
from core.nodes.datavault_analyzer import HubAnalyzer
from core.utils.llm import init_llm

class MetadataController:
    def __init__(self):
        self.llm=init_llm('ollama')
        self.metadata_service = MetadataService()
        self.analyzer_service = LLMMetadataAnalyzer(self.llm)
        self.builder_service = LLMDataVaultBuilder(self.llm)
        self.hub_analyzer = HubAnalyzer(self.llm)
        
    def _config_llm(self,model_name:str='ollama'):
        return init_llm(model_name)
        
    async def process_metadata_file(self, file: UploadFile,llm:str) -> Dict:
        self.llm = self._config_llm(model_name=llm)
        """Process metadata file"""
        try:
            # Get file extension
            file_extension = Path(file.filename).suffix.lower()
            
            # Validate file format
            if file_extension not in ['.csv', '.xlsx', '.xls', '.yaml', '.yml']:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported file format: {file_extension}"
                )
            
            # Process file using service
            metadata_result = await self.metadata_service.process_file(file)
            analyzed_result =  self.analyzer_service.analyze_table(metadata_result)
            result =  self.builder_service.recommend_data_model(analyzed_result)
            return result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error processing file: {str(e)}"
            )
            
    async def get_hub_info(self, metadata: str,llm:str) -> str:
        self.llm =  self._config_llm(llm)
        """Analyze metadata directly"""
        try:
            return self.hub_analyzer.analyze(metadata)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error analyzing metadata: {str(e)}"
            )

    async def analyze_metadata(self, metadata: str,llm:str) -> str:
        self.llm =  self._config_llm(llm)
        """Analyze metadata directly"""
        try:
            return self.analyzer_service.analyze_table(metadata)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error analyzing metadata: {str(e)}"
            )
            
    async def process_upload_file(self, file: UploadFile) -> Dict:
        """Process metadata file"""
        try:
            # Get file extension
            file_extension = Path(file.filename).suffix.lower()
            
            # Validate file format
            if file_extension not in ['.csv', '.xlsx', '.xls', '.yaml', '.yml']:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported file format: {file_extension}"
                )
            
            # Process file using service
            metadata_result = await self.metadata_service.process_file(file)
            return metadata_result
            
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Error processing file: {str(e)}"
            )
