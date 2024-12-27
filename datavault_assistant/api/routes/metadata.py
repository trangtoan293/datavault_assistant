from fastapi import APIRouter, File, UploadFile, Depends, HTTPException
from ..controllers.metadata_controller import MetadataController
from typing import Dict, Union

router = APIRouter(prefix="/metadata", tags=["metadata"])
controller=MetadataController()

@router.get("/formats", operation_id="get_supported_formats")
async def get_supported_formats():
    """Get supported file formats"""
    return {
        "supported_formats": [
            "CSV (.csv)",
            "Excel (.xlsx, .xls)",
            "YAML (.yaml, .yml)"
        ]
    }

@router.post("/upload_analyze/", operation_id="upload_metadata_analyze")
async def upload_metadata(
    file: UploadFile = File(...)
):
    """Upload và phân tích metadata file"""
    try:
        # Process file và analyze metadata
        result = await controller.process_metadata_file(file=file,llm='ollama')
        return {
            "filename": file.filename,
            "metadata": result,
            "message": "File processed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze/", operation_id="analyze_metadata")
async def analyze_metadata(
    metadata: str,
    llm:str
):
    """Phân tích metadata bằng LLM"""
    try:
        analyzed_result = await controller.analyze_metadata(metadata,llm)
        return {
            "analyzed_metadata": analyzed_result,
            "message": "Metadata analyzed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/upload/", operation_id="upload_file")
async def upload_file(
    file: UploadFile = File(...)
):
    """Upload và phân tích metadata file"""
    try:
        # Process file và analyze metadata
        result = await controller.process_upload_file(file)
        return {
            "filename": file.filename,
            "metadata": result,
            "message": "File processed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze_hub/", operation_id="analyze_metadata_hub")
async def analyze_metadata_hub(
    metadata: str,
    llm:str
):
    """Phân tích metadata bằng LLM"""
    try:
        analyzed_result = await controller.get_hub_info(metadata,llm)
        return {
            "analyzed_metadata": analyzed_result,
            "message": "Metadata analyzed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
