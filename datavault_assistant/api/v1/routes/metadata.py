from fastapi import APIRouter, File, UploadFile, Depends, HTTPException
from typing import Dict, Union
from datavault_assistant.api.v1.services.metadata_service import MetadataService
router = APIRouter(prefix="/metadata", tags=["metadata"])
service = MetadataService()

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

@router.post("/upload_metadata_file/", operation_id="read_upload_file")
async def read_upload_file(
    file: UploadFile = File(...)
):
    """Upload và phân tích metadata file"""
    try:
        # Process file và analyze metadata
        result = await service.read_upload_file(file=file)
        return {
            "filename": file.filename,
            "metadata": result,
            "message": "File processed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/analyze_metadata_file/", operation_id="process_upload_file")
async def process_upload_file(
    llm:str="groq",
    file: UploadFile = File(...)
):
    """Phân tích metadata bằng LLM"""
    try:
        analyzed_result = await service.process_upload_file(file,llm)
        return {
            "analyzed_metadata": analyzed_result,
            "message": "Metadata analyzed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze_metadata_file/", operation_id="process_upload_file")
async def process_upload_file(
    llm:str="groq",
    file: UploadFile = File(...)
):
    """Phân tích metadata bằng LLM"""
    try:
        analyzed_result = await service.process_upload_file(file,llm)
        return {
            "analyzed_metadata": analyzed_result,
            "message": "Metadata analyzed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
