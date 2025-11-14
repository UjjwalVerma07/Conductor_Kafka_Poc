"""
Ingestion Service - Handles file transfer from MinIO to S3 and workflow preparation
"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from ingestion_service import IngestionService
import os
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="Ingestion Service",
    description="Service to handle file ingestion from MinIO to S3 and prepare workflow definitions",
    version="1.0.0"
)

# Initialize IngestionService
ingestion_service = IngestionService()


class WorkflowRequest(BaseModel):
    workflow: Dict[str, Any]  # Full workflow JSON from UI
    metadata_url: str          # URL of the JSON template that contains input/output URIs
    workflow_id: str           # Workflow instance ID (optional for logging)
    

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "ingestion-service",
        "status": "running",
        "version": "1.0.0"
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.post("/ingest")
async def ingest_workflow(request: WorkflowRequest):
    """
    Endpoint to ingest workflow and dynamically process service chain.
    Updates workflow JSON with input/output URIs for each service.
    """
    try:
        workflow_json = request.workflow
        metadata_url = request.metadata_url
        workflow_id = request.workflow_id

        # Step 1: Execute the service chain dynamically
        result = ingestion_service.execute_service_chain(
            workflow_json=workflow_json,
            metadata_url=metadata_url,
            workflow_id=workflow_id
        )

        # Step 2: Update workflow template with real URIs
        updated_workflow = ingestion_service.update_workflow_template(
            workflow_json=workflow_json,
            metadata_url=metadata_url
        )

        return JSONResponse(
            status_code=200,
            content={
                "message": "Workflow ingested and updated successfully",
                "service_chain_result": result,
                "updated_workflow": updated_workflow
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ingesting workflow: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
