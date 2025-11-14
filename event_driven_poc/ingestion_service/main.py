from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional
from ingestion_service import IngestionService
from conductor_api import deployConductorWorkflow, triggerConductorWorkflow
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Initialize FastAPI
app = FastAPI(title="Ingestion Service", version="1.1.0")

# Enable CORS for testing
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conductor server URL from environment or default
CONDUCTOR_URL = os.getenv("CONDUCTOR_URL", "http://conductor-server-event:8080")

# Initialize ingestion service
ingestion_service = IngestionService()


# Pydantic model for workflow payload
class WorkflowPayload(BaseModel):
    workflow: Dict[str, Any]
    workflow_id: Optional[str] = None
    trigger_conductor: Optional[bool] = True


@app.post("/workflow/deploy")
async def deploy_workflow(payload: WorkflowPayload):
    try:
        # Hardcoded MinIO URI for ingestion
        minio_input_uri = "minio://raw-data/1000861642.in"
        print("Using hardcoded MinIO URI:", minio_input_uri)

        # 1️⃣ Execute ingestion only for trigger_airflow_dag tasks
        service_results,_ = ingestion_service.execute_service_chain(
            workflow_json=payload.workflow,
            minio_input_uri=minio_input_uri,
            workflow_id=payload.workflow_id
        )

        # 2️⃣ Update workflow template with updated metadata URLs
        updated_workflow = ingestion_service.update_workflow_template(
            workflow_json=payload.workflow,
            service_results=service_results
        )

        # 3️⃣ Ensure workflow has a version
        if "version" not in updated_workflow:
            updated_workflow["version"] = int(time.time())

        workflow_instance_id = None
        if payload.trigger_conductor:
            # 4️⃣ Deploy workflow to Conductor
            deployConductorWorkflow(CONDUCTOR_URL, updated_workflow)

            # 5️⃣ Trigger workflow instance
            workflow_instance_id = triggerConductorWorkflow(
                CONDUCTOR_URL,
                updated_workflow.get("name"),
                inputParams={},
                version=updated_workflow.get("version")
            )

        # 6️⃣ Get ingestion results for first executed task (if any)
        first_service_name = list(service_results.keys())[0] if service_results else None
        first_result = service_results.get(first_service_name, {})

        return {
            "runId": first_result.get("run_id"),
            "s3_input_uri": first_result.get("s3_input_uri"),
            "s3_output_uri": first_result.get("s3_output_uri"),
            "s3_report_uri": first_result.get("s3_report_uri"),
            "updated_json_uri": first_result.get("updated_json_uri"),
            "updated_json_s3_key": first_result.get("updated_json_s3_key"),
            "workflow_instance_id": workflow_instance_id,
            "message": (
                "Workflow deployed and triggered successfully"
                if workflow_instance_id else "Workflow deployed without triggering Conductor"
            )
        }

    except Exception as e:
        import traceback
        print("Error deploying workflow:", str(e))
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to deploy workflow: {str(e)}"
        )
