from pydantic import BaseModel

from . import Tool


class ApprovalConfig(BaseModel):
    message: str = "Approval required"


# Executed by the engine (LangGraph interrupt + persisted approval event), not by a run function.
TOOL = Tool(name="approval", config_model=ApprovalConfig, run=None)
