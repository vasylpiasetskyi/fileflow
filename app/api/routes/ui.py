from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from app.api.deps import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "index.html")
