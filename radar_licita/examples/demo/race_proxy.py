"""Local test proxy that returns the R$ 5,000 recommendation after a later query.

Run only against a disposable demo API. This does not contact any external source.
"""

import asyncio

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["http://127.0.0.1:5175"], allow_methods=["*"], allow_headers=["*"])


@app.api_route("/api/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def forward(path: str, request: Request) -> Response:
    body = await request.body()
    async with httpx.AsyncClient(timeout=20) as client:
        upstream = await client.request(
            request.method,
            f"http://127.0.0.1:8001/api/v1/{path}",
            params=request.query_params,
            content=body,
            headers={"content-type": request.headers.get("content-type", "application/json")},
        )
    if path == "recommendations/search" and b'"available_capital":"5000"' in body:
        await asyncio.sleep(3)
    return Response(upstream.content, status_code=upstream.status_code, media_type=upstream.headers.get("content-type"))
