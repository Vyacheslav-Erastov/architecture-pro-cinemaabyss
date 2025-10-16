from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
import httpx
import os
import random


client = httpx.AsyncClient(timeout=30.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await client.aclose()


app = FastAPI(title="Proxy Service", lifespan=lifespan)

PORT = int(os.getenv("PORT", 8000))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true")
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))


def should_use_microservice(migration_percent: int) -> bool:
    """Determine if request should go to microservice based on migration percentage"""
    return random.randint(1, 100) <= migration_percent


def get_target_url(path: str) -> str:
    """Determine target URL based on path and migration settings"""

    if path.startswith("api/events"):
        return f"{EVENTS_SERVICE_URL}/{path}"

    if path.startswith("api/movies"):
        if GRADUAL_MIGRATION == "true" and should_use_microservice(
            MOVIES_MIGRATION_PERCENT
        ):
            return f"{MOVIES_SERVICE_URL}/{path}"
        return f"{MONOLITH_URL}/{path}"

    return f"{MONOLITH_URL}/{path}"


@app.api_route(
    "/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
)
async def universal_proxy(request: Request, path: str):
    """Universal proxy handler for all HTTP methods and paths"""

    target_url = get_target_url(path)

    print(f"Routing {request.method} {request.url.path} -> {target_url}")

    try:
        headers = dict(request.headers)
        headers.pop("host", None)

        response = await client.request(
            method=request.method,
            url=target_url,
            params=dict(request.query_params),
            headers=headers,
            content=(await request.body()),
        )

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=dict(response.headers),
        )

    except httpx.ConnectError as e:
        raise HTTPException(
            status_code=502, detail=f"Cannot connect to backend service: {str(e)}"
        )
    except httpx.TimeoutException as e:
        raise HTTPException(
            status_code=504, detail=f"Backend service timeout: {str(e)}"
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502, detail=f"Error connecting to backend service: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
