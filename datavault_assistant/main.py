from fastapi import FastAPI
from api.routes import metadata

app = FastAPI(
    title="Metadata Parser API",
    description="API để đọc và xử lý metadata từ CSV, Excel, và YAML files",
    version="1.0.0"
)

# Include routers
app.include_router(metadata.router)
app.include_router(metadata.router)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)