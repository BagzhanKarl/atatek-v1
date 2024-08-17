from fastapi import FastAPI

app = FastAPI()

@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Alizhan {name}"}
