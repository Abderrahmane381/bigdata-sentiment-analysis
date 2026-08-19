from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List
import asyncio

app = FastAPI()

# Liste des clients connectés
connected_clients: List[WebSocket] = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    print(f"✅ Client connecté — {len(connected_clients)} clients actifs")
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        print(f"❌ Client déconnecté — {len(connected_clients)} clients actifs")

@app.post("/predict")
async def receive_prediction(data: dict):
    """Reçoit une prédiction de Spark et l'envoie à tous les clients WebSocket"""
    import json
    message = json.dumps(data)
    disconnected = []
    for client in connected_clients:
        try:
            await client.send_text(message)
        except:
            disconnected.append(client)
    for client in disconnected:
        connected_clients.remove(client)
    return {"status": "sent", "clients": len(connected_clients)}
