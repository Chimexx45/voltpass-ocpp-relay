import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 8080;
const HTTP_OCPP_HANDLER = process.env.HTTP_OCPP_HANDLER;
const RELAY_SECRET = process.env.RELAY_SECRET;

app.get("/health", (req, res) => {
  res.json({ status: "ok", service: "voltpass-ocpp-relay" });
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const parts = url.pathname.split("/");
  const chargerId = parts[parts.length - 1];

  if (!chargerId || chargerId === "ocpp") {
    ws.close(1008, "Missing chargerId");
    return;
  }

  console.log(`Charger connected: ${chargerId}`);

  ws.on("message", async (data) => {
    const rawFrame = data.toString();

    try {
      const response = await fetch(HTTP_OCPP_HANDLER, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-relay-secret": RELAY_SECRET
        },
        body: JSON.stringify({
          chargerId,
          frame: rawFrame,
          receivedAt: new Date().toISOString()
        })
      });

      const result = await response.text();

      if (result && result.trim() !== "") {
        ws.send(result);
      }
    } catch (error) {
      console.error("Relay error:", error);
      ws.close(1011, "Relay error");
    }
  });

  ws.on("close", () => {
    console.log(`Charger disconnected: ${chargerId}`);
  });
});

server.listen(PORT, () => {
  console.log(`OCPP relay running on port ${PORT}`);
});
