import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import dotenv from "dotenv";
import { createClient } from "@supabase/supabase-js";

dotenv.config();

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 8080;
const CSMS_HTTP_BASE_URL =
  process.env.CSMS_HTTP_BASE_URL || "https://voltpass.ng/api/public/ocpp";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const supabase =
  SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
    ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    : null;

const activeChargers = new Map();

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "voltpass-ocpp-relay",
    activeChargers: activeChargers.size,
  });
});


const routeWss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (!url.pathname.startsWith("/api/public/ocpp/")) {
    socket.destroy();
    return;
  }

  routeWss.handleUpgrade(req, socket, head, (ws) => {
    routeWss.emit("connection", ws, req);
  });
});

routeWss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const chargerId = url.pathname.split("/").pop();

  const authKey =
    req.headers.authorization?.replace("Bearer ", "") ||
    req.headers["x-ocpp-auth-key"] ||
    url.searchParams.get("auth_key");

  if (!chargerId) {
    ws.close(1008, "Missing chargerId");
    return;
  }

  if (!authKey) {
    ws.close(1008, "Missing auth key");
    return;
  }

  activeChargers.set(chargerId, ws);
  console.log(`Charger connected: ${chargerId}`);

  ws.on("message", async (data) => {
    try {
      const frame = JSON.parse(data.toString());

      if (!Array.isArray(frame)) {
        ws.send(JSON.stringify([4, "unknown", "ProtocolError", "Invalid OCPP frame", {}]));
        return;
      }

      const [messageTypeId, uniqueId, action, payload] = frame;

      // Charger -> CSMS CALL
      if (messageTypeId === 2) {
        const response = await fetch(`${CSMS_HTTP_BASE_URL}/${chargerId}`, {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${authKey}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            action,
            payload: payload || {},
          }),
        });

        const text = await response.text();

        let result;
        try {
          result = JSON.parse(text);
        } catch {
          console.error("Non-JSON CSMS response:", text.slice(0, 300));
          ws.send(JSON.stringify([4, uniqueId, "InternalError", "CSMS returned non-JSON", {}]));
          return;
        }

        if (!response.ok) {
          ws.send(JSON.stringify([4, uniqueId, "SecurityError", result.error || "CSMS error", {}]));
          return;
        }

        ws.send(JSON.stringify([3, uniqueId, result.response || {}]));

        if (Array.isArray(result.pendingCommands)) {
          for (const cmd of result.pendingCommands) {
            pushCommandToCharger(chargerId, cmd);
          }
        }

        return;
      }

      // Charger -> CSMS CALLRESULT / CALLERROR for remote command
      if (messageTypeId === 3 || messageTypeId === 4) {
        await recordCommandResponse(uniqueId, frame);
        return;
      }
    } catch (err) {
      console.error("Message handling error:", err);
      ws.close(1011, "Relay error");
    }
  });

  ws.on("close", () => {
    activeChargers.delete(chargerId);
    console.log(`Charger disconnected: ${chargerId}`);
  });

  ws.on("error", (err) => {
    console.error(`WebSocket error for ${chargerId}:`, err);
  });
});

async function pushCommandToCharger(chargerId, commandRow) {
  const ws = activeChargers.get(chargerId);

  if (!ws || ws.readyState !== ws.OPEN) {
    return;
  }

  const uniqueId = commandRow.id;
  const action = commandRow.command;
  const payload = commandRow.payload || {};

  const frame = [2, uniqueId, action, payload];

  ws.send(JSON.stringify(frame));

  if (supabase) {
    await supabase
      .from("ocpp_commands")
      .update({
        status: "sent",
        sent_at: new Date().toISOString(),
      })
      .eq("id", commandRow.id);
  }
}

async function recordCommandResponse(uniqueId, frame) {
  if (!supabase) return;

  const messageTypeId = frame[0];

  await supabase
    .from("ocpp_commands")
    .update({
      status: messageTypeId === 3 ? "acknowledged" : "error",
      response: frame,
      responded_at: new Date().toISOString(),
    })
    .eq("id", uniqueId);
}

if (supabase) {
  supabase
    .channel("ocpp-command-stream")
    .on(
      "postgres_changes",
      {
        event: "INSERT",
        schema: "public",
        table: "ocpp_commands",
      },
      (payload) => {
        const command = payload.new;
        pushCommandToCharger(command.charger_id, command);
      }
    )
    .subscribe((status) => {
      console.log("Supabase realtime status:", status);
    });
} else {
  console.warn("Supabase realtime disabled. Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

server.listen(PORT, () => {
  console.log(`OCPP relay running on port ${PORT}`);
  console.log(`Forwarding to ${CSMS_HTTP_BASE_URL}/<chargerId>`);
});
