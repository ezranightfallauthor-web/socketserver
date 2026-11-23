import "dotenv/config";

import express from "express";
import http from "http";
import { Server } from "socket.io";

import { refreshLanguages, isValidLanguage } from "./languages.js";
import {
    realtimeRoom,
    languageRoom,
    tvRoom
} from "./rooms.js";
import {
    emitActiveLanguageState,
    setMetricsService
} from "./transcription.js";
import { RealtimeWhisperService } from "./realtimeWhisperService.js";
import { MetricsService } from "./metrics.js";

const app = express();

const PORT = process.env.PORT || 3000;
const HOST = "127.0.0.1";

/*
========================================
HTTP SERVER
========================================
*/

const server = http.createServer(app);

/*
========================================
SOCKET.IO
========================================
*/

const io = new Server(server, {
    cors: {
        origin: [
            "https://1303ee1c91cf451239c907c16e581eba-10157.sites.k-hosting.co.uk",
            "https://socket.chillexion.com",
            "https://chillexion.co.uk",
            "https://www.chillexion.com",
            "https://chillexion.com"
        ],
        methods: ["GET", "POST"],
        credentials: true
    },
    transports: ["websocket", "polling"]
});

io.use((socket, next) => {
    console.log(`[CONNECT ATTEMPT] id=${socket.id} transport=${socket.conn.transport.name}`);
    console.log(`[HEADERS]`, socket.handshake.headers);
    console.log(`[QUERY]`, socket.handshake.query);
    console.log(`[AUTH]`, socket.handshake.auth);

    next();
});

/*
========================================
OPENAI TRANSCRIPTION + TRANSLATION
========================================
*/

const metrics = new MetricsService({
    io
});

setMetricsService(metrics);

const whisper = new RealtimeWhisperService({
    apiKey: process.env.OPENAI_API_KEY,
    io
});

function normaliseMode(value) {
    const mode = String(value || "attendee").toLowerCase().trim();

    if (["speaker", "attendee", "tv"].includes(mode)) {
        return mode;
    }

    return "attendee";
}

function detachVenueIfNoSpeakers(venueId) {
    if (!venueId) return;

    setTimeout(() => {
        const room = io.sockets
            .adapter
            .rooms
            .get(
                realtimeRoom(venueId)
            );

        if (!room?.size) {
            void whisper.detachVenue(venueId);
        }
    }, 0);
}

function leaveCurrentMode(socket, { silent = false } = {}) {
    const venueId = socket.data?.venueId;
    const language = socket.data?.language;
    const mode = socket.data?.mode;

    if (!venueId || !mode) {
        return;
    }

    if (mode === "speaker") {
        whisper.releaseAudioOwner(venueId, socket.id);
        socket.leave(realtimeRoom(venueId));
        detachVenueIfNoSpeakers(venueId);
    }

    if (mode === "attendee") {
        if (language) {
            socket.leave(languageRoom(venueId, language));
        }

        // After the socket has left, update TV displays with the active
        // attendee language list.
        setTimeout(() => {
            emitActiveLanguageState(io, venueId);
        }, 0);
    }

    if (mode === "tv") {
        socket.leave(tvRoom(venueId));
    }

    metrics.recordLeave(socket, {
        venueId,
        mode,
        language,
        reason: silent ? "silent" : "leave"
    });

    if (!silent) {
        console.log(`${socket.id} left venue:${venueId} mode:${mode}`);
    }

    socket.data.venueId = null;
    socket.data.language = null;
    socket.data.mode = null;
}

/*
========================================
HEALTH
========================================
*/

app.get("/", (req, res) => {
    res.json({
        ok: true,
        service: "socket.chillexion.com"
    });
});

app.get("/health", (req, res) => {
    res.json({
        ok: true
    });
});

/*
========================================
METRICS API
========================================
*/

app.get("/api/metrics", (req, res) => {
    res.json(metrics.getAllSnapshot());
});

app.get("/api/metrics/venues/:venueId", (req, res) => {
    const snapshot = metrics.getVenueSnapshot(req.params.venueId);

    if (!snapshot) {
        res.status(404).json({
            ok: false,
            message: "Venue not found"
        });
        return;
    }

    res.json({
        ok: true,
        ...snapshot
    });
});

app.get("/api/metrics/venues/:venueId/languages", (req, res) => {
    res.json({
        ok: true,
        venueId: String(req.params.venueId),
        languages: metrics.getActiveLanguages(req.params.venueId)
    });
});

app.get("/api/metrics/venues/:venueId/events", (req, res) => {
    res.json({
        ok: true,
        venueId: String(req.params.venueId),
        events: metrics.getVenueEvents(
            req.params.venueId,
            req.query.limit
        )
    });
});

/*
========================================
SOCKET EVENTS
========================================
*/

io.on("connection", socket => {
    metrics.recordConnection(socket);
    console.log("connected", socket.id);

    socket.onAny((event, ...args) => {
        if (event === "audio:chunk") {
            metrics.recordAudioChunk(socket, args[0]);
        }

        
    });

    socket.on("room:join", async ({ venueId, vid, language, lang, mode, role, clientMode } = {}) => {
        const id = venueId ?? vid;
        const code = language ?? lang;
        const selectedMode = normaliseMode(mode ?? role ?? clientMode);

        if (!id) {
            console.warn("[ROOM] join rejected: missing venueId");
            return;
        }

        // Avoid leaving/rejoining the exact same mode unnecessarily.
        if (
            String(socket.data?.venueId || "") === String(id) &&
            socket.data?.mode === selectedMode &&
            String(socket.data?.language || "") === String(code || "")
        ) {
            return;
        }

        leaveCurrentMode(socket, { silent: true });

        socket.data.venueId = id;
        socket.data.mode = selectedMode;
        socket.data.language = code || null;

        if (selectedMode === "speaker") {
            // Speaker mode sends microphone audio and only joins the realtime
            // room. It does not join attendee language rooms, the TV room, or
            // the general venue room.
            socket.join(realtimeRoom(id));

            await whisper.attachVenue(id, socket);

            socket.emit("mode:joined", {
                venueId: id,
                mode: "speaker",
                language: code || null
            });

            metrics.recordJoin(socket, {
                venueId: id,
                mode: "speaker",
                language: code || null
            });

            console.log(`${socket.id} -> venue:${id}:speaker`);
            return;
        }

        if (selectedMode === "attendee") {
            if (!code) {
                console.warn(`[ROOM] attendee join rejected for venue:${id}: missing language/lang`);
                socket.emit("room:error", {
                    venueId: id,
                    mode: "attendee",
                    message: "Attendee mode requires lang/language."
                });
                socket.data.venueId = null;
                socket.data.mode = null;
                socket.data.language = null;
                return;
            }

            if (!isValidLanguage(code)) {
                console.warn(`[ROOM] invalid attendee language: ${code}`);
                socket.emit("room:error", {
                    venueId: id,
                    mode: "attendee",
                    language: code,
                    message: "Invalid language."
                });
                socket.data.venueId = null;
                socket.data.mode = null;
                socket.data.language = null;
                return;
            }

            socket.join(languageRoom(id, code));
            socket.data.language = code;

            socket.emit("mode:joined", {
                venueId: id,
                mode: "attendee",
                language: code
            });

            metrics.recordJoin(socket, {
                venueId: id,
                mode: "attendee",
                language: code
            });

            emitActiveLanguageState(io, id);

            console.log(`${socket.id} -> venue:${id}:attendee:${code}`);
            return;
        }

        if (selectedMode === "tv") {
            // TV mode receives every translated sentence for every currently
            // active attendee language. It does not create a language room and
            // therefore does not itself trigger extra translations.
            socket.join(tvRoom(id));

            socket.emit("mode:joined", {
                venueId: id,
                mode: "tv"
            });

            metrics.recordJoin(socket, {
                venueId: id,
                mode: "tv"
            });

            emitActiveLanguageState(io, id);

            console.log(`${socket.id} -> venue:${id}:tv`);
        }
    });

    socket.on("room:leave", ({ venueId, vid } = {}) => {
        const id = venueId ?? vid ?? socket.data.venueId;

        if (!id) {
            return;
        }

        leaveCurrentMode(socket);
    });

    socket.on("disconnect", () => {
        leaveCurrentMode(socket, { silent: true });
        metrics.recordDisconnect(socket);
        console.log("disconnected", socket.id);
    });
});

/*
========================================
BACKGROUND STARTUP
========================================
*/

async function startBackgroundServices() {
    try {
        await refreshLanguages();

        setInterval(() => {
            refreshLanguages().catch(error => {
                console.error("[LANGUAGE REFRESH ERROR]", error);
            });
        }, 300000);

        console.log("languages loaded");
    } catch (error) {
        console.error("[LANGUAGE STARTUP ERROR]", error);
    }

    try {
        await whisper.connect();
        console.log("whisper connected");
    } catch (error) {
        console.error("[WHISPER CONNECT ERROR]", error);
    }
}

/*
========================================
START SERVER
========================================
*/

server.listen(PORT, HOST, () => {
    console.log(`server listening on http://${HOST}:${PORT}`);
    startBackgroundServices();
});
