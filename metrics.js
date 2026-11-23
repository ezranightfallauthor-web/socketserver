import {
    realtimeRoom,
    tvRoom
} from "./rooms.js";

const DEFAULT_MAX_EVENTS = 250;
const DEFAULT_AUDIO_ACTIVE_MS = 5000;
const DEFAULT_EMIT_THROTTLE_MS = 1000;

function nowIso() {
    return new Date().toISOString();
}

function normaliseVenueId(value) {
    if (value === undefined || value === null || value === "") {
        return null;
    }

    return String(value);
}

function normaliseLanguage(value) {
    if (!value) return null;
    return String(value).toLowerCase().trim();
}

function normaliseMode(value) {
    const mode = String(value || "unknown").toLowerCase().trim();

    if (["speaker", "attendee", "tv"].includes(mode)) {
        return mode;
    }

    return "unknown";
}

function bufferLength(input) {
    if (!input) return 0;

    if (Buffer.isBuffer(input)) {
        return input.length;
    }

    if (input instanceof ArrayBuffer) {
        return input.byteLength;
    }

    if (ArrayBuffer.isView(input)) {
        return input.byteLength;
    }

    if (Array.isArray(input)) {
        return input.length;
    }

    if (input.type === "Buffer" && Array.isArray(input.data)) {
        return input.data.length;
    }

    if (typeof input === "string") {
        return Buffer.byteLength(input, "base64");
    }

    return 0;
}

function extractAudioPayload(payload = {}) {
    if (Buffer.isBuffer(payload)) return payload;

    return (
        payload.audio ??
        payload.buffer ??
        payload.chunk ??
        payload.data ??
        null
    );
}

function getIp(socket) {
    return (
        socket.handshake?.headers?.["x-forwarded-for"] ||
        socket.handshake?.address ||
        socket.conn?.remoteAddress ||
        null
    );
}

export class MetricsService {
    constructor({
        io,
        maxEvents = Number(process.env.METRICS_MAX_EVENTS || DEFAULT_MAX_EVENTS),
        audioActiveMs = Number(process.env.METRICS_AUDIO_ACTIVE_MS || DEFAULT_AUDIO_ACTIVE_MS),
        emitThrottleMs = Number(process.env.METRICS_EMIT_THROTTLE_MS || DEFAULT_EMIT_THROTTLE_MS)
    } = {}) {
        this.io = io;
        this.maxEvents = maxEvents;
        this.audioActiveMs = audioActiveMs;
        this.emitThrottleMs = emitThrottleMs;

        this.startedAt = nowIso();
        this.connectedSockets = new Map();
        this.venues = new Map();
    }

    getOrCreateVenue(venueId) {
        const id = normaliseVenueId(venueId);

        if (!id) return null;

        let venue = this.venues.get(id);

        if (!venue) {
            venue = {
                venueId: id,
                createdAt: nowIso(),
                updatedAt: nowIso(),
                users: new Map(),
                audio: {
                    active: false,
                    firstChunkAt: null,
                    lastChunkAt: null,
                    lastStartedAt: null,
                    chunkCount: 0,
                    byteCount: 0,
                    streamStartCount: 0,
                    currentSpeakerSocketId: null,
                    lastSampleRate: null,
                    lastChannels: null,
                    lastFormat: null
                },
                translation: {
                    sentenceCount: 0,
                    errorCount: 0,
                    lastTranslatedAt: null,
                    lastErrorAt: null,
                    byLanguage: new Map()
                },
                events: [],
                lastEmitAt: 0
            };

            this.venues.set(id, venue);
        }

        return venue;
    }

    recordConnection(socket) {
        this.connectedSockets.set(socket.id, {
            socketId: socket.id,
            connectedAt: nowIso(),
            userAgent: socket.handshake?.headers?.["user-agent"] || null,
            ip: getIp(socket)
        });
    }

    recordDisconnect(socket) {
        this.connectedSockets.delete(socket.id);
    }

    recordJoin(socket, { venueId, mode, language = null } = {}) {
        const id = normaliseVenueId(venueId);
        const selectedMode = normaliseMode(mode);
        const code = normaliseLanguage(language);
        const venue = this.getOrCreateVenue(id);

        if (!venue) return;

        venue.users.set(socket.id, {
            socketId: socket.id,
            mode: selectedMode,
            language: code,
            joinedAt: nowIso(),
            userAgent: socket.handshake?.headers?.["user-agent"] || null,
            ip: getIp(socket)
        });

        venue.updatedAt = nowIso();

        this.addEvent(id, {
            type: "join",
            socketId: socket.id,
            mode: selectedMode,
            language: code
        });

        this.emitVenueUpdate(id, true);
    }

    recordLeave(socket, { venueId, mode, language = null, reason = "leave" } = {}) {
        const id = normaliseVenueId(venueId);
        const selectedMode = normaliseMode(mode);
        const code = normaliseLanguage(language);
        const venue = this.getOrCreateVenue(id);

        if (!venue) return;

        venue.users.delete(socket.id);
        venue.updatedAt = nowIso();

        this.addEvent(id, {
            type: "leave",
            socketId: socket.id,
            mode: selectedMode,
            language: code,
            reason
        });

        this.emitVenueUpdate(id, true);
    }

    recordAudioChunk(socket, payload = {}) {
        const id = normaliseVenueId(
            payload?.venueId ??
            payload?.vid ??
            socket.data?.venueId
        );

        const venue = this.getOrCreateVenue(id);

        if (!venue) return;

        const audio = extractAudioPayload(payload);
        const bytes = bufferLength(audio);

        if (!bytes) return;

        const now = Date.now();
        const timestamp = new Date(now).toISOString();
        const previousLast = venue.audio.lastChunkAt
            ? Date.parse(venue.audio.lastChunkAt)
            : 0;

        const wasIdle = !previousLast || now - previousLast > this.audioActiveMs;

        if (!venue.audio.firstChunkAt) {
            venue.audio.firstChunkAt = timestamp;
        }

        if (wasIdle) {
            venue.audio.lastStartedAt = timestamp;
            venue.audio.streamStartCount += 1;

            this.addEvent(id, {
                type: "audio:start",
                socketId: socket.id,
                mode: socket.data?.mode || "speaker",
                sampleRate: payload.sampleRate ?? null,
                channels: payload.channels ?? null,
                format: payload.format ?? null
            });
        }

        venue.audio.active = true;
        venue.audio.lastChunkAt = timestamp;
        venue.audio.chunkCount += 1;
        venue.audio.byteCount += bytes;
        venue.audio.currentSpeakerSocketId = socket.id;
        venue.audio.lastSampleRate = payload.sampleRate ?? venue.audio.lastSampleRate;
        venue.audio.lastChannels = payload.channels ?? venue.audio.lastChannels;
        venue.audio.lastFormat = payload.format ?? venue.audio.lastFormat;
        venue.updatedAt = timestamp;

        this.emitVenueUpdate(id, false);
    }

    recordTranslation(venueId, language, payload = {}) {
        const id = normaliseVenueId(venueId);
        const code = normaliseLanguage(language);
        const venue = this.getOrCreateVenue(id);

        if (!venue || !code) return;

        let languageMetrics = venue.translation.byLanguage.get(code);

        if (!languageMetrics) {
            languageMetrics = {
                language: code,
                sentenceCount: 0,
                errorCount: 0,
                lastTranslatedAt: null,
                lastErrorAt: null,
                provider: null,
                translated: null
            };

            venue.translation.byLanguage.set(code, languageMetrics);
        }

        const timestamp = nowIso();

        languageMetrics.sentenceCount += 1;
        languageMetrics.lastTranslatedAt = timestamp;
        languageMetrics.provider = payload.provider ?? null;
        languageMetrics.translated = payload.translated ?? null;

        venue.translation.sentenceCount += 1;
        venue.translation.lastTranslatedAt = timestamp;
        venue.updatedAt = timestamp;

        this.emitVenueUpdate(id, false);
    }

    recordTranslationError(venueId, language, error = {}) {
        const id = normaliseVenueId(venueId);
        const code = normaliseLanguage(language);
        const venue = this.getOrCreateVenue(id);

        if (!venue || !code) return;

        let languageMetrics = venue.translation.byLanguage.get(code);

        if (!languageMetrics) {
            languageMetrics = {
                language: code,
                sentenceCount: 0,
                errorCount: 0,
                lastTranslatedAt: null,
                lastErrorAt: null,
                provider: null,
                translated: null
            };

            venue.translation.byLanguage.set(code, languageMetrics);
        }

        const timestamp = nowIso();

        languageMetrics.errorCount += 1;
        languageMetrics.lastErrorAt = timestamp;
        languageMetrics.provider = error.provider ?? "openai";

        venue.translation.errorCount += 1;
        venue.translation.lastErrorAt = timestamp;
        venue.updatedAt = timestamp;

        this.addEvent(id, {
            type: "translation:error",
            language: code,
            provider: error.provider ?? "openai",
            message: error.message ?? "Translation failed"
        });

        this.emitVenueUpdate(id, true);
    }

    addEvent(venueId, event) {
        const venue = this.getOrCreateVenue(venueId);

        if (!venue) return;

        venue.events.push({
            time: nowIso(),
            venueId: venue.venueId,
            ...event
        });

        while (venue.events.length > this.maxEvents) {
            venue.events.shift();
        }
    }

    getActiveLanguages(venueId) {
        const venue = this.getOrCreateVenue(venueId);

        if (!venue) return [];

        const counts = new Map();

        for (const user of venue.users.values()) {
            if (user.mode !== "attendee" || !user.language) {
                continue;
            }

            counts.set(
                user.language,
                (counts.get(user.language) || 0) + 1
            );
        }

        return [...counts.entries()]
            .map(([language, clients]) => ({
                language,
                clients,
                translating: clients > 0
            }))
            .sort((a, b) => a.language.localeCompare(b.language));
    }

    getVenueSnapshot(venueId) {
        const venue = this.getOrCreateVenue(venueId);

        if (!venue) return null;

        const now = Date.now();
        const lastChunkTime = venue.audio.lastChunkAt
            ? Date.parse(venue.audio.lastChunkAt)
            : 0;

        const users = [...venue.users.values()];
        const speakers = users.filter(user => user.mode === "speaker").length;
        const attendees = users.filter(user => user.mode === "attendee").length;
        const tvs = users.filter(user => user.mode === "tv").length;
        const activeLanguages = this.getActiveLanguages(venue.venueId);

        const translationByLanguage = [...venue.translation.byLanguage.values()]
            .map(item => ({ ...item }))
            .sort((a, b) => a.language.localeCompare(b.language));

        return {
            venueId: venue.venueId,
            createdAt: venue.createdAt,
            updatedAt: venue.updatedAt,
            users: {
                total: users.length,
                speakers,
                attendees,
                tvs
            },
            activeLanguages,
            audio: {
                ...venue.audio,
                active: Boolean(lastChunkTime && now - lastChunkTime <= this.audioActiveMs)
            },
            translation: {
                sentenceCount: venue.translation.sentenceCount,
                errorCount: venue.translation.errorCount,
                lastTranslatedAt: venue.translation.lastTranslatedAt,
                lastErrorAt: venue.translation.lastErrorAt,
                byLanguage: translationByLanguage
            }
        };
    }

    getAllSnapshot() {
        return {
            ok: true,
            serviceStartedAt: this.startedAt,
            now: nowIso(),
            connectedSockets: this.connectedSockets.size,
            venues: [...this.venues.keys()]
                .sort((a, b) => a.localeCompare(b))
                .map(venueId => this.getVenueSnapshot(venueId))
        };
    }

    getVenueEvents(venueId, limit = 100) {
        const venue = this.getOrCreateVenue(venueId);

        if (!venue) return [];

        const safeLimit = Math.max(1, Math.min(Number(limit) || 100, this.maxEvents));

        return venue.events.slice(-safeLimit).reverse();
    }

    emitVenueUpdate(venueId, force = false) {
        if (!this.io) return;

        const venue = this.getOrCreateVenue(venueId);

        if (!venue) return;

        const now = Date.now();

        if (!force && now - venue.lastEmitAt < this.emitThrottleMs) {
            return;
        }

        venue.lastEmitAt = now;

        const snapshot = this.getVenueSnapshot(venue.venueId);
        const languagesPayload = {
            venueId: venue.venueId,
            languages: snapshot.activeLanguages
        };

        // Speaker dashboard / realtime status.
        this.io.to(
            realtimeRoom(
                venue.venueId
            )
        ).emit(
            "metrics:venue",
            snapshot
        );

        this.io.to(
            realtimeRoom(
                venue.venueId
            )
        ).emit(
            "languages:active",
            languagesPayload
        );

        // TV dashboard / all language view.
        this.io.to(
            tvRoom(
                venue.venueId
            )
        ).emit(
            "metrics:venue",
            snapshot
        );

        this.io.to(
            tvRoom(
                venue.venueId
            )
        ).emit(
            "languages:active",
            languagesPayload
        );
    }
}
