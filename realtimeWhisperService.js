import WebSocket from "ws";
import { randomUUID } from "node:crypto";

import { realtimeRoom } from "./rooms.js";
import { processTranscript } from "./transcription.js";

const DEFAULT_REALTIME_TRANSCRIPTION_URL =
    "wss://api.openai.com/v1/realtime?intent=transcription";

function resolveRealtimeTranscriptionUrl() {
    const rawUrl =
        process.env.OPENAI_REALTIME_URL ||
        DEFAULT_REALTIME_TRANSCRIPTION_URL;

    try {
        const url = new URL(rawUrl);

        // A transcription session is created by connecting with
        // intent=transcription. If the old realtime model URL is used,
        // OpenAI creates a realtime conversation session instead, and then
        // rejects session.type="transcription" with:
        // "Passing a transcription session update to a realtime session is not allowed."
        if (url.searchParams.get("intent") !== "transcription") {
            if (url.searchParams.has("model")) {
                console.warn(
                    "[OPENAI] OPENAI_REALTIME_URL used a realtime model URL. Rewriting it to intent=transcription for gpt-realtime-whisper."
                );
            }

            url.searchParams.delete("model");
            url.searchParams.set("intent", "transcription");
        }

        return url.toString();
    } catch {
        console.warn(
            `[OPENAI] Invalid OPENAI_REALTIME_URL '${rawUrl}'. Falling back to ${DEFAULT_REALTIME_TRANSCRIPTION_URL}`
        );
        return DEFAULT_REALTIME_TRANSCRIPTION_URL;
    }
}

const REALTIME_URL = resolveRealtimeTranscriptionUrl();

const SAMPLE_RATE = Number(process.env.WHISPER_SAMPLE_RATE || 24000);
const CHANNELS = Number(process.env.WHISPER_CHANNELS || 1);
const BITS_PER_SAMPLE = Number(process.env.WHISPER_BITS_PER_SAMPLE || 16);
const BYTES_PER_SAMPLE = BITS_PER_SAMPLE / 8;

const TRANSCRIPTION_MODEL =
    process.env.OPENAI_TRANSCRIPTION_MODEL || "gpt-realtime-whisper";

const TRANSCRIPTION_DELAY = process.env.OPENAI_TRANSCRIPTION_DELAY || "low";
const TRANSCRIPTION_LANGUAGE = process.env.OPENAI_TRANSCRIPTION_LANGUAGE || "";

// Sentence emission settings. We keep realtime deltas internal by default,
// then emit only complete sentence-sized transcript chunks.
const EMIT_PARTIAL_TRANSCRIPTS =
    String(process.env.EMIT_PARTIAL_TRANSCRIPTS || "false").toLowerCase() === "true";
const SENTENCE_FLUSH_MS = Number(process.env.SENTENCE_FLUSH_MS || 1500);
const SENTENCE_MAX_CHARS = Number(process.env.SENTENCE_MAX_CHARS || 500);

// OpenAI should not be sent tiny appends. With 24kHz mono PCM16,
// 100ms is 4,800 bytes.
const MIN_APPEND_MS = Number(process.env.OPENAI_MIN_APPEND_MS || 100);
const MIN_APPEND_BYTES = Math.floor(
    SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * (MIN_APPEND_MS / 1000)
);

// Minimum audio worth committing to OpenAI after VAD has detected an utterance.
const VAD_MIN_SPEECH_MS = Number(process.env.VAD_MIN_SPEECH_MS || 300);
const MIN_COMMIT_BYTES = Math.floor(
    SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * (VAD_MIN_SPEECH_MS / 1000)
);

// Simple RMS VAD thresholds. You will tune these in production.
const VAD_ENABLED = String(process.env.VAD_ENABLED ?? "true").toLowerCase() !== "false";
const VAD_START_THRESHOLD = Number(process.env.VAD_START_THRESHOLD || 0.012);
const VAD_STOP_THRESHOLD = Number(process.env.VAD_STOP_THRESHOLD || 0.008);
const VAD_SILENCE_MS = Number(process.env.VAD_SILENCE_MS || 700);
const VAD_PREROLL_MS = Number(process.env.VAD_PREROLL_MS || 250);
const VAD_TRAILING_SILENCE_MS = Number(process.env.VAD_TRAILING_SILENCE_MS || 250);

const PREROLL_BYTES = Math.floor(
    SAMPLE_RATE * CHANNELS * BYTES_PER_SAMPLE * (VAD_PREROLL_MS / 1000)
);

function normaliseAudioBuffer(input) {
    if (!input) return null;

    if (Buffer.isBuffer(input)) {
        return input;
    }

    if (input instanceof ArrayBuffer) {
        return Buffer.from(input);
    }

    if (ArrayBuffer.isView(input)) {
        return Buffer.from(input.buffer, input.byteOffset, input.byteLength);
    }

    if (Array.isArray(input)) {
        return Buffer.from(input);
    }

    if (input.type === "Buffer" && Array.isArray(input.data)) {
        return Buffer.from(input.data);
    }

    if (typeof input === "string") {
        return Buffer.from(input, "base64");
    }

    return null;
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

function safeJsonParse(data) {
    try {
        return JSON.parse(data.toString());
    } catch {
        return null;
    }
}

function pcm16Rms(buffer) {
    if (!buffer || buffer.length < 2) {
        return 0;
    }

    let sumSquares = 0;
    let samples = 0;

    for (let i = 0; i + 1 < buffer.length; i += 2) {
        const sample = buffer.readInt16LE(i) / 32768;
        sumSquares += sample * sample;
        samples += 1;
    }

    if (!samples) {
        return 0;
    }

    return Math.sqrt(sumSquares / samples);
}

function bytesToMs(
    bytes,
    sampleRate = SAMPLE_RATE,
    channels = CHANNELS,
    bytesPerSample = BYTES_PER_SAMPLE
) {
    if (!bytes || !sampleRate || !channels || !bytesPerSample) {
        return 0;
    }

    return (bytes / (sampleRate * channels * bytesPerSample)) * 1000;
}

function appendAndTrim(buffer, chunk, maxBytes) {
    if (!maxBytes || maxBytes <= 0) {
        return Buffer.alloc(0);
    }

    const combined = Buffer.concat([buffer, chunk]);

    if (combined.length <= maxBytes) {
        return combined;
    }

    return combined.subarray(combined.length - maxBytes);
}

export class RealtimeWhisperService {
    constructor({ apiKey, io }) {
        this.apiKey = apiKey;
        this.io = io;

        // venueId -> realtime session state
        this.sessions = new Map();

        // venueId -> socketId. Only one socket should own microphone audio per venue.
        this.audioOwner = new Map();
    }

    async connect() {
        if (!this.apiKey) {
            throw new Error("OPENAI_API_KEY is missing from environment variables.");
        }

        console.log("OpenAI Realtime Whisper service ready", {
            realtimeUrl: REALTIME_URL,
            transcriptionModel: TRANSCRIPTION_MODEL,
            sampleRate: SAMPLE_RATE,
            channels: CHANNELS,
            bitsPerSample: BITS_PER_SAMPLE,
            minAppendMs: MIN_APPEND_MS,
            minAppendBytes: MIN_APPEND_BYTES,
            vadEnabled: VAD_ENABLED,
            vadStartThreshold: VAD_START_THRESHOLD,
            vadStopThreshold: VAD_STOP_THRESHOLD,
            vadSilenceMs: VAD_SILENCE_MS,
            vadMinSpeechMs: VAD_MIN_SPEECH_MS,
            vadPrerollMs: VAD_PREROLL_MS,
            vadTrailingSilenceMs: VAD_TRAILING_SILENCE_MS,
            emitPartialTranscripts: EMIT_PARTIAL_TRANSCRIPTS,
            sentenceFlushMs: SENTENCE_FLUSH_MS,
            sentenceMaxChars: SENTENCE_MAX_CHARS
        });
    }

    createBlankSession() {
        return {
            ws: null,
            ready: false,
            connecting: false,
            connectPromise: null,
            lastError: null,
            clientRequestId: null,

            // Audio waiting to be sent to OpenAI. We only append in >=100ms blocks.
            pendingAppendBuffer: Buffer.alloc(0),

            // Audio already appended to OpenAI but not yet committed.
            openaiBufferedBytes: 0,

            vad: {
                speaking: false,
                speechMs: 0,
                silenceMs: 0,
                trailingSilenceMsSent: 0,
                lastRms: 0,
                prerollBuffer: Buffer.alloc(0)
            },

            // Final transcripts from OpenAI are buffered here until a full
            // sentence is available. This prevents the frontend receiving
            // word/phrase fragments.
            sentence: {
                buffer: "",
                flushTimer: null
            }
        };
    }

    getOrCreateSessionRecord(venueId) {
        let session = this.sessions.get(venueId);

        if (!session) {
            session = this.createBlankSession();
            this.sessions.set(venueId, session);
        }

        return session;
    }

    async createVenueSession(venueId) {
        const session = this.getOrCreateSessionRecord(venueId);

        if (
            session.ws &&
            session.ready &&
            session.ws.readyState === WebSocket.OPEN
        ) {
            return session;
        }

        if (session.connecting && session.connectPromise) {
            return session.connectPromise;
        }

        session.connecting = true;
        session.ready = false;
        session.clientRequestId = randomUUID();

        session.connectPromise = new Promise((resolve, reject) => {
            const ws = new WebSocket(REALTIME_URL, {
                headers: {
                    Authorization: `Bearer ${this.apiKey}`,
                    "X-Client-Request-Id": session.clientRequestId
                }
            });

            session.ws = ws;

            const failConnect = error => {
                session.connecting = false;
                session.ready = false;
                session.lastError = error;
                reject(error);
            };

            ws.on("open", () => {
                console.log(`[OPENAI:${venueId}] realtime connected`, {
                    clientRequestId: session.clientRequestId
                });

                ws.send(JSON.stringify({
                    type: "session.update",
                    session: {
                        type: "transcription",
                        audio: {
                            input: {
                                format: {
                                    type: "audio/pcm",
                                    rate: SAMPLE_RATE
                                },
                                transcription: {
                                    model: TRANSCRIPTION_MODEL,
                                    ...(TRANSCRIPTION_LANGUAGE ? { language: TRANSCRIPTION_LANGUAGE } : {}),
                                    ...(TRANSCRIPTION_DELAY ? { delay: TRANSCRIPTION_DELAY } : {})
                                },

                                // Required for gpt-realtime-whisper. The socket server
                                // decides when to commit using its own VAD below.
                                turn_detection: null
                            }
                        }
                    }
                }));

                session.ready = true;
                session.connecting = false;
                resolve(session);
            });

            ws.on("message", data => {
                this.handleRealtimeEvent(venueId, data);
            });

            ws.on("error", error => {
                console.error(`[OPENAI:${venueId}] realtime websocket error`, error);
                session.lastError = error;

                this.io.to(realtimeRoom(venueId)).emit("transcript:error", {
                    venueId,
                    provider: "openai-realtime",
                    message: error.message || "OpenAI realtime websocket error"
                });

                if (!session.ready) {
                    failConnect(error);
                }
            });

            ws.on("close", (code, reason) => {
                console.log(
                    `[OPENAI:${venueId}] realtime closed`,
                    code,
                    reason.toString()
                );

                session.ready = false;
                session.connecting = false;
                session.ws = null;
            });
        });

        return session.connectPromise;
    }

    handleRealtimeEvent(venueId, data) {
        const event = safeJsonParse(data);

        if (!event) {
            console.warn(`[OPENAI:${venueId}] non-json realtime message`, data.toString());
            return;
        }

        if (event.type === "error") {
            console.error(
                `[OPENAI:${venueId}] realtime error`,
                JSON.stringify(event.error || event, null, 2)
            );

            this.io.to(realtimeRoom(venueId)).emit("transcript:error", {
                venueId,
                provider: "openai-realtime",
                message:
                    event.error?.message ||
                    event.message ||
                    "OpenAI realtime transcription error"
            });

            return;
        }

        if (event.type === "session.updated") {
            console.log(`[OPENAI:${venueId}] realtime session updated`);
            return;
        }

        if (event.type === "conversation.item.input_audio_transcription.delta") {
            const text = event.delta || "";

            if (!text) return;

            // gpt-realtime-whisper can send deltas, but for the public client
            // we normally suppress them so the UI receives sentence-sized
            // transcript chunks only. Enable this env var only for debugging.
            if (EMIT_PARTIAL_TRANSCRIPTS) {
                this.io.to(realtimeRoom(venueId)).emit("transcript:partial", {
                    venueId,
                    provider: "openai-realtime",
                    itemId: event.item_id ?? null,
                    text
                });
            }

            return;
        }

        if (event.type === "conversation.item.input_audio_transcription.completed") {
            const text = (event.transcript || "").trim();

            if (!text) return;

            console.log(`[OPENAI:${venueId}] realtime transcript completed`, {
                itemId: event.item_id ?? null,
                chars: text.length
            });

            this.bufferTranscriptForSentences(venueId, text, {
                itemId: event.item_id ?? null,
                sourceLanguage: TRANSCRIPTION_LANGUAGE || null
            });
        }
    }


    bufferTranscriptForSentences(venueId, text, options = {}) {
        const session = this.sessions.get(venueId);

        if (!session || !text?.trim()) {
            return;
        }

        const incoming = text.trim().replace(/\s+/g, " ");
        const existing = session.sentence.buffer.trim();

        session.sentence.buffer = existing
            ? `${existing} ${incoming}`.replace(/\s+/g, " ")
            : incoming;

        const extracted = this.extractCompleteSentences(session.sentence.buffer);
        session.sentence.buffer = extracted.remainder;

        for (const sentence of extracted.sentences) {
            this.emitSentenceTranscript(venueId, sentence, options);
        }

        if (session.sentence.buffer.length >= SENTENCE_MAX_CHARS) {
            this.flushSentenceBuffer(venueId, "max-chars", options);
            return;
        }

        if (session.sentence.buffer) {
            this.scheduleSentenceFlush(venueId, options);
        }
    }

    extractCompleteSentences(text) {
        const input = String(text || "").trim();

        if (!input) {
            return {
                sentences: [],
                remainder: ""
            };
        }

        const terminals = new Set([".", "!", "?", "…", "。", "！", "？"]);
        const closers = new Set(["\"", "'", "”", "’", ")", "]", "}"]);
        const abbreviations = new Set([
            "mr", "mrs", "ms", "dr", "prof", "sr", "jr", "st", "vs", "etc", "eg", "ie"
        ]);

        const sentences = [];
        let start = 0;

        for (let i = 0; i < input.length; i += 1) {
            const char = input[i];

            if (!terminals.has(char)) {
                continue;
            }

            // Avoid splitting decimal numbers like 3.14.
            if (
                char === "." &&
                /\d/.test(input[i - 1] || "") &&
                /\d/.test(input[i + 1] || "")
            ) {
                continue;
            }

            const beforeTerminal = input.slice(start, i).trim();
            const lastWord = (
                beforeTerminal.match(/([A-Za-z]{1,8})$/)?.[1] || ""
            ).toLowerCase();

            if (char === "." && abbreviations.has(lastWord)) {
                continue;
            }

            let end = i + 1;

            while (end < input.length && closers.has(input[end])) {
                end += 1;
            }

            const nextChar = input[end] || "";

            if (nextChar && !/\s/.test(nextChar)) {
                continue;
            }

            const sentence = input.slice(start, end).trim();

            if (sentence) {
                sentences.push(sentence);
            }

            while (end < input.length && /\s/.test(input[end])) {
                end += 1;
            }

            start = end;
            i = end - 1;
        }

        return {
            sentences,
            remainder: input.slice(start).trim()
        };
    }

    scheduleSentenceFlush(venueId, options = {}) {
        const session = this.sessions.get(venueId);

        if (!session) return;

        if (session.sentence.flushTimer) {
            clearTimeout(session.sentence.flushTimer);
        }

        if (SENTENCE_FLUSH_MS <= 0) {
            return;
        }

        session.sentence.flushTimer = setTimeout(() => {
            this.flushSentenceBuffer(venueId, "timeout", options);
        }, SENTENCE_FLUSH_MS);
    }

    flushSentenceBuffer(venueId, reason = "manual", options = {}) {
        const session = this.sessions.get(venueId);

        if (!session) return;

        if (session.sentence.flushTimer) {
            clearTimeout(session.sentence.flushTimer);
            session.sentence.flushTimer = null;
        }

        const text = session.sentence.buffer.trim();
        session.sentence.buffer = "";

        if (!text) {
            return;
        }

        this.emitSentenceTranscript(venueId, text, {
            ...options,
            sentenceFlushReason: reason
        });
    }

    emitSentenceTranscript(venueId, sentence, options = {}) {
        const text = String(sentence || "").trim();

        if (!text) {
            return;
        }

        console.log(`[OPENAI:${venueId}] sentence transcript ready`, {
            chars: text.length,
            itemId: options.itemId ?? null,
            reason: options.sentenceFlushReason ?? "punctuation"
        });

        const payload = {
            venueId,
            provider: "openai-realtime",
            model: TRANSCRIPTION_MODEL,
            itemId: options.itemId ?? null,
            text,
            sourceLanguage: options.sourceLanguage ?? null,
            translated: false,
            sentence: true,
            sentenceFlushReason: options.sentenceFlushReason ?? "punctuation"
        };

        // New explicit event for sentence-based frontends.
        this.io.to(realtimeRoom(venueId)).emit("transcript:sentence", payload);

        // Backwards-compatible event name, now sentence-sized rather than
        // utterance-sized or partial-sized.
        this.io.to(realtimeRoom(venueId)).emit("transcript", payload);

        void processTranscript(
            this.io,
            venueId,
            text,
            {
                sourceLanguage: options.sourceLanguage ?? null,
                sentenceFlushReason: options.sentenceFlushReason ?? "punctuation"
            }
        ).catch(error => {
            console.error(`[TRANSLATE:${venueId}]`, error);
        });
    }

    async attachVenue(venueId, socket) {
        if (!venueId || !socket) return;

        await this.createVenueSession(venueId);

        if (!socket.data.whisperAttachedVenues) {
            socket.data.whisperAttachedVenues = new Set();
        }

        const attachKey = String(venueId);

        if (socket.data.whisperAttachedVenues.has(attachKey)) {
            return;
        }

        socket.data.whisperAttachedVenues.add(attachKey);

        console.log(`[WHISPER:${venueId}] attached realtime VAD listener for socket ${socket.id}`);

        socket.on("audio:chunk", async (payload = {}) => {
            if (socket.data?.mode && socket.data.mode !== "speaker") {
                return;
            }

            const payloadVenueId = payload.venueId ?? payload.vid ?? venueId;

            if (String(payloadVenueId) !== String(venueId)) {
                return;
            }

            const rawAudio = extractAudioPayload(payload);
            const audioBuffer = normaliseAudioBuffer(rawAudio);

            if (!audioBuffer || audioBuffer.length === 0) {
                console.warn(`[WHISPER:${venueId}] audio:chunk missing valid audio/buffer`);
                return;
            }

            if (!this.audioOwner.has(venueId)) {
                this.audioOwner.set(venueId, socket.id);
                console.log(`🔒 venue ${venueId} audio owner: ${socket.id}`);
            }

            if (this.audioOwner.get(venueId) !== socket.id) {
                return;
            }

            await this.sendAudioChunk(venueId, audioBuffer, {
                sampleRate: payload.sampleRate || SAMPLE_RATE,
                channels: payload.channels || CHANNELS,
                format: payload.format || "pcm_s16le"
            });
        });

        // Optional manual fallback. VAD will usually commit automatically
        // when it sees enough silence after speech.
        socket.on("audio:commit", async (payload = {}) => {
            if (socket.data?.mode && socket.data.mode !== "speaker") {
                return;
            }

            const payloadVenueId = payload.venueId ?? payload.vid ?? venueId;

            if (String(payloadVenueId) !== String(venueId)) {
                return;
            }

            await this.commitVenueAudio(venueId, "manual", { force: true });
        });

        socket.on("disconnect", () => {
            if (this.audioOwner.get(venueId) === socket.id) {
                this.audioOwner.delete(venueId);
                console.log(`🔓 venue ${venueId} unlocked`);
            }
        });
    }

    async sendAudioChunk(venueId, audioBuffer, meta = {}) {
        const session = this.sessions.get(venueId);

        if (!session) {
            console.warn(`[WHISPER:${venueId}] no realtime session`);
            return;
        }

        if (!session.ws || session.ws.readyState !== WebSocket.OPEN) {
            console.warn(`[OPENAI:${venueId}] cannot send audio; realtime websocket not open`);
            return;
        }

        const sampleRate = Number(meta.sampleRate || SAMPLE_RATE);
        const channels = Number(meta.channels || CHANNELS);
        const bytesPerSample = BYTES_PER_SAMPLE;

        if (sampleRate !== SAMPLE_RATE || channels !== CHANNELS) {
            console.warn(`[WHISPER:${venueId}] audio format does not match realtime session`, {
                receivedSampleRate: sampleRate,
                expectedSampleRate: SAMPLE_RATE,
                receivedChannels: channels,
                expectedChannels: CHANNELS,
                format: meta.format
            });
        }

        if (!VAD_ENABLED) {
            this.queueAudioForOpenAI(venueId, session, audioBuffer);
            return;
        }

        await this.processVadAudio(venueId, session, audioBuffer, {
            sampleRate,
            channels,
            bytesPerSample
        });
    }

    async processVadAudio(venueId, session, audioBuffer, meta) {
        const rms = pcm16Rms(audioBuffer);
        const chunkMs = bytesToMs(
            audioBuffer.length,
            meta.sampleRate,
            meta.channels,
            meta.bytesPerSample
        );

        session.vad.lastRms = rms;

        const isSpeech = session.vad.speaking
            ? rms >= VAD_STOP_THRESHOLD
            : rms >= VAD_START_THRESHOLD;

        if (isSpeech) {
            if (!session.vad.speaking) {
                console.log(`[WHISPER:${venueId}] VAD speech started`, {
                    rms: Number(rms.toFixed(5)),
                    prerollBytes: session.vad.prerollBuffer.length
                });

                // Include a little audio from before the threshold was crossed
                // so the first syllable is less likely to be clipped.
                if (session.vad.prerollBuffer.length > 0) {
                    this.queueAudioForOpenAI(
                        venueId,
                        session,
                        session.vad.prerollBuffer
                    );
                    session.vad.prerollBuffer = Buffer.alloc(0);
                }
            }

            session.vad.speaking = true;
            session.vad.speechMs += chunkMs;
            session.vad.silenceMs = 0;
            session.vad.trailingSilenceMsSent = 0;

            this.queueAudioForOpenAI(venueId, session, audioBuffer);

            return;
        }

        if (session.vad.speaking) {
            session.vad.silenceMs += chunkMs;

            // Feed a small amount of trailing silence to Whisper so utterance
            // endings are not chopped too aggressively.
            if (session.vad.trailingSilenceMsSent < VAD_TRAILING_SILENCE_MS) {
                this.queueAudioForOpenAI(venueId, session, audioBuffer);
                session.vad.trailingSilenceMsSent += chunkMs;
            }

            if (
                session.vad.silenceMs >= VAD_SILENCE_MS &&
                session.vad.speechMs >= VAD_MIN_SPEECH_MS
            ) {
                console.log(`[WHISPER:${venueId}] VAD speech ended`, {
                    speechMs: Math.round(session.vad.speechMs),
                    silenceMs: Math.round(session.vad.silenceMs),
                    rms: Number(rms.toFixed(5))
                });

                this.flushAppendBuffer(venueId, session, { padFinal: true });
                await this.commitVenueAudio(venueId, "vad-silence");
                this.resetVadState(session);
            }

            return;
        }

        // Not speaking and still silence/noise: keep a small pre-roll buffer,
        // but do not send it to OpenAI yet.
        session.vad.prerollBuffer = appendAndTrim(
            session.vad.prerollBuffer,
            audioBuffer,
            PREROLL_BYTES
        );

        if (process.env.VAD_DEBUG === "true") {
            console.log(`[WHISPER:${venueId}] VAD idle`, {
                rms: Number(rms.toFixed(5)),
                chunkMs: Math.round(chunkMs),
                prerollBytes: session.vad.prerollBuffer.length
            });
        }
    }

    resetVadState(session) {
        session.vad.speaking = false;
        session.vad.speechMs = 0;
        session.vad.silenceMs = 0;
        session.vad.trailingSilenceMsSent = 0;
        session.vad.lastRms = 0;
        session.vad.prerollBuffer = Buffer.alloc(0);
    }

    queueAudioForOpenAI(venueId, session, audioBuffer) {
        if (!audioBuffer || audioBuffer.length === 0) {
            return;
        }

        session.pendingAppendBuffer = Buffer.concat([
            session.pendingAppendBuffer,
            audioBuffer
        ]);

        console.log(
            `[WHISPER:${venueId}] queued ${audioBuffer.length} bytes; pending append ${session.pendingAppendBuffer.length}/${MIN_APPEND_BYTES}; openai buffered ${session.openaiBufferedBytes}/${MIN_COMMIT_BYTES}`
        );

        this.flushAppendBuffer(venueId, session, { padFinal: false });
    }

    flushAppendBuffer(venueId, session, { padFinal = false } = {}) {
        if (!session?.ws || session.ws.readyState !== WebSocket.OPEN) {
            return;
        }

        if (!session.pendingAppendBuffer?.length) {
            return;
        }

        if (session.pendingAppendBuffer.length < MIN_APPEND_BYTES) {
            if (!padFinal) {
                return;
            }

            const paddingBytes = MIN_APPEND_BYTES - session.pendingAppendBuffer.length;

            session.pendingAppendBuffer = Buffer.concat([
                session.pendingAppendBuffer,
                Buffer.alloc(paddingBytes, 0)
            ]);

            console.log(
                `[WHISPER:${venueId}] padded final realtime append with ${paddingBytes} silence bytes to reach ${MIN_APPEND_MS}ms`
            );
        }

        while (session.pendingAppendBuffer.length >= MIN_APPEND_BYTES) {
            const chunk = session.pendingAppendBuffer.subarray(0, MIN_APPEND_BYTES);
            session.pendingAppendBuffer = session.pendingAppendBuffer.subarray(MIN_APPEND_BYTES);

            session.ws.send(JSON.stringify({
                type: "input_audio_buffer.append",
                audio: chunk.toString("base64")
            }));

            session.openaiBufferedBytes += chunk.length;

            console.log(
                `[WHISPER:${venueId}] appended ${chunk.length} bytes to OpenAI; openai buffered ${session.openaiBufferedBytes}/${MIN_COMMIT_BYTES} min bytes; pending ${session.pendingAppendBuffer.length}`
            );
        }
    }

    async commitVenueAudio(venueId, reason = "manual", { force = false } = {}) {
        const session = this.sessions.get(venueId);

        if (!session) return;

        this.flushAppendBuffer(venueId, session, { padFinal: true });

        if (!session.openaiBufferedBytes) {
            return;
        }

        if (!force && session.openaiBufferedBytes < MIN_COMMIT_BYTES) {
            console.log(`[WHISPER:${venueId}] commit skipped; buffered ${session.openaiBufferedBytes}/${MIN_COMMIT_BYTES} min bytes`);
            return;
        }

        if (!session.ws || session.ws.readyState !== WebSocket.OPEN) {
            console.warn(`[OPENAI:${venueId}] cannot commit audio; realtime websocket not open`);
            return;
        }

        const bytes = session.openaiBufferedBytes;
        session.openaiBufferedBytes = 0;

        session.ws.send(JSON.stringify({
            type: "input_audio_buffer.commit"
        }));

        console.log(`[WHISPER:${venueId}] committed realtime audio`, {
            reason,
            bytes
        });

        this.io.to(realtimeRoom(venueId)).emit("transcript:processing", {
            venueId,
            provider: "openai-realtime",
            reason,
            bytes
        });
    }

    releaseAudioOwner(venueId, socketId) {
        const currentOwner = this.audioOwner.get(venueId);

        if (currentOwner === socketId) {
            this.audioOwner.delete(venueId);
            console.log(`🔓 venue ${venueId} audio owner released: ${socketId}`);
            return true;
        }

        return false;
    }

    async detachVenue(venueId) {
        const session = this.sessions.get(venueId);

        if (!session) return;

        if (session.pendingAppendBuffer?.length || session.openaiBufferedBytes) {
            console.log(`[WHISPER:${venueId}] discarding uncommitted realtime audio on detach`, {
                pendingAppendBytes: session.pendingAppendBuffer?.length || 0,
                openaiBufferedBytes: session.openaiBufferedBytes || 0
            });
        }

        if (session.sentence?.flushTimer) {
            clearTimeout(session.sentence.flushTimer);
            session.sentence.flushTimer = null;
        }

        if (session.sentence?.buffer?.trim()) {
            this.flushSentenceBuffer(venueId, "detach");
        }

        if (session.ws && session.ws.readyState === WebSocket.OPEN) {
            session.ws.close();
        }

        this.sessions.delete(venueId);
        this.audioOwner.delete(venueId);

        console.log(`[WHISPER:${venueId}] detached`);
    }
}
