import { randomUUID } from "node:crypto";

import {
    getLanguageCodes
} from "./languages.js";

import {
    languageRoom,
    tvRoom
} from "./rooms.js";

const OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses";
const TRANSLATION_MODEL = process.env.OPENAI_TRANSLATION_MODEL || "gpt-5.4-nano";

let metricsService = null;

export function setMetricsService(service) {
    metricsService = service || null;
}

export async function processTranscript(
    io,
    venueId,
    text,
    options = {}
) {
    if (!text?.trim()) {
        return;
    }

    const activeLanguages = getActiveLanguages(
        io,
        venueId
    );

    if (!activeLanguages.length) {
        console.log(`[TRANSLATE:${venueId}] no active attendee language rooms`);
        return;
    }

    const sourceLanguage = options.sourceLanguage ?? null;

    const results = await Promise.allSettled(
        activeLanguages.map(
            async language => {
                const result = await translateText(
                    text,
                    language,
                    sourceLanguage
                );

                const payload = {
                    venueId,
                    mode: "attendee",
                    language,
                    sourceLanguage,
                    provider: result.provider,
                    requestId: result.openaiRequestId ?? null,
                    clientRequestId: result.clientRequestId ?? null,
                    text: result.text,
                    originalText: text,
                    translated: result.translated,
                    sentence: true,
                    sentenceFlushReason: options.sentenceFlushReason ?? null
                };

                console.log("[TRANSLATE] emit", {
                    venueId,
                    language,
                    translated: result.translated,
                    chars: result.text?.length ?? 0
                });

                // Attendee mode: one selected language only.
                io.to(
                    languageRoom(
                        venueId,
                        language
                    )
                ).emit(
                    "transcript",
                    payload
                );

                io.to(
                    languageRoom(
                        venueId,
                        language
                    )
                ).emit(
                    "transcript:sentence",
                    payload
                );

                // TV mode: every active language transcript.
                io.to(
                    tvRoom(
                        venueId
                    )
                ).emit(
                    "tv:transcript",
                    {
                        ...payload,
                        mode: "tv"
                    }
                );

                metricsService?.recordTranslation(
                    venueId,
                    language,
                    {
                        provider: result.provider,
                        translated: result.translated,
                        chars: result.text?.length ?? 0
                    }
                );
            }
        )
    );

    results.forEach((result, index) => {
        if (result.status === "rejected") {
            const language = activeLanguages[index];

            console.error(
                `[TRANSLATE:${venueId}:${language}]`,
                result.reason
            );

            const payload = {
                venueId,
                language,
                provider: "openai",
                message:
                    result.reason?.message ||
                    "Translation failed"
            };

            io.to(
                languageRoom(
                    venueId,
                    language
                )
            ).emit(
                "translation:error",
                payload
            );

            io.to(
                tvRoom(
                    venueId
                )
            ).emit(
                "translation:error",
                {
                    ...payload,
                    mode: "tv"
                }
            );

            metricsService?.recordTranslationError(
                venueId,
                language,
                {
                    provider: "openai",
                    message: payload.message
                }
            );
        }
    });

    metricsService?.emitVenueUpdate(venueId, true);
}

export function getActiveLanguages(
    io,
    venueId
) {
    const active = [];

    for (const code of getLanguageCodes()) {
        const room = io.sockets
            .adapter
            .rooms
            .get(
                languageRoom(
                    venueId,
                    code
                )
            );

        if (room?.size) {
            active.push(code);
        }
    }

    return active;
}

export function getActiveLanguageDetails(
    io,
    venueId
) {
    return getLanguageCodes()
        .map(code => {
            const room = io.sockets
                .adapter
                .rooms
                .get(
                    languageRoom(
                        venueId,
                        code
                    )
                );

            return {
                language: code,
                clients: room?.size || 0
            };
        })
        .filter(item => item.clients > 0);
}

export function emitActiveLanguageState(
    io,
    venueId
) {
    if (!venueId) return;

    const languages = getActiveLanguageDetails(
        io,
        venueId
    );

    io.to(
        tvRoom(
            venueId
        )
    ).emit(
        "tv:languages",
        {
            venueId,
            languages
        }
    );
}

function sameLanguage(sourceLanguage, targetLanguage) {
    if (!sourceLanguage || !targetLanguage) {
        return false;
    }

    const source = String(sourceLanguage).toLowerCase().split("-")[0];
    const target = String(targetLanguage).toLowerCase().split("-")[0];

    return source === target;
}

async function translateText(
    text,
    targetLanguage,
    sourceLanguage = null
) {
    if (!text?.trim()) {
        return {
            provider: "openai",
            translated: false,
            text: ""
        };
    }

    if (sameLanguage(sourceLanguage, targetLanguage)) {
        return {
            provider: "openai",
            translated: false,
            text
        };
    }

    const translated = await translateWithOpenAi(
        text,
        targetLanguage,
        sourceLanguage
    );

    return {
        provider: "openai",
        translated: true,
        text: translated.text || text,
        clientRequestId: translated.clientRequestId,
        openaiRequestId: translated.openaiRequestId
    };
}

async function translateWithOpenAi(
    text,
    targetLanguage,
    sourceLanguage = null
) {
    if (!process.env.OPENAI_API_KEY) {
        throw new Error("OPENAI_API_KEY is missing");
    }

    const clientRequestId = randomUUID();

    const response = await fetch(OPENAI_RESPONSES_URL, {
        method: "POST",
        headers: {
            Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
            "Content-Type": "application/json",
            "X-Client-Request-Id": clientRequestId
        },
        body: JSON.stringify({
            model: TRANSLATION_MODEL,
            instructions: [
                "You are a live venue translation engine.",
                "Translate the user's text into the requested target language.",
                "Preserve meaning, names, numbers, tone, and sentence boundaries.",
                "Return only the translated text. Do not add notes, labels, or explanations."
            ].join(" "),
            input: [
                {
                    role: "user",
                    content: [
                        `Target language code: ${targetLanguage}`,
                        sourceLanguage ? `Detected source language: ${sourceLanguage}` : "Detected source language: unknown",
                        "Text:",
                        text
                    ].join("\n")
                }
            ]
        })
    });

    const openaiRequestId = response.headers.get("x-request-id");

    if (!response.ok) {
        const body = await response.text();
        throw new Error(
            `OpenAI translation failed ${response.status}; clientRequestId=${clientRequestId}; openaiRequestId=${openaiRequestId}; ${body}`
        );
    }

    const data = await response.json();
    const translatedText = extractResponseText(data).trim();

    console.log("[TRANSLATE] translated with OpenAI", {
        targetLanguage,
        clientRequestId,
        openaiRequestId
    });

    return {
        text: translatedText || text,
        clientRequestId,
        openaiRequestId
    };
}

function extractResponseText(data) {
    if (!data) return "";

    if (typeof data.output_text === "string") {
        return data.output_text;
    }

    const chunks = [];

    for (const item of data.output || []) {
        for (const content of item.content || []) {
            if (
                content.type === "output_text" &&
                typeof content.text === "string"
            ) {
                chunks.push(content.text);
            }
        }
    }

    return chunks.join("");
}
