export function venueRoom(venueId) {
    return `venue:${venueId}`;
}

// Realtime room is for speaker / microphone sockets only.
// It receives OpenAI realtime status, source transcripts, and errors.
export function realtimeRoom(venueId) {
    return `venue:${venueId}:realtime`;
}

// Backwards-compatible alias for older server code.
export function speakerRoom(venueId) {
    return realtimeRoom(venueId);
}

export function languageRoom(
    venueId,
    language
) {
    return `venue:${venueId}:lang:${language}`;
}

export function tvRoom(venueId) {
    return `venue:${venueId}:tv`;
}
