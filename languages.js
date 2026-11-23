let codes = new Set();

export async function refreshLanguages() {
    try {

        const response = await fetch(
            process.env.LANGUAGE_ENDPOINT
        );

        const data =
            await response.json();

        codes = new Set(
            data.languages.map(
                language => language.code
            )
        );

        console.log(
            `[languages] loaded ${codes.size}`
        );

    } catch (error) {

        console.error(
            "[languages]",
            error
        );

    }
}

export function isValidLanguage(
    code
) {
    return codes.has(code);
}

export function getLanguageCodes() {
    return [...codes];
}