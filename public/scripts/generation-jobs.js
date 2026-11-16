const nativeFetch = window.fetch.bind(window);
const generationEndpoints = new Set([
    '/api/backends/chat-completions/generate',
    '/api/backends/text-completions/generate',
    '/api/backends/kobold/generate',
    '/api/novelai/generate',
]);
const activeJobIds = new Set();
const terminalJobIds = new Set();
let currentContext = {};
let pendingResumeId = null;

class NonRetryableGenerationStreamError extends Error { }

function dispatchJobStarted(id, metadata = {}) {
    terminalJobIds.delete(id);
    activeJobIds.add(id);
    metadata.onJobStarted?.(id);
    window.dispatchEvent(new CustomEvent('st-generation-job-started', { detail: { id } }));
}

function toHeaders(headers) {
    try {
        return new Headers(headers || {});
    } catch {
        return new Headers();
    }
}

function getPath(input) {
    try {
        return new URL(typeof input === 'string' ? input : input.url, window.location.href).pathname;
    } catch {
        return '';
    }
}

function abortError() {
    return new DOMException('The generation request was aborted.', 'AbortError');
}

function wait(ms, signal) {
    return new Promise((resolve, reject) => {
        if (signal?.aborted) {
            reject(abortError());
            return;
        }
        const timeout = setTimeout(done, ms);
        function done() {
            signal?.removeEventListener('abort', onAbort);
            resolve();
        }
        function onAbort() {
            clearTimeout(timeout);
            reject(abortError());
        }
        signal?.addEventListener('abort', onAbort, { once: true });
    });
}

async function cancelJob(id, headers) {
    try {
        await nativeFetch('/api/generation-jobs/' + encodeURIComponent(id) + '/cancel', {
            method: 'POST',
            headers,
            cache: 'no-store',
        });
    } catch {
        // The job remains recoverable if the browser cannot deliver the cancel request.
    }
    activeJobIds.delete(id);
    terminalJobIds.delete(id);
}

async function createJob(endpoint, payload, headers, metadata) {
    const response = await nativeFetch('/api/generation-jobs', {
        method: 'POST',
        headers,
        cache: 'no-store',
        body: JSON.stringify({
            endpoint,
            payload,
            metadata,
        }),
    });
    if (!response.ok) {
        let message = 'Could not start a background generation job.';
        try {
            const data = await response.json();
            message = data.error || message;
        } catch {
            // Keep the default message if the server did not return JSON.
        }
        throw new Error(message);
    }
    const data = await response.json();
    dispatchJobStarted(data.id, metadata);
    return data.id;
}

function parseEventFrame(frame) {
    const result = { event: 'message', id: null, data: [] };
    for (const line of frame.split(/\r?\n/)) {
        if (line.startsWith(':')) continue;
        const separator = line.indexOf(':');
        const field = separator < 0 ? line : line.slice(0, separator);
        let value = separator < 0 ? '' : line.slice(separator + 1);
        if (value.startsWith(' ')) value = value.slice(1);
        if (field === 'event') result.event = value;
        if (field === 'id') result.id = value;
        if (field === 'data') result.data.push(value);
    }
    return result;
}

function decodeBase64(value) {
    const binary = atob(value);
    const result = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index++) {
        result[index] = binary.charCodeAt(index);
    }
    return result;
}

async function openStream(id, after, headers, signal) {
    return nativeFetch('/api/generation-jobs/' + encodeURIComponent(id) + '/stream?after=' + after, {
        method: 'GET',
        headers,
        cache: 'no-store',
        signal,
    });
}

function isRetryableStatus(status) {
    return status === 408 || status === 425 || status === 429 || status >= 500;
}

async function connectToJobStream(id, after, headers, signal, retryState) {
    while (true) {
        if (signal?.aborted) throw abortError();

        try {
            const response = await openStream(id, after, headers, signal);
            if (!response.ok) {
                const message = 'Could not reconnect to generation job (' + response.status + ').';
                if (!isRetryableStatus(response.status)) {
                    throw new NonRetryableGenerationStreamError(message);
                }
                throw new Error(message);
            }

            if (!response.headers.get('content-type')?.toLowerCase().includes('text/event-stream')) {
                throw new NonRetryableGenerationStreamError('The generation job returned an unexpected response. Refresh the page and sign in again.');
            }
            return response;
        } catch (error) {
            if (signal?.aborted) throw abortError();
            if (error instanceof NonRetryableGenerationStreamError) throw error;
            await wait(retryState.delay, signal);
            retryState.delay = Math.min(retryState.delay * 2, 10000);
        }
    }
}

function makeReplayStream(id, firstResponse, headers, signal, generationResponseStatus) {
    let stopped = false;
    const drainWaiters = [];
    const stream = new ReadableStream({
        start(controller) {
            const errorAfterQueuedChunks = async error => {
                while (!stopped && controller.desiredSize <= 0) {
                    await new Promise(resolve => drainWaiters.push(resolve));
                }
                if (!stopped) controller.error(error);
            };
            let after = 0;
            let nextResponse = firstResponse;
            const retryState = { delay: 500 };

            const pump = async () => {
                while (!stopped) {
                    if (signal?.aborted) throw abortError();

                    let response;
                    try {
                        response = nextResponse || await connectToJobStream(id, after, headers, signal, retryState);
                    } catch (error) {
                        if (!stopped) throw error;
                        return;
                    }
                    nextResponse = null;
                    try {
                        const reader = response.body?.getReader();
                        if (!reader) throw new Error('The generation job stream was closed.');

                        const decoder = new TextDecoder();
                        let buffer = '';
                        let ended = false;

                        while (!ended && !stopped) {
                            const { done, value } = await reader.read();
                            if (done) break;
                            buffer += decoder.decode(value, { stream: true });
                            let boundary = buffer.search(/\r?\n\r?\n/);
                            while (boundary >= 0) {
                                const frame = buffer.slice(0, boundary);
                                const delimiter = buffer.slice(boundary).match(/^\r?\n\r?\n/)[0];
                                buffer = buffer.slice(boundary + delimiter.length);
                                const parsed = parseEventFrame(frame);
                                const data = parsed.data.join('\n');

                                if (parsed.event === 'chunk' && parsed.id) {
                                    const sequence = Number(parsed.id);
                                    if (Number.isSafeInteger(sequence) && sequence > after && data) {
                                        let envelope;
                                        try {
                                            envelope = JSON.parse(data);
                                            controller.enqueue(decodeBase64(envelope.data));
                                        } catch (error) {
                                            throw new NonRetryableGenerationStreamError('The saved generation output could not be decoded: ' + error.message);
                                        }
                                        after = sequence;
                                        retryState.delay = 500;
                                    }
                                } else if (parsed.event === 'end') {
                                    let endData;
                                    try {
                                        endData = data ? JSON.parse(data) : {};
                                    } catch (error) {
                                        throw new NonRetryableGenerationStreamError('The generation job ended with invalid status data: ' + error.message);
                                    }

                                    terminalJobIds.add(id);
                                    activeJobIds.add(id);
                                    ended = true;
                                    if (endData.status === 'complete' || (endData.status === 'failed' && generationResponseStatus >= 400)) {
                                        controller.close();
                                    } else {
                                        await errorAfterQueuedChunks(new Error(endData.message || (endData.status === 'interrupted'
                                            ? 'Generation was interrupted. The saved partial response is available.'
                                            : 'Generation did not complete.')));
                                    }
                                    return;
                                }
                                boundary = buffer.search(/\r?\n\r?\n/);
                            }
                        }

                        if (stopped) return;
                        if (signal?.aborted) throw abortError();
                    } catch (error) {
                        if (stopped) return;
                        if (signal?.aborted || error instanceof NonRetryableGenerationStreamError) throw error;
                        // A transient read failure may occur after some frames arrived. The event cursor
                        // lets the next connection replay only the missing frames.
                    }

                    await wait(retryState.delay, signal);
                    retryState.delay = Math.min(retryState.delay * 2, 10000);
                }
            };

            void pump().catch(error => {
                void errorAfterQueuedChunks(error);
            });
        },
        cancel() {
            stopped = true;
            for (const resolve of drainWaiters.splice(0)) resolve();
        },
        pull() {
            for (const resolve of drainWaiters.splice(0)) resolve();
        },
    });
    return stream;
}

async function getAttachedResponse(id, headers, signal) {
    const response = await connectToJobStream(id, 0, headers, signal, { delay: 500 });

    const status = Number(response.headers.get('X-Generation-Response-Status')) || 200;
    const statusText = response.headers.get('X-Generation-Response-Status-Text') || '';
    const contentType = response.headers.get('X-Generation-Response-Type') || 'application/octet-stream';
    const responseHeaders = new Headers({ 'Content-Type': contentType });
    const body = makeReplayStream(id, response, headers, signal, status);
    return new Response(body, { status, statusText, headers: responseHeaders });
}

async function fetchGeneration(input, init) {
    const endpoint = getPath(input);
    const headers = toHeaders(init.headers || (input instanceof Request ? input.headers : undefined));
    const signal = init.signal || (input instanceof Request ? input.signal : undefined);
    const payload = init.body;

    if (
        typeof payload !== 'string' ||
        !currentContext.generationType ||
        ['quiet', 'impersonate'].includes(currentContext.generationType)
    ) {
        return nativeFetch(input, init);
    }

    const metadata = currentContext;
    currentContext = {};
    let id = pendingResumeId;
    if (id) {
        pendingResumeId = null;
        dispatchJobStarted(id, metadata);
    } else {
        id = await createJob(endpoint, payload, headers, metadata);
    }

    if (signal?.aborted) {
        void cancelJob(id, headers);
        throw abortError();
    }
    return getAttachedResponse(id, headers, signal);
}

window.fetch = function (input, init = {}) {
    const method = String(init.method || (input instanceof Request ? input.method : 'GET')).toUpperCase();
    if (method === 'POST' && generationEndpoints.has(getPath(input))) {
        return fetchGeneration(input, init);
    }
    return nativeFetch(input, init);
};

export function setGenerationJobContext(context) {
    currentContext = { ...(context || {}) };
}

export function setPendingGenerationJob(id) {
    pendingResumeId = id || null;
}

export function cancelActiveGenerationJobs(headers) {
    for (const id of [...activeJobIds]) {
        void cancelJob(id, toHeaders(headers));
    }
}

export async function getRecoverableGenerationJobs(headers) {
    const response = await nativeFetch('/api/generation-jobs/recoverable', {
        method: 'GET',
        headers: toHeaders(headers),
        cache: 'no-store',
    });
    if (!response.ok) return [];
    return response.json();
}

export async function acknowledgeGenerationJobs(headers) {
    const ids = [...activeJobIds].filter(id => terminalJobIds.has(id));
    for (const id of ids) {
        await acknowledgeGenerationJob(id, headers);
    }
}

export async function acknowledgeGenerationJob(id, headers) {
    try {
        await nativeFetch('/api/generation-jobs/' + encodeURIComponent(id) + '/ack', {
            method: 'POST',
            headers: toHeaders(headers),
            cache: 'no-store',
        });
    } catch {
        // The chat save remains authoritative; a later page load can retry delivery.
    }
    activeJobIds.delete(id);
    terminalJobIds.delete(id);
}

export function forgetGenerationJobTracking() {
    activeJobIds.clear();
    terminalJobIds.clear();
}
