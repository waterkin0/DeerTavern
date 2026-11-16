import { Buffer } from 'node:buffer';
import { randomBytes, timingSafeEqual } from 'node:crypto';

export const GENERATION_JOB_ENDPOINTS = new Set([
    '/api/backends/chat-completions/generate',
    '/api/backends/text-completions/generate',
    '/api/backends/kobold/generate',
    '/api/novelai/generate',
]);

export const GENERATION_JOB_RELAY_HEADER = 'x-st-generation-relay';
const relayToken = randomBytes(32);
export const GENERATION_JOB_RELAY_TOKEN = relayToken.toString('hex');

/**
 * Checks whether a request is an authenticated, in-process generation relay.
 * @param {import('express').Request} request
 * @returns {boolean}
 */
export function isTrustedGenerationJobRelay(request) {
    if (request.method !== 'POST' || !GENERATION_JOB_ENDPOINTS.has(request.path)) {
        return false;
    }

    const suppliedToken = request.headers[GENERATION_JOB_RELAY_HEADER];
    if (typeof suppliedToken !== 'string' || suppliedToken.length !== relayToken.length * 2) {
        return false;
    }

    let suppliedBuffer;
    try {
        suppliedBuffer = Buffer.from(suppliedToken, 'hex');
    } catch {
        return false;
    }

    return suppliedBuffer.length === relayToken.length && timingSafeEqual(suppliedBuffer, relayToken);
}
