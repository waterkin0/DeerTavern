import fs from 'node:fs';
import path from 'node:path';
import https from 'node:https';
import { randomUUID } from 'node:crypto';
import express from 'express';
import fetch from 'node-fetch';
import {
    GENERATION_JOB_ENDPOINTS,
    GENERATION_JOB_RELAY_HEADER,
    GENERATION_JOB_RELAY_TOKEN,
} from '../generation-job-relay.js';

const router = express.Router();
const MAX_ACTIVE_JOBS_PER_USER = 8;
const MAX_JOB_OUTPUT_BYTES = 64 * 1024 * 1024;
const JOB_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const PERSIST_INTERVAL_MS = 250;
const httpsAgent = new https.Agent({ keepAlive: true, rejectUnauthorized: false });
const jobs = new Map();

function getOwnerDirectory(owner) {
    const safeOwner = String(owner).replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(globalThis.DATA_ROOT, '_generation-jobs', safeOwner);
}

function getJobPath(owner, id) {
    return path.join(getOwnerDirectory(owner), id + '.jsonl');
}

function createDeferred() {
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    return { promise, resolve };
}

function persistLine(job, record) {
    job.persistQueue = job.persistQueue.then(() => fs.promises.appendFile(
        job.filePath,
        JSON.stringify(record) + '\n',
        'utf8',
    ));
    return job.persistQueue;
}

function notifyChunk(job, event) {
    const message = 'id: ' + event.seq + '\nevent: chunk\ndata: ' + JSON.stringify({ data: event.data }) + '\n\n';
    for (const subscriber of [...job.subscribers]) {
        if (subscriber.destroyed) {
            job.subscribers.delete(subscriber);
            continue;
        }
        subscriber.write(message);
    }
}

function notifyEnd(job) {
    const payload = JSON.stringify({ status: job.status, message: job.error });
    const message = 'event: end\ndata: ' + payload + '\n\n';
    for (const subscriber of [...job.subscribers]) {
        if (!subscriber.destroyed) {
            subscriber.write(message);
            subscriber.end();
        }
        job.subscribers.delete(subscriber);
    }
}

function setResponseDetails(job, status, statusText, contentType) {
    if (job.responseDetails) return;
    job.responseDetails = {
        status: Number(status) || 200,
        statusText: String(statusText || ''),
        contentType: String(contentType || 'application/octet-stream'),
    };
    job.responseReady.resolve(job.responseDetails);
    void persistLine(job, { kind: 'response', ...job.responseDetails }).catch(error => {
        console.error('Could not persist generation response metadata:', error);
    });
}

function getInternalUrls(endpoint) {
    const args = globalThis.COMMAND_LINE_ARGS;
    const protocol = args?.ssl ? 'https' : 'http';
    const port = Number(args?.port || 8000);
    const hosts = [];

    if (args?.listen && args.listenAddressIPv4 && !['0.0.0.0', '127.0.0.1'].includes(args.listenAddressIPv4)) {
        hosts.push(args.listenAddressIPv4);
    }
    hosts.push('127.0.0.1');

    if (args?.listen && args.listenAddressIPv6 && !['[::]', '::1', '[::1]'].includes(args.listenAddressIPv6)) {
        hosts.push(args.listenAddressIPv6.replace(/^\[|\]$/g, ''));
    }
    hosts.push('::1');

    return [...new Set(hosts)].map(host => {
        const formattedHost = host.includes(':') ? '[' + host + ']' : host;
        return protocol + '://' + formattedHost + ':' + port + endpoint;
    });
}

function cleanMetadata(metadata = {}) {
    const allowed = ['chatKey', 'generationType', 'mainApi', 'completionSource', 'createdAt', 'stream'];
    return Object.fromEntries(allowed
        .filter(key => typeof metadata[key] === 'string' || typeof metadata[key] === 'number' || typeof metadata[key] === 'boolean')
        .map(key => [key, metadata[key]]));
}

function makeJob(owner, id, metadata, filePath) {
    const responseReady = createDeferred();
    return {
        id,
        owner,
        filePath,
        metadata: cleanMetadata(metadata),
        status: 'starting',
        createdAt: Date.now(),
        finishedAt: null,
        deliveredAt: null,
        error: '',
        responseDetails: null,
        responseReady,
        controller: new AbortController(),
        events: [],
        nextSeq: 1,
        totalBytes: 0,
        pendingLines: [],
        persistQueue: Promise.resolve(),
        flushTimer: null,
        subscribers: new Set(),
    };
}

async function flushJob(job) {
    if (job.flushTimer) {
        clearTimeout(job.flushTimer);
        job.flushTimer = null;
    }
    if (job.pendingLines.length === 0) {
        await job.persistQueue;
        return;
    }
    const lines = job.pendingLines.splice(0).join('');
    job.persistQueue = job.persistQueue.then(() => fs.promises.appendFile(job.filePath, lines, 'utf8'));
    await job.persistQueue;
}

function scheduleFlush(job) {
    if (job.flushTimer) return;
    job.flushTimer = setTimeout(() => {
        job.flushTimer = null;
        void flushJob(job).catch(error => {
            console.error('Could not persist generation job ' + job.id + ':', error);
        });
    }, PERSIST_INTERVAL_MS);
    job.flushTimer.unref?.();
}

function appendChunk(job, chunk) {
    const data = Buffer.from(chunk);
    job.totalBytes += data.length;
    if (job.totalBytes > MAX_JOB_OUTPUT_BYTES) {
        throw new Error('Generation output exceeded the storage limit.');
    }

    const event = {
        seq: job.nextSeq++,
        data: data.toString('base64'),
    };
    job.events.push(event);
    job.pendingLines.push(JSON.stringify({ kind: 'chunk', ...event }) + '\n');
    scheduleFlush(job);
    notifyChunk(job, event);
}

async function finishJob(job, status, error = '') {
    if (['complete', 'failed', 'cancelled', 'interrupted'].includes(job.status) && job.finishedAt) return;
    job.status = status;
    job.error = String(error || '');
    job.finishedAt = Date.now();
    if (!job.responseDetails) {
        setResponseDetails(job, status === 'cancelled' ? 499 : 502, '', 'application/json');
    }
    try {
        await flushJob(job);
        await persistLine(job, {
            kind: 'state',
            status: job.status,
            error: job.error,
            finishedAt: job.finishedAt,
        });
    } catch (persistError) {
        console.error('Could not persist the final state of generation job ' + job.id + ':', persistError);
        if (job.status === 'complete') {
            job.status = 'failed';
        }
        job.error ||= 'The generation finished, but its result could not be saved reliably.';
        try {
            await fs.promises.appendFile(job.filePath, JSON.stringify({
                kind: 'state',
                status: job.status,
                error: job.error,
                finishedAt: job.finishedAt,
            }) + '\n', 'utf8');
        } catch (stateError) {
            console.error('Could not save generation job state ' + job.id + ':', stateError);
        }
    } finally {
        notifyEnd(job);
    }
}

async function requestInternalGeneration(job, endpoint, payload, requestHeaders) {
    const headers = {
        'content-type': 'application/json',
        accept: requestHeaders.accept || '*/*',
        [GENERATION_JOB_RELAY_HEADER]: GENERATION_JOB_RELAY_TOKEN,
    };
    for (const name of ['cookie', 'authorization', 'x-csrf-token', 'user-agent', 'host']) {
        if (requestHeaders[name]) headers[name] = requestHeaders[name];
    }

    let lastError;
    for (const url of getInternalUrls(endpoint)) {
        try {
            return await fetch(url, {
                method: 'POST',
                headers,
                body: payload,
                signal: job.controller.signal,
                agent: url.startsWith('https:') ? httpsAgent : undefined,
            });
        } catch (error) {
            if (job.controller.signal.aborted) throw error;
            lastError = error;
        }
    }
    throw lastError || new Error('Could not reach the local generation endpoint.');
}

async function runJob(job, endpoint, payload, requestHeaders) {
    try {
        const upstream = await requestInternalGeneration(job, endpoint, payload, requestHeaders);
        setResponseDetails(job, upstream.status, upstream.statusText, upstream.headers.get('content-type'));

        if (job.status === 'cancelled') {
            upstream.body?.destroy();
            return;
        }

        for await (const chunk of upstream.body) {
            if (job.status === 'cancelled') break;
            appendChunk(job, chunk);
        }

        if (job.status !== 'cancelled') {
            await finishJob(job, upstream.ok ? 'complete' : 'failed');
        }
    } catch (error) {
        if (job.status === 'cancelled') return;
        console.error('Generation job ' + job.id + ' failed:', error);
        const message = error?.message || 'Generation request failed.';
        if (!job.responseDetails) {
            setResponseDetails(job, 502, 'Bad Gateway', 'application/json');
            appendChunk(job, Buffer.from(JSON.stringify({ error: { message } })));
        }
        await finishJob(job, 'failed', message);
    }
}

async function createJob(owner, endpoint, payload, metadata, requestHeaders) {
    const ownerJobs = await listJobs(owner);
    const activeCount = ownerJobs.filter(job => ['starting', 'running'].includes(job.status)).length;
    if (activeCount >= MAX_ACTIVE_JOBS_PER_USER) {
        const error = new Error('Too many active generation jobs.');
        error.status = 429;
        throw error;
    }

    const id = randomUUID();
    const filePath = getJobPath(owner, id);
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    const job = makeJob(owner, id, metadata, filePath);
    jobs.set(id, job);
    await fs.promises.writeFile(filePath, JSON.stringify({
        kind: 'job',
        id,
        owner,
        metadata: job.metadata,
        status: job.status,
        createdAt: job.createdAt,
    }) + '\n', 'utf8');
    job.status = 'running';
    await persistLine(job, { kind: 'state', status: job.status });
    void runJob(job, endpoint, payload, requestHeaders);
    return job;
}

async function loadJob(owner, id) {
    if (!/^[0-9a-f-]{36}$/i.test(id)) return null;
    const existing = jobs.get(id);
    if (existing) return existing.owner === owner ? existing : null;

    const filePath = getJobPath(owner, id);
    let content;
    try {
        content = await fs.promises.readFile(filePath, 'utf8');
    } catch {
        return null;
    }

    const records = content.split('\n').filter(Boolean).map(line => {
        try {
            return JSON.parse(line);
        } catch {
            return null;
        }
    }).filter(Boolean);
    const header = records.find(record => record.kind === 'job');
    if (!header || header.owner !== owner) return null;

    const job = makeJob(owner, id, header.metadata, filePath);
    job.createdAt = header.createdAt;
    for (const record of records) {
        if (record.kind === 'response') {
            job.responseDetails = {
                status: record.status,
                statusText: record.statusText,
                contentType: record.contentType,
            };
        } else if (record.kind === 'chunk') {
            job.events.push({ seq: record.seq, data: record.data });
            job.totalBytes += Buffer.from(record.data, 'base64').length;
            job.nextSeq = Math.max(job.nextSeq, record.seq + 1);
        } else if (record.kind === 'state') {
            job.status = record.status;
            job.error = record.error || '';
            job.finishedAt = record.finishedAt || null;
            if (Object.hasOwn(record, 'deliveredAt')) {
                job.deliveredAt = record.deliveredAt || null;
            }
        }
    }

    if (['starting', 'running'].includes(job.status)) {
        job.status = 'interrupted';
        job.finishedAt = Date.now();
        job.error = 'The server restarted before generation finished. The saved partial output is available.';
        if (job.metadata.stream) {
            job.responseDetails = {
                status: 200,
                statusText: 'OK',
                contentType: 'text/event-stream',
            };
        } else if (!job.responseDetails) {
            job.responseDetails = {
                status: 502,
                statusText: 'Generation Interrupted',
                contentType: 'application/json',
            };
        } else {
            job.responseDetails.status = 502;
            job.responseDetails.statusText = 'Generation Interrupted';
        }
        await persistLine(job, {
            kind: 'state',
            status: job.status,
            error: job.error,
            finishedAt: job.finishedAt,
        });
    }

    if (job.responseDetails) job.responseReady.resolve(job.responseDetails);
    jobs.set(id, job);
    return job;
}

async function listJobs(owner) {
    const directory = getOwnerDirectory(owner);
    let entries;
    try {
        entries = await fs.promises.readdir(directory, { withFileTypes: true });
    } catch {
        return [];
    }

    const results = [];
    for (const entry of entries) {
        if (!entry.isFile() || !entry.name.endsWith('.jsonl')) continue;
        const id = entry.name.slice(0, -6);
        const job = await loadJob(owner, id);
        if (!job) continue;
        if (Date.now() - job.createdAt > JOB_RETENTION_MS && job.status !== 'running') {
            await fs.promises.rm(job.filePath, { force: true });
            jobs.delete(job.id);
            continue;
        }
        if (!job.deliveredAt && ['starting', 'running', 'complete', 'failed', 'interrupted'].includes(job.status)) {
            results.push(job);
        }
    }
    return results.sort((a, b) => a.createdAt - b.createdAt);
}

function sendSse(response, event, data, id = null) {
    if (id !== null) response.write('id: ' + id + '\n');
    response.write('event: ' + event + '\ndata: ' + JSON.stringify(data) + '\n\n');
}

async function streamJob(job, response, after) {
    await job.responseReady.promise;
    if (response.destroyed) return;

    const details = job.responseDetails || { status: 200, statusText: '', contentType: 'application/octet-stream' };
    response.status(200);
    response.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    response.setHeader('Cache-Control', 'no-cache, no-transform');
    response.setHeader('X-Generation-Job-Id', job.id);
    response.setHeader('X-Generation-Response-Status', String(details.status));
    response.setHeader('X-Generation-Response-Status-Text', details.statusText);
    response.setHeader('X-Generation-Response-Type', details.contentType);
    response.flushHeaders?.();

    for (const event of job.events) {
        if (event.seq > after) {
            sendSse(response, 'chunk', { data: event.data }, event.seq);
        }
    }

    if (job.finishedAt) {
        sendSse(response, 'end', { status: job.status, message: job.error });
        response.end();
        return;
    }

    const subscriber = response;
    job.subscribers.add(subscriber);
    const keepAlive = setInterval(() => {
        if (!response.destroyed && !response.writableEnded) response.write(': keep-alive\n\n');
    }, 15000);
    keepAlive.unref?.();
    response.once('close', () => {
        clearInterval(keepAlive);
        job.subscribers.delete(subscriber);
    });
}

router.post('/', async (request, response) => {
    const { endpoint, payload, metadata } = request.body || {};
    if (!GENERATION_JOB_ENDPOINTS.has(endpoint) || typeof payload !== 'string') {
        return response.status(400).json({ error: 'Invalid generation request.' });
    }

    try {
        const owner = request.user.profile.handle;
        const job = await createJob(owner, endpoint, payload, metadata, request.headers);
        return response.status(202).json({ id: job.id, status: job.status });
    } catch (error) {
        console.error('Could not create generation job:', error);
        return response.status(error.status || 500).json({ error: error.message || 'Could not create generation job.' });
    }
});

router.get('/recoverable', async (request, response) => {
    const jobs = await listJobs(request.user.profile.handle);
    response.json(jobs.map(job => ({
        id: job.id,
        status: job.status,
        createdAt: job.createdAt,
        metadata: job.metadata,
        error: job.error,
    })));
});

router.get('/:id/stream', async (request, response) => {
    const job = await loadJob(request.user.profile.handle, request.params.id);
    if (!job) return response.sendStatus(404);

    const after = Number(request.query.after || 0);
    if (!Number.isSafeInteger(after) || after < 0) {
        return response.status(400).json({ error: 'Invalid event cursor.' });
    }
    return streamJob(job, response, after);
});

router.post('/:id/cancel', async (request, response) => {
    const job = await loadJob(request.user.profile.handle, request.params.id);
    if (!job) return response.sendStatus(404);
    if (['starting', 'running'].includes(job.status)) {
        job.controller.abort();
        await finishJob(job, 'cancelled', 'Generation was cancelled by the user.');
    }
    response.json({ status: job.status });
});

router.post('/:id/ack', async (request, response) => {
    const job = await loadJob(request.user.profile.handle, request.params.id);
    if (!job) return response.sendStatus(404);
    job.deliveredAt = Date.now();
    await persistLine(job, { kind: 'state', status: job.status, deliveredAt: job.deliveredAt });
    response.sendStatus(204);
});

export { router };
