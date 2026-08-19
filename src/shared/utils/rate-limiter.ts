import Bottleneck from 'bottleneck';
import axios from 'axios';
import { logger } from './logger';

const DEFAULT_RATE_LIMIT_COOLDOWN_MS = 5 * 60 * 1000;
const MAX_TRANSIENT_RETRIES = 3;
const TRANSIENT_NETWORK_CODES = new Set([
  'ETIMEDOUT',
  'ECONNRESET',
  'ECONNABORTED',
  'EAI_AGAIN',
  'ERR_NETWORK',
]);
const rateLimitLogger = logger.child({ subsystem: 'twelvedata-rest' });

type FailureDecision =
  | {
    kind: 'rate_limited';
    cooldownStarted: boolean;
    cooldownUntil: number;
    retryAfterMs: number;
  }
  | { kind: 'retry'; retryAfterMs: number }
  | { kind: 'fail' };

interface TwelveDataRequestPolicyOptions {
  now?: () => number;
  random?: () => number;
  defaultCooldownMs?: number;
}

export class TwelveDataCooldownError extends Error {
  constructor(
    readonly retryAfterMs: number,
    readonly cooldownUntil: number,
  ) {
    super('TwelveData REST requests are cooling down');
    this.name = 'TwelveDataCooldownError';
  }
}

export class TwelveDataRequestPolicy {
  private nextAllowedRequestAt = 0;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly defaultCooldownMs: number;

  constructor(options: TwelveDataRequestPolicyOptions = {}) {
    this.now = options.now ?? Date.now;
    this.random = options.random ?? Math.random;
    this.defaultCooldownMs = options.defaultCooldownMs
      ?? DEFAULT_RATE_LIMIT_COOLDOWN_MS;
  }

  beforeRequest(): { resumed: boolean } {
    const now = this.now();
    if (this.nextAllowedRequestAt > now) {
      throw new TwelveDataCooldownError(
        this.nextAllowedRequestAt - now,
        this.nextAllowedRequestAt,
      );
    }

    const resumed = this.nextAllowedRequestAt > 0;
    if (resumed) this.nextAllowedRequestAt = 0;
    return { resumed };
  }

  handleFailure(error: unknown, retryCount: number): FailureDecision {
    if (error instanceof TwelveDataCooldownError) return { kind: 'fail' };

    const status = getHttpStatus(error);
    if (status === 429) {
      const now = this.now();
      const retryAfterMs = parseRetryAfterMs(error, now)
        ?? this.defaultCooldownMs;
      const cooldownUntil = now + retryAfterMs;
      const cooldownStarted = this.nextAllowedRequestAt <= now;
      this.nextAllowedRequestAt = Math.max(
        this.nextAllowedRequestAt,
        cooldownUntil,
      );
      return {
        kind: 'rate_limited',
        cooldownStarted,
        cooldownUntil: this.nextAllowedRequestAt,
        retryAfterMs: this.nextAllowedRequestAt - now,
      };
    }

    if (isTransientFailure(error) && retryCount < MAX_TRANSIENT_RETRIES) {
      const baseDelayMs = 1000 * (2 ** retryCount);
      const jitterMs = Math.floor(baseDelayMs * 0.25 * this.random());
      return { kind: 'retry', retryAfterMs: baseDelayMs + jitterMs };
    }

    return { kind: 'fail' };
  }
}

/**
 * Shared protection for optional TwelveData REST workloads such as backfill.
 */
export const twelveDataLimiter = new Bottleneck({
  maxConcurrent: 10,
  minTime: 100,
  highWater: 5000,
  strategy: Bottleneck.strategy.LEAK,
});

const requestPolicy = new TwelveDataRequestPolicy();

twelveDataLimiter.on('failed', async (error, jobInfo) => {
  const decision = requestPolicy.handleFailure(error, jobInfo.retryCount);

  if (decision.kind === 'rate_limited') {
    if (decision.cooldownStarted) {
      rateLimitLogger.warn('TwelveData REST rate limited; global cooldown started', {
        event: 'twelvedata_rate_limited',
        retryAfterMs: decision.retryAfterMs,
        cooldownUntil: new Date(decision.cooldownUntil).toISOString(),
        error: getErrorMessage(error),
      });
    }
    return undefined;
  }

  if (decision.kind === 'retry') {
    rateLimitLogger.warn('TwelveData transient request failure; retry scheduled', {
      event: 'twelvedata_transient_retry',
      retryCount: jobInfo.retryCount,
      retryAfterMs: decision.retryAfterMs,
      error: getErrorMessage(error),
    });
    return decision.retryAfterMs;
  }

  return undefined;
});

export async function scheduleTwelveDataRequest<T>(
  request: () => Promise<T>,
): Promise<T> {
  assertRequestAllowed();
  return twelveDataLimiter.schedule(async () => {
    assertRequestAllowed();
    return request();
  });
}

export function isTwelveDataRateLimitError(error: unknown): boolean {
  return error instanceof TwelveDataCooldownError || getHttpStatus(error) === 429;
}

function assertRequestAllowed(): void {
  const { resumed } = requestPolicy.beforeRequest();
  if (resumed) {
    rateLimitLogger.info('TwelveData REST requests resumed after cooldown', {
      event: 'twelvedata_rest_resumed',
    });
  }
}

function getHttpStatus(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const response = (error as { response?: unknown }).response;
  if (!response || typeof response !== 'object') return undefined;
  const status = (response as { status?: unknown }).status;
  return typeof status === 'number' ? status : undefined;
}

function isTransientFailure(error: unknown): boolean {
  const status = getHttpStatus(error);
  if (status === 408 || (status !== undefined && status >= 500 && status <= 599)) {
    return true;
  }
  if (status !== undefined || !axios.isAxiosError(error)) return false;

  return typeof error.code === 'string'
    && TRANSIENT_NETWORK_CODES.has(error.code);
}

function parseRetryAfterMs(error: unknown, now: number): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const response = (error as { response?: unknown }).response;
  if (!response || typeof response !== 'object') return undefined;
  const headers = (response as { headers?: unknown }).headers;
  const rawValue = readHeader(headers, 'retry-after');
  if (rawValue === undefined) return undefined;

  const value = Array.isArray(rawValue) ? rawValue[0] : rawValue;
  if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
    return value * 1000;
  }
  if (typeof value !== 'string') return undefined;

  const seconds = Number(value.trim());
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;

  const retryAt = Date.parse(value);
  if (!Number.isFinite(retryAt) || retryAt <= now) return undefined;
  return retryAt - now;
}

function readHeader(headers: unknown, name: string): unknown {
  if (!headers || typeof headers !== 'object') return undefined;
  const getter = (headers as { get?: unknown }).get;
  if (typeof getter === 'function') {
    return getter.call(headers, name);
  }
  const record = headers as Record<string, unknown>;
  return record[name] ?? record[name.toLowerCase()] ?? record[name.toUpperCase()];
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}
