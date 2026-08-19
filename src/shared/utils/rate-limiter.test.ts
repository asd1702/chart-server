import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  TwelveDataCooldownError,
  TwelveDataRequestPolicy,
} from './rate-limiter';

function httpError(status: number, retryAfter?: string): Error {
  return Object.assign(new Error(`HTTP ${status}`), {
    response: {
      status,
      headers: retryAfter === undefined ? {} : { 'retry-after': retryAfter },
    },
  });
}

function networkError(code: string): Error {
  return Object.assign(new Error(code), {
    code,
    isAxiosError: true,
  });
}

describe('TwelveDataRequestPolicy', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('does not retry 429 and honors Retry-After across all requests', () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-08-20T00:00:00.000Z'));
    const policy = new TwelveDataRequestPolicy();

    const decision = policy.handleFailure(httpError(429, '2'), 0);

    expect(decision).toMatchObject({
      kind: 'rate_limited',
      cooldownStarted: true,
      retryAfterMs: 2000,
    });
    expect(() => policy.beforeRequest()).toThrow(TwelveDataCooldownError);

    vi.advanceTimersByTime(1999);
    expect(() => policy.beforeRequest()).toThrow(TwelveDataCooldownError);
    vi.advanceTimersByTime(1);
    expect(policy.beforeRequest()).toEqual({ resumed: true });
    expect(policy.beforeRequest()).toEqual({ resumed: false });
  });

  it('uses a bounded cooldown when Retry-After is absent', () => {
    const policy = new TwelveDataRequestPolicy({
      now: () => 1000,
      defaultCooldownMs: 300_000,
    });

    expect(policy.handleFailure(httpError(429), 0)).toMatchObject({
      kind: 'rate_limited',
      retryAfterMs: 300_000,
    });
  });

  it('honors an HTTP-date Retry-After value without sleeping', () => {
    const now = Date.parse('2026-08-20T00:00:00.000Z');
    const policy = new TwelveDataRequestPolicy({ now: () => now });

    expect(policy.handleFailure(
      httpError(429, 'Thu, 20 Aug 2026 00:02:00 GMT'),
      0,
    )).toMatchObject({
      kind: 'rate_limited',
      retryAfterMs: 120_000,
      cooldownUntil: now + 120_000,
    });
  });

  it('retries timeout and 5xx failures with exponential backoff and jitter', () => {
    const policy = new TwelveDataRequestPolicy({ random: () => 0.5 });

    expect(policy.handleFailure(networkError('ETIMEDOUT'), 0)).toEqual({
      kind: 'retry',
      retryAfterMs: 1125,
    });
    expect(policy.handleFailure(httpError(503), 1)).toEqual({
      kind: 'retry',
      retryAfterMs: 2250,
    });
    expect(policy.handleFailure(httpError(503), 3)).toEqual({ kind: 'fail' });
  });

  it('does not retry non-rate-limit 4xx failures', () => {
    const policy = new TwelveDataRequestPolicy();
    expect(policy.handleFailure(httpError(401), 0)).toEqual({ kind: 'fail' });
  });

  it('does not retry an unexpected application exception', () => {
    const policy = new TwelveDataRequestPolicy();
    expect(policy.handleFailure(new Error('programming error'), 0)).toEqual({
      kind: 'fail',
    });
  });
});
