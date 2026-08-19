import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { assertDestructiveDbScriptAllowed } from './script-safety';

const ENV_KEYS = [
  'NODE_ENV',
  'CONFIRM_DB_RESET',
  'DATABASE_URL',
] as const;

describe('assertDestructiveDbScriptAllowed', () => {
  const originalEnv = new Map<string, string | undefined>();

  beforeEach(() => {
    for (const key of ENV_KEYS) {
      originalEnv.set(key, process.env[key]);
    }
    process.env.NODE_ENV = 'development';
    process.env.DATABASE_URL =
      'postgresql://chart:chart@localhost:5432/chart_dev';
    delete process.env.CONFIRM_DB_RESET;
  });

  afterEach(() => {
    for (const key of ENV_KEYS) {
      const value = originalEnv.get(key);
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    originalEnv.clear();
  });

  it('rejects execution without explicit confirmation', () => {
    expect(() => assertDestructiveDbScriptAllowed('reset-db')).toThrow(
      'Set CONFIRM_DB_RESET=YES'
    );
  });

  it('rejects execution in production', () => {
    process.env.NODE_ENV = 'production';
    process.env.CONFIRM_DB_RESET = 'YES';

    expect(() => assertDestructiveDbScriptAllowed('reset-db')).toThrow(
      'blocked in production'
    );
  });

  it('rejects an AWS RDS URL without exposing it', () => {
    process.env.CONFIRM_DB_RESET = 'YES';
    process.env.DATABASE_URL =
      'postgresql://chart:secret@chart.cluster.rds.amazonaws.com:5432/chart';

    expect(() => assertDestructiveDbScriptAllowed('reset-db')).toThrow(
      'suspicious DATABASE_URL'
    );
  });

  it('allows an explicitly confirmed local development database', () => {
    process.env.CONFIRM_DB_RESET = 'YES';

    expect(() =>
      assertDestructiveDbScriptAllowed('reset-db')
    ).not.toThrow();
  });
});
