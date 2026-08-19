import { describe, expect, it } from 'vitest';
import { DEFAULT_STREAM_SYMBOL, parseStreamSymbols } from './market.config';

describe('market configuration', () => {
  it('uses BTC/USD as the architecture-lab workload default', () => {
    expect(parseStreamSymbols(DEFAULT_STREAM_SYMBOL)).toEqual(['BTC/USD']);
  });

  it('keeps comma-separated multi-symbol capability', () => {
    expect(parseStreamSymbols('BTC/USD, ETH/USD')).toEqual([
      'BTC/USD',
      'ETH/USD',
    ]);
  });

  it('fails fast when no ingestion symbol is configured', () => {
    expect(() => parseStreamSymbols(' , ')).toThrow(
      'STREAM_SYMBOLS must contain at least one symbol',
    );
  });
});
