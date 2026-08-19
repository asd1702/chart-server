export const DEFAULT_STREAM_SYMBOL = 'BTC/USD';

export interface MarketConfig {
  streamSymbols: string[];
  historicalBackfillEnabled: boolean;
}

export function parseStreamSymbols(value: string): string[] {
  const symbols = [...new Set(
    value
      .split(',')
      .map((symbol) => symbol.trim())
      .filter(Boolean),
  )];

  if (symbols.length === 0) {
    throw new Error('STREAM_SYMBOLS must contain at least one symbol');
  }

  return symbols;
}
