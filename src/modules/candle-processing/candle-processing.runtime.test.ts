import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Candle } from '../candle/candle.types';
import type {
  ConsumedRawTickRecord,
  RawTickConsumer,
} from '../kafka/raw-tick.consumer';
import type { MarketEventPublisher } from '../messaging/pubsub.interface';
import type { CandleProcessingResult } from './candle-processor';
import {
  CandleProcessingRuntime,
  type CandleMessageProcessor,
} from './candle-processing.runtime';

const mocks = vi.hoisted(() => ({
  logger: {
    warn: vi.fn(),
  },
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) =>
    error instanceof Error ? error.message : 'Unknown error',
  logger: { child: vi.fn(() => mocks.logger) },
}));

const completedCandle: Candle = {
  symbol: 'BTC/USD',
  startTime: 60,
  open: 100,
  high: 110,
  low: 100,
  close: 110,
  volume: 0,
};

describe('CandleProcessingRuntime fatal and realtime handling', () => {
  let handler:
    | ((record: ConsumedRawTickRecord) => Promise<void>)
    | undefined;
  let consumer: RawTickConsumer;

  beforeEach(() => {
    handler = undefined;
    vi.clearAllMocks();
    consumer = {
      start: vi.fn(async (input) => {
        handler = input;
      }),
      commitOffset: vi.fn(),
      stop: vi.fn().mockResolvedValue(undefined),
    };
  });

  function validRecord(
    value: Buffer | null = Buffer.from(JSON.stringify({
      schemaVersion: 1,
      symbol: 'BTC/USD',
      price: 100,
      providerTimestampSec: 60,
      receivedAtMs: 60_000,
      source: 'twelvedata',
    })),
  ): ConsumedRawTickRecord {
    return {
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '150',
      value,
    };
  }

  function createPublisher(): MarketEventPublisher {
    return {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
  }

  async function startAndGetHandler(
    processor: CandleMessageProcessor,
    publisher: MarketEventPublisher,
    onFatalError: ReturnType<typeof vi.fn>,
  ): Promise<(record: ConsumedRawTickRecord) => Promise<void>> {
    const runtime = new CandleProcessingRuntime(
      consumer,
      processor,
      publisher,
      onFatalError,
    );

    await runtime.start();

    if (!handler) {
      throw new Error('Consumer handler was not registered');
    }

    return handler;
  }

  function resultProcessor(
    result: CandleProcessingResult,
  ): CandleMessageProcessor {
    return {
      process: vi.fn().mockResolvedValue(result),
    };
  }

  it('publishes a completed candle only after processor persistence and checkpoint success', async () => {
    const processor = resultProcessor({ completedCandle });
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(registeredHandler(validRecord())).resolves.toBeUndefined();

    expect(publisher.publish).toHaveBeenCalledWith({
      type: 'candle',
      timeframe: '1m',
      candle: completedCandle,
    });
    expect(onFatalError).not.toHaveBeenCalled();
  });

  it('keeps consuming when completed candle Redis publish fails', async () => {
    const processor = resultProcessor({ completedCandle });
    const publisher = createPublisher();
    publisher.publish = vi.fn()
      .mockRejectedValueOnce(new Error('redis unavailable'))
      .mockResolvedValue(undefined);
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(registeredHandler(validRecord())).resolves.toBeUndefined();
    await expect(registeredHandler(validRecord())).resolves.toBeUndefined();

    expect(processor.process).toHaveBeenCalledTimes(2);
    expect(publisher.publish).toHaveBeenCalledTimes(2);
    expect(onFatalError).not.toHaveBeenCalled();
    expect(mocks.logger.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        event: 'completed_candle_publish_failed',
      }),
    );
  });

  it('routes a database failure through fatal handling without publishing a candle', async () => {
    const processingError = new Error('database unavailable');
    const processor: CandleMessageProcessor = {
      process: vi.fn().mockRejectedValue(processingError),
    };
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(registeredHandler(validRecord())).rejects.toThrow(
      'database unavailable',
    );

    expect(publisher.publish).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledWith(processingError);
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('routes an offset commit failure through fatal handling without publishing a candle', async () => {
    const processingError = new Error('Kafka commit failure');
    const processor: CandleMessageProcessor = {
      process: vi.fn().mockRejectedValue(processingError),
    };
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(registeredHandler(validRecord())).rejects.toThrow(
      'Kafka commit failure',
    );

    expect(publisher.publish).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledWith(processingError);
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('fails fatally when a Kafka record contains invalid JSON', async () => {
    const processor = resultProcessor({ completedCandle: null });
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(
      registeredHandler(validRecord(Buffer.from('{invalid-json'))),
    ).rejects.toThrow('RawMarketTick value must be valid JSON');

    expect(processor.process).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('fails fatally when a Kafka record has a null value', async () => {
    const processor = resultProcessor({ completedCandle: null });
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    await expect(registeredHandler(validRecord(null))).rejects.toThrow(
      'RawMarketTick value must not be null',
    );

    expect(processor.process).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('fails fatally when a Kafka record has an invalid RawMarketTick schema', async () => {
    const processor = resultProcessor({ completedCandle: null });
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    const invalidSchemaRecord = validRecord(
      Buffer.from(JSON.stringify({
        schemaVersion: 99,
        symbol: 'BTC/USD',
        price: 100,
        providerTimestampSec: 60,
        receivedAtMs: 60_000,
        source: 'twelvedata',
      })),
    );

    await expect(registeredHandler(invalidSchemaRecord)).rejects.toThrow(
      'RawMarketTick schemaVersion must be 1',
    );

    expect(processor.process).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('routes an unexpected symbol failure through the same fatal boundary', async () => {
    const processingError = new Error(
      'Unexpected symbol for candle processor: ETH/USD',
    );
    const processor: CandleMessageProcessor = {
      process: vi.fn().mockRejectedValue(processingError),
    };
    const publisher = createPublisher();
    const onFatalError = vi.fn().mockResolvedValue(undefined);
    const registeredHandler = await startAndGetHandler(
      processor,
      publisher,
      onFatalError,
    );

    const unexpectedSymbolRecord = validRecord(
      Buffer.from(JSON.stringify({
        schemaVersion: 1,
        symbol: 'ETH/USD',
        price: 100,
        providerTimestampSec: 60,
        receivedAtMs: 60_000,
        source: 'twelvedata',
      })),
    );

    await expect(registeredHandler(unexpectedSymbolRecord)).rejects.toThrow(
      'Unexpected symbol for candle processor: ETH/USD',
    );

    expect(publisher.publish).not.toHaveBeenCalled();
    expect(onFatalError).toHaveBeenCalledWith(processingError);
    expect(onFatalError).toHaveBeenCalledOnce();
  });

  it('stops the consumer during a normal shutdown', async () => {
    const runtime = new CandleProcessingRuntime(
      consumer,
      resultProcessor({ completedCandle: null }),
      createPublisher(),
      vi.fn().mockResolvedValue(undefined),
    );

    await runtime.stop();

    expect(consumer.stop).toHaveBeenCalledOnce();
  });
});
