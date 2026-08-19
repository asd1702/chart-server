import http from 'http';
import WebSocket from 'ws';
import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';

import {
  closeWebSocketServer,
  initWebSocketServer,
  WebSocketService,
} from './websocket.service';

type ServerMessage = {
  type: string;
  [key: string]: unknown;
};

type TestClient = {
  ws: WebSocket;
  messages: ServerMessage[];
};

describe('WebSocketService', () => {
  let httpServer: http.Server;
  let service: WebSocketService;
  let url: string;
  let clients: TestClient[];

  beforeEach(async () => {
    httpServer = http.createServer();
    service = new WebSocketService(httpServer, {
      heartbeatIntervalMs: 60_000,
    });
    clients = [];

    await new Promise<void>((resolve) => {
      httpServer.listen(0, '127.0.0.1', resolve);
    });
    const address = httpServer.address();
    if (!address || typeof address === 'string') {
      throw new Error('Test HTTP server did not receive a TCP address.');
    }
    url = `ws://127.0.0.1:${address.port}/ws`;
  });

  afterEach(async () => {
    await service.destroy();
    await new Promise<void>((resolve, reject) => {
      httpServer.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  });

  async function connectClient(): Promise<TestClient> {
    const ws = new WebSocket(url);
    const messages: ServerMessage[] = [];
    ws.on('message', (data) => {
      messages.push(JSON.parse(data.toString()) as ServerMessage);
    });

    await new Promise<void>((resolve, reject) => {
      ws.once('open', resolve);
      ws.once('error', reject);
    });

    const client = { ws, messages };
    clients.push(client);
    return client;
  }

  async function waitForMessage(
    client: TestClient,
    type: string
  ): Promise<ServerMessage> {
    const existing = client.messages.find((message) => message.type === type);
    if (existing) return existing;

    return new Promise<ServerMessage>((resolve, reject) => {
      const timeout = setTimeout(() => {
        client.ws.off('message', onMessage);
        reject(new Error(`Timed out waiting for ${type} message.`));
      }, 1000);

      const onMessage = (data: WebSocket.RawData) => {
        const message = JSON.parse(data.toString()) as ServerMessage;
        if (message.type !== type) return;

        clearTimeout(timeout);
        client.ws.off('message', onMessage);
        resolve(message);
      };
      client.ws.on('message', onMessage);
    });
  }

  async function subscribe(
    client: TestClient,
    symbols: string[]
  ): Promise<void> {
    client.ws.send(JSON.stringify({ type: 'subscribe', symbols }));
    await waitForMessage(client, 'subscribed');
  }

  async function allowMessagesToArrive(): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  it('sends a welcome message immediately after connection', async () => {
    const client = await connectClient();

    await expect(waitForMessage(client, 'welcome')).resolves.toMatchObject({
      type: 'welcome',
      message: 'Connected to Chart WebSocket.',
      subscriptionRequired: true,
    });
  });

  it('does not send symbol messages before subscribe', async () => {
    const client = await connectClient();
    await waitForMessage(client, 'welcome');

    service.broadcastLocal({
      type: 'tick',
      symbol: 'AAPL',
      price: 100,
      timestamp: 1_700_000_000,
    });
    await allowMessagesToArrive();

    expect(client.messages).not.toContainEqual(
      expect.objectContaining({ type: 'tick' })
    );
  });

  it('sends only a subscribed symbol', async () => {
    const client = await connectClient();
    await subscribe(client, ['AAPL']);

    service.broadcastLocal({
      type: 'tick',
      symbol: 'AAPL',
      price: 100,
      timestamp: 1_700_000_000,
    });
    service.broadcastLocal({
      type: 'tick',
      symbol: 'MSFT',
      price: 200,
      timestamp: 1_700_000_000,
    });
    await allowMessagesToArrive();

    const ticks = client.messages.filter((message) => message.type === 'tick');
    expect(ticks).toEqual([
      expect.objectContaining({ symbol: 'AAPL' }),
    ]);
  });

  it('stops sending a symbol after unsubscribe', async () => {
    const client = await connectClient();
    await subscribe(client, ['AAPL']);
    client.ws.send(JSON.stringify({
      type: 'unsubscribe',
      symbols: ['AAPL'],
    }));
    await waitForMessage(client, 'unsubscribed');

    service.broadcastLocal({
      type: 'tick',
      symbol: 'AAPL',
      price: 100,
      timestamp: 1_700_000_000,
    });
    await allowMessagesToArrive();

    expect(client.messages).not.toContainEqual(
      expect.objectContaining({ type: 'tick' })
    );
  });

  it('keeps subscriptions isolated per client', async () => {
    const aaplClient = await connectClient();
    const msftClient = await connectClient();
    await subscribe(aaplClient, ['AAPL']);
    await subscribe(msftClient, ['MSFT']);

    service.broadcastLocal({
      type: 'tick',
      symbol: 'AAPL',
      price: 100,
      timestamp: 1_700_000_000,
    });
    service.broadcastLocal({
      type: 'tick',
      symbol: 'MSFT',
      price: 200,
      timestamp: 1_700_000_000,
    });
    await allowMessagesToArrive();

    expect(
      aaplClient.messages.filter((message) => message.type === 'tick')
    ).toEqual([expect.objectContaining({ symbol: 'AAPL' })]);
    expect(
      msftClient.messages.filter((message) => message.type === 'tick')
    ).toEqual([expect.objectContaining({ symbol: 'MSFT' })]);
  });

  it('reports an invalid message without closing the connection', async () => {
    const client = await connectClient();
    client.ws.send('{invalid json');

    await expect(waitForMessage(client, 'error')).resolves.toMatchObject({
      errorCode: 'INVALID_WS_MESSAGE',
      message: 'Invalid WebSocket message.',
    });
    expect(client.ws.readyState).toBe(WebSocket.OPEN);
  });

  it('rejects an invalid subscription symbol', async () => {
    const client = await connectClient();
    client.ws.send(JSON.stringify({
      type: 'subscribe',
      symbols: [''],
    }));

    await expect(waitForMessage(client, 'error')).resolves.toMatchObject({
      errorCode: 'INVALID_SYMBOL',
      message: 'Invalid symbol.',
    });
    expect(client.ws.readyState).toBe(WebSocket.OPEN);
  });

  it('filters candle messages by candle symbol', async () => {
    const client = await connectClient();
    await subscribe(client, ['BTC/USD']);

    service.broadcastLocal({
      type: 'candle',
      timeframe: '1m',
      candle: {
        symbol: 'BTC/USD',
        startTime: 1_700_000_000,
        open: 100,
        high: 101,
        low: 99,
        close: 100.5,
        volume: 0,
      },
    });

    await expect(waitForMessage(client, 'candle')).resolves.toMatchObject({
      candle: { symbol: 'BTC/USD' },
    });
  });

  it('terminates a client that misses the heartbeat', async () => {
    await connectClient();
    const internals = service as unknown as {
      clients: Map<WebSocket, { isAlive: boolean }>;
      checkConnections: () => void;
    };
    const serverClient = internals.clients.keys().next().value;
    if (!serverClient) throw new Error('Expected a connected server client.');
    internals.clients.get(serverClient)!.isAlive = false;

    internals.checkConnections();

    expect(service.getClientCount()).toBe(0);
  });
});

describe('Chart Server realtime routing', () => {
  it('routes a subscriber event to a matching local WebSocket client', async () => {
    const httpServer = http.createServer();
    let onMarketEvent: ((message: {
      type: 'tick';
      symbol: string;
      price: number;
      timestamp: number;
    }) => void) | undefined;
    const subscriber = {
      subscribe: vi.fn(async (callback: typeof onMarketEvent) => {
        onMarketEvent = callback;
      }),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    await initWebSocketServer(httpServer, subscriber);
    await new Promise<void>((resolve) => httpServer.listen(0, '127.0.0.1', resolve));
    const address = httpServer.address();
    if (!address || typeof address === 'string') {
      throw new Error('Test HTTP server did not receive a TCP address.');
    }

    const client = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
    const messages: ServerMessage[] = [];
    client.on('message', (data) => {
      messages.push(JSON.parse(data.toString()) as ServerMessage);
    });

    try {
      await new Promise<void>((resolve, reject) => {
        client.once('open', resolve);
        client.once('error', reject);
      });
      client.send(JSON.stringify({ type: 'subscribe', symbols: ['AAPL'] }));

      await vi.waitFor(() => {
        expect(messages).toContainEqual(expect.objectContaining({ type: 'subscribed' }));
      });
      onMarketEvent?.({
        type: 'tick',
        symbol: 'AAPL',
        price: 123,
        timestamp: 1_700_000_000,
      });

      await vi.waitFor(() => {
        expect(messages).toContainEqual(expect.objectContaining({
          type: 'tick',
          symbol: 'AAPL',
        }));
      });
      expect(subscriber.subscribe).toHaveBeenCalledOnce();
    } finally {
      await closeWebSocketServer();
      if (client.readyState !== WebSocket.CLOSED) client.terminate();
      await new Promise<void>((resolve, reject) => {
        httpServer.close((error) => error ? reject(error) : resolve());
      });
    }
  });
});
