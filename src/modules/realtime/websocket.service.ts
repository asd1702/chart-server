import http from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { parseSymbol } from '../candle/candle.validation';
import { ValidationError } from '../../shared/types/common.types';
import type { MarketEventSubscriber } from '../messaging/pubsub.interface';
import { InboundSocketMessage } from './realtime.types';
import type { MarketEvent } from '../market-data/market-data.types';

interface ClientSubscription {
  symbols: Set<string>;
  isAlive: boolean;
}

interface WebSocketServiceOptions {
  heartbeatIntervalMs?: number;
}

const WELCOME_MESSAGE = {
  type: 'welcome',
  message: 'Connected to Chart WebSocket.',
  subscriptionRequired: true,
} as const;

export class WebSocketService {
  private readonly wss: WebSocketServer;
  private readonly clients = new Map<WebSocket, ClientSubscription>();
  private heartbeatInterval: NodeJS.Timeout | null = null;

  constructor(
    httpServer: http.Server,
    options: WebSocketServiceOptions = {}
  ) {
    this.wss = new WebSocketServer({ server: httpServer, path: '/ws' });
    this.wss.on('connection', (ws) => this.handleConnection(ws));

    const heartbeatIntervalMs = options.heartbeatIntervalMs ?? 30_000;
    this.heartbeatInterval = setInterval(
      () => this.checkConnections(),
      heartbeatIntervalMs
    );
    this.heartbeatInterval.unref();
  }

  private handleConnection(ws: WebSocket): void {
    this.clients.set(ws, {
      symbols: new Set(),
      isAlive: true,
    });
    this.send(ws, WELCOME_MESSAGE);

    ws.on('pong', () => {
      const client = this.clients.get(ws);
      if (client) client.isAlive = true;
    });

    ws.on('message', (data) => {
      this.handleClientMessage(ws, data.toString());
    });

    ws.on('close', () => {
      this.clients.delete(ws);
    });

    ws.on('error', (error) => {
      console.error('[Frontend] WebSocket 오류:', error);
    });
  }

  private handleClientMessage(ws: WebSocket, rawMessage: string): void {
    let message: unknown;
    try {
      message = JSON.parse(rawMessage);
    } catch {
      this.sendError(
        ws,
        'INVALID_WS_MESSAGE',
        'Invalid WebSocket message.'
      );
      return;
    }

    if (!this.isSubscriptionMessage(message)) {
      this.sendError(
        ws,
        'INVALID_WS_MESSAGE',
        'Invalid WebSocket message.'
      );
      return;
    }

    let symbols: string[];
    try {
      symbols = message.symbols.map(parseSymbol);
    } catch (error) {
      if (error instanceof ValidationError) {
        this.sendError(ws, error.errorCode, error.message);
        return;
      }
      throw error;
    }

    const client = this.clients.get(ws);
    if (!client) return;

    if (message.type === 'subscribe') {
      symbols.forEach((symbol) => client.symbols.add(symbol));
      this.send(ws, { type: 'subscribed', symbols: [...new Set(symbols)] });
      return;
    }

    symbols.forEach((symbol) => client.symbols.delete(symbol));
    this.send(ws, { type: 'unsubscribed', symbols: [...new Set(symbols)] });
  }

  private isSubscriptionMessage(
    message: unknown
  ): message is InboundSocketMessage {
    if (!message || typeof message !== 'object') return false;

    const candidate = message as Partial<InboundSocketMessage>;
    return (
      (candidate.type === 'subscribe' || candidate.type === 'unsubscribe') &&
      Array.isArray(candidate.symbols) &&
      candidate.symbols.length > 0 &&
      candidate.symbols.every((symbol) => typeof symbol === 'string')
    );
  }

  broadcastLocal(message: MarketEvent): void {
    const symbol =
      'symbol' in message ? message.symbol : message.candle.symbol;
    const data = JSON.stringify(message);

    for (const [ws, client] of this.clients) {
      if (
        ws.readyState === WebSocket.OPEN &&
        client.symbols.has(symbol)
      ) {
        ws.send(data);
      }
    }
  }

  private checkConnections(): void {
    for (const [ws, client] of this.clients) {
      if (!client.isAlive) {
        this.clients.delete(ws);
        ws.terminate();
        continue;
      }

      client.isAlive = false;
      ws.ping();
    }
  }

  private send(ws: WebSocket, message: object): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private sendError(
    ws: WebSocket,
    errorCode: string,
    message: string
  ): void {
    this.send(ws, { type: 'error', errorCode, message });
  }

  getClientCount(): number {
    return this.clients.size;
  }

  async destroy(): Promise<void> {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    // Reject new upgrades first, then terminate every server-owned peer. The
    // HTTP server lifecycle is owned by server.ts and is awaited separately.
    const connectedPeers = [...this.wss.clients];
    this.wss.close();
    await Promise.all(connectedPeers.map((ws) => new Promise<void>((resolve) => {
      if (ws.readyState === WebSocket.CLOSED) {
        resolve();
        return;
      }
      ws.once('close', resolve);
      ws.terminate();
    })));
    this.clients.clear();
  }
}

let webSocketService: WebSocketService | undefined;

export async function initWebSocketServer(
  httpServer: http.Server,
  subscriber: MarketEventSubscriber
): Promise<void> {
  webSocketService = new WebSocketService(httpServer);
  await subscriber.subscribe((message) => {
    webSocketService?.broadcastLocal(message);
  });
}

export function getClientCount(): number {
  return webSocketService?.getClientCount() ?? 0;
}

export async function closeWebSocketServer(): Promise<void> {
  if (!webSocketService) return;

  const service = webSocketService;
  webSocketService = undefined;
  await service.destroy();
}
