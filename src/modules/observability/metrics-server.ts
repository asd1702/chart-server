import {
  createServer,
  type Server,
  type ServerResponse,
} from 'node:http';
import type { Registry } from 'prom-client';

/** Small process-local HTTP server; scrape errors never affect business work. */
export class MetricsServer {
  private server: Server | null = null;

  constructor(
    private readonly port: number,
    private readonly registry: Registry,
  ) {}

  async start(): Promise<void> {
    if (this.server) {
      return;
    }

    const server = createServer((request, response) => {
      void this.handleRequest(request.url, response);
    });

    await new Promise<void>((resolve, reject) => {
      server.once('listening', resolve);
      server.once('error', reject);
      server.listen(this.port);
    }).catch((error) => {
      server.close();
      throw error;
    });

    this.server = server;
  }

  async stop(): Promise<void> {
    const server = this.server;
    this.server = null;

    if (!server) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });
  }

  private async handleRequest(
    requestUrl: string | undefined,
    response: ServerResponse,
  ): Promise<void> {
    if (requestUrl !== '/metrics') {
      response.statusCode = 404;
      response.end();
      return;
    }

    try {
      response.setHeader('Content-Type', this.registry.contentType);
      response.end(await this.registry.metrics());
    } catch {
      /* A broken scrape must not crash or disturb the workload process. */
      response.statusCode = 500;
      response.end();
    }
  }
}
