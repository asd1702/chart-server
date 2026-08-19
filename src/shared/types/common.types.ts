import { Request, Response, NextFunction } from 'express';

// API Response 타입
export interface ApiResponse<T = unknown> {
  success: boolean;
  data?: T;
  errorCode?: string;
  message?: string;
}

// Pagination 타입
export interface PaginationParams {
  limit?: number;
  offset?: number;
  cursor?: string;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    total?: number;
    hasMore: boolean;
    nextCursor?: string;
  };
}

// Express 타입 확장
export type AsyncHandler<P = unknown, ResBody = unknown, ReqBody = unknown, ReqQuery = unknown> = (
  req: Request<P, ResBody, ReqBody, ReqQuery>,
  res: Response,
  next: NextFunction
) => Promise<void | Response>;

// 에러 타입
export class AppError extends Error {
  constructor(
    public statusCode: number,
    public errorCode: string,
    message: string,
    public isOperational = true
  ) {
    super(message);
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class BadRequestError extends AppError {
  constructor(message = 'Bad Request') {
    super(400, 'BAD_REQUEST', message);
  }
}

export class ValidationError extends AppError {
  constructor(errorCode: string, message: string) {
    super(400, errorCode, message);
  }
}

export class UnauthorizedError extends AppError {
  constructor(message = 'Unauthorized') {
    super(401, 'UNAUTHORIZED', message);
  }
}

export class ForbiddenError extends AppError {
  constructor(message = 'Forbidden') {
    super(403, 'FORBIDDEN', message);
  }
}

export class NotFoundError extends AppError {
  constructor(message = 'Not Found') {
    super(404, 'NOT_FOUND', message);
  }
}
