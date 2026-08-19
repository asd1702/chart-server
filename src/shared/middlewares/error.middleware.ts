import { Request, Response, NextFunction } from 'express';
import { AppError } from '../types/common.types';

export function errorHandler(
  err: Error,
  req: Request,
  res: Response,
  _next: NextFunction
): void {
  console.error(`[Error] ${err.message}`, {
    stack: err.stack,
    path: req.path,
    method: req.method,
  });

  // AppError (우리가 정의한 에러)
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      success: false,
      errorCode: err.errorCode,
      message: err.message,
    });
    return;
  }

  // Prisma 에러 처리
  if (err.name === 'PrismaClientKnownRequestError') {
    res.status(400).json({
      success: false,
      errorCode: 'DATABASE_OPERATION_FAILED',
      message: 'Database operation failed.',
    });
    return;
  }

  // Validation 에러 (예: Zod)
  if (err.name === 'ZodError') {
    res.status(400).json({
      success: false,
      errorCode: 'VALIDATION_FAILED',
      message: 'Validation failed.',
    });
    return;
  }

  // 알 수 없는 에러
  res.status(500).json({
    success: false,
    errorCode: 'INTERNAL_SERVER_ERROR',
    message: process.env.NODE_ENV === 'production'
      ? 'Internal server error.'
      : err.message,
  });
}

export function notFoundHandler(req: Request, res: Response): void {
  res.status(404).json({
    success: false,
    errorCode: 'NOT_FOUND',
    message: `Cannot ${req.method} ${req.path}`,
  });
}
