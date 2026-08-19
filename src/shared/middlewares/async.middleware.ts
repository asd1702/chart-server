import { Request, Response, NextFunction, RequestHandler } from 'express';

/**
 * Async 핸들러 래퍼
 * try-catch 없이 async 에러를 자동으로 next()로 전달
 */
export function asyncHandler<P = unknown, ResBody = unknown, ReqBody = unknown, ReqQuery = unknown>(
  fn: (
    req: Request<P, ResBody, ReqBody, ReqQuery>,
    res: Response,
    next: NextFunction
  ) => Promise<void | Response>
): RequestHandler<P, ResBody, ReqBody, ReqQuery> {
  return (req, res, next) => {
    Promise.resolve(fn(req as Request<P, ResBody, ReqBody, ReqQuery>, res, next)).catch(next);
  };
}
