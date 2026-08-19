/**
 * Express Request 객체 타입 확장 (Declaration Merging)
 * 
 * - req.user 타입을 전역적으로 정의하여 'as any' 제거
 * - 인증 미들웨어 사용 시 타입 안전성 확보
 */

declare global {
  namespace Express {
    interface Request {
      /**
       * 인증 미들웨어를 거친 경우 user 정보가 주입됨
       * Optional: 인증되지 않은 요청도 있을 수 있음
       */
      user?: {
        id: string;
        email: string;
        name?: string | null;
      };
    }
  }
}

// 이 파일을 모듈로 만들기 위해 필요
export {};
