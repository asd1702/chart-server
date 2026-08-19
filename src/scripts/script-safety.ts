const SUSPICIOUS_DATABASE_URL_KEYWORDS = [
  'rds.amazonaws.com',
  'amazonaws.com',
  'prod',
  'production',
] as const;

export function assertDestructiveDbScriptAllowed(scriptName: string): void {
  if (process.env.NODE_ENV === 'production') {
    throw new Error(`${scriptName} is blocked in production.`);
  }

  if (process.env.CONFIRM_DB_RESET !== 'YES') {
    throw new Error(
      `${scriptName} is destructive. Set CONFIRM_DB_RESET=YES to continue.`
    );
  }

  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error(`${scriptName} requires DATABASE_URL.`);
  }

  const normalizedUrl = databaseUrl.toLowerCase();
  if (
    SUSPICIOUS_DATABASE_URL_KEYWORDS.some((keyword) =>
      normalizedUrl.includes(keyword)
    )
  ) {
    throw new Error(
      `${scriptName} refused to run against a suspicious DATABASE_URL.`
    );
  }
}
