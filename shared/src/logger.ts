export type LogLevel = "info" | "warn" | "error";

export class Logger {
  constructor(private readonly scope: string) {}

  log(level: LogLevel, message: string, meta?: unknown): void {
    const prefix = `[${new Date().toISOString()}] [${this.scope}] [${level.toUpperCase()}]`;
    if (meta === undefined) {
      console.log(prefix, message);
      return;
    }

    console.log(prefix, message, meta);
  }

  info(message: string, meta?: unknown): void {
    this.log("info", message, meta);
  }

  warn(message: string, meta?: unknown): void {
    this.log("warn", message, meta);
  }

  error(message: string, meta?: unknown): void {
    this.log("error", message, meta);
  }
}
