import winston from 'winston';

const fmt = process.env.NODE_ENV === 'production'
  ? winston.format.combine(winston.format.timestamp(), winston.format.json())
  : winston.format.combine(
      winston.format.colorize(),
      winston.format.timestamp({ format: 'HH:mm:ss' }),
      winston.format.printf(({ level, message, timestamp, ...meta }) => {
        const m = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
        return `${timestamp} ${level}: ${message}${m}`;
      })
    );

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: fmt,
  defaultMeta: { service: 'raft-kv' },
  transports: [new winston.transports.Console()],
});
