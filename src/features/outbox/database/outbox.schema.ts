import {
    int,
    json,
    mysqlTable,
    text,
    timestamp,
    uniqueIndex,
    varchar,
} from "drizzle-orm/mysql-core";

/**
 * History of every message the service publishes (publishEvent, publishTacticalEvent,
 * sendCommand): one row per message id with its last status.
 * status: "sent" | "failed".
 */
export const outbox = mysqlTable(
    "outbox",
    {
        id: int("id").autoincrement().primaryKey(),
        messageId: varchar("message_id", { length: 191 }).notNull(),
        type: varchar("type", { length: 191 }).notNull(),
        headers: json("headers").notNull(),
        body: json("body").notNull(),
        exchange: varchar("exchange", { length: 191 }).notNull(),
        routingKey: varchar("routing_key", { length: 191 }).notNull(),
        status: varchar("status", { length: 16 }).notNull().default("sent"),
        attempts: int("attempts").notNull().default(1),
        error: text("error"),
        sentAt: timestamp("sent_at"),
        failedAt: timestamp("failed_at"),
        createdAt: timestamp("created_at").defaultNow(),
    },
    (table) => [
        uniqueIndex("uq_outbox_message_id").on(table.messageId),
    ],
);

export type OutboxRow = typeof outbox.$inferSelect;
export type NewOutboxRow = typeof outbox.$inferInsert;

/** SQL that creates the table (run on start, see database/registry.ts). */
export const outboxMigration = `CREATE TABLE IF NOT EXISTS outbox (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  message_id VARCHAR(191) NOT NULL,
  type VARCHAR(191) NOT NULL,
  headers JSON NOT NULL,
  body JSON NOT NULL,
  exchange VARCHAR(191) NOT NULL,
  routing_key VARCHAR(191) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'sent',
  attempts INT NOT NULL DEFAULT 1,
  error TEXT NULL,
  sent_at TIMESTAMP NULL,
  failed_at TIMESTAMP NULL,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_outbox_message_id (message_id)
);`;
