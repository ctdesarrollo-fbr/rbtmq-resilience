import {
    index,
    int,
    json,
    mysqlTable,
    timestamp,
    uniqueIndex,
    varchar,
} from "drizzle-orm/mysql-core";
import { JsonObject } from "@/shared/primitives";

export const inbox = mysqlTable(
    "inbox",
    {
        id: int("id").autoincrement().primaryKey(),
        messageId: varchar("message_id", { length: 191 }).notNull(),
        handlerName: varchar("handler_name", { length: 191 }).notNull(),
        payload: json("payload").notNull(),
        processedAt: timestamp("processed_at").defaultNow(),
    },
    (table) => [
        index("idx_inbox_message_id").on(table.messageId),
        uniqueIndex("uq_inbox_message_handler").on(
            table.messageId,
            table.handlerName,
        ),
    ],
);

export type InboxRow = typeof inbox.$inferSelect;

/** SQL that creates the table (run on start, see database/registry.ts). */
export const inboxMigration = `CREATE TABLE IF NOT EXISTS inbox (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  message_id VARCHAR(191) NOT NULL,
  handler_name VARCHAR(191) NOT NULL,
  payload JSON NOT NULL,
  processed_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_inbox_message_id (message_id),
  UNIQUE KEY uq_inbox_message_handler (message_id, handler_name)
);`;

/**
 * Inbox insert executed inside the service's transaction (`inboxTransaction.execute`).
 * A plain INSERT (no ON DUPLICATE KEY UPDATE): if a concurrent delivery already recorded
 * this message/handler, the unique key fails and the whole transaction rolls back.
 */
export const inboxInsertSql = "INSERT INTO inbox (message_id, handler_name, payload) VALUES (?, ?, ?)";

export function inboxInsertParams(messageId: string, handlerName: string, payload: JsonObject): unknown[] {
    return [messageId, handlerName, JSON.stringify(payload)];
}
