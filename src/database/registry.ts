// Registers the tables, repositories and migrations of every feature (public exports).
import { inboxMigration } from "@/features/consuming/database/inbox.schema";
import { outboxMigration } from "@/features/outbox/database/outbox.schema";

export { inbox, type InboxRow } from "@/features/consuming/database/inbox.schema";
export { outbox, type OutboxRow, type NewOutboxRow } from "@/features/outbox/database/outbox.schema";

export type { ResilienceDatabase } from "./database.service";
export { InboxRepository } from "@/features/consuming/database/inbox.repository";
export { OutboxRepository } from "@/features/outbox/database/outbox.repository";

/** Migrations of every feature, in execution order. */
export const mysqlMigration = `
${inboxMigration}

${outboxMigration}
`;
