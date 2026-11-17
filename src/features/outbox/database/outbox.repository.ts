import { sql } from "drizzle-orm";
import { outbox } from "./outbox.schema";
import { PublishedMessageDto, PublishHistoryRecorder } from "@/shared/dtos";
import { DatabaseService } from "@/database";

/**
 * History of published messages: one row per message id with its last status.
 * Publishing the same message id again updates the row and counts one more attempt.
 */
export class OutboxRepository implements PublishHistoryRecorder {
    constructor(
        private readonly databaseService: DatabaseService,
    ) {}

    async recordSent(message: PublishedMessageDto): Promise<void> {
        const now = new Date();
        await this.upsert(message, { status: "sent", error: null, sentAt: now, failedAt: null });
    }

    async recordFailed(message: PublishedMessageDto, error: string): Promise<void> {
        const now = new Date();
        await this.upsert(message, { status: "failed", error, sentAt: null, failedAt: now });
    }

    private async upsert(
        message: PublishedMessageDto,
        result: { status: string; error: string | null; sentAt: Date | null; failedAt: Date | null },
    ): Promise<void> {
        // before start() the database is not ready yet: nothing to record
        const db = this.databaseService.getDb();
        if (!db) return;

        const headers = {
            ...message.headers,
            type: message.type,
            message_id: message.messageId,
            content_type: "application/json",
            "x-exchange": message.exchange,
            "x-routing-key": message.routingKey,
        };

        await db
            .insert(outbox)
            .values({
                messageId: message.messageId,
                type: message.type,
                headers,
                body: message.body ?? {},
                exchange: message.exchange,
                routingKey: message.routingKey,
                attempts: 1,
                ...result,
            })
            .onDuplicateKeyUpdate({
                set: { ...result, attempts: sql`${outbox.attempts} + 1` },
            });
    }
}
