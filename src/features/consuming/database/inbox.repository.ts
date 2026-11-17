import { and, eq } from "drizzle-orm";
import { inbox } from "./inbox.schema";
import { DatabaseService } from "@/database";

export class InboxRepository {
    constructor(
        private readonly databaseService: DatabaseService,
    ) {}

    async hasProcessed(messageId: string, handlerName: string): Promise<boolean> {
        const db = this.databaseService.getDb();
        if (!db) return false;

        const rows = await db
            .select({ id: inbox.id })
            .from(inbox)
            .where(and(eq(inbox.messageId, messageId), eq(inbox.handlerName, handlerName)))
            .limit(1);

        return rows.length > 0;
    }
}
