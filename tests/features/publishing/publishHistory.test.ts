import { afterEach, describe, expect, test } from "bun:test";
import { getTableName } from "drizzle-orm";
import { RabbitMQResilience } from "@/index";
import { fakeAmqp } from "@tests/helpers/fakeAmqp";
import { createFakeDatabase } from "@tests/helpers/fakeDatabase";
import { buildServiceConfig, withDb } from "@tests/helpers/config";

/**
 * Publishing goes straight to the exchange. The outbox table is the history
 * of what was published: one row per message id with its status (sent / failed).
 */

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

async function started() {
    const channel = fakeAmqp.reset();
    const database = createFakeDatabase();
    current = new RabbitMQResilience(withDb(buildServiceConfig({ tacticalExchange: "app.tactical" }), database.db));
    await current.start();
    channel.calls.length = 0;
    return { resilience: current, channel, database };
}

/** The rows written to the outbox, with the upsert set used when the message id already exists. */
function historyRows(database: ReturnType<typeof createFakeDatabase>) {
    const inserts = database.operations
        .map((operation, index) => ({ operation, index }))
        .filter(({ operation }) => operation.op === "insert" && getTableName(operation.args[0] as never) === "outbox");
    return inserts.map(({ index }) => ({
        row: database.operations[index + 1]!.args[0] as Record<string, unknown>,
        onDuplicate: (database.operations[index + 2]!.args[0] as { set: Record<string, unknown> }).set,
    }));
}

describe("outbox as publish history", () => {
    test("publishes straight to the exchange and records the message as sent", async () => {
        const { resilience, channel, database } = await started();

        const messageId = await resilience.publishEvent({ type: "OrderCreated", payload: { id: 1 }, options: { messageId: "m-1" } });

        expect(messageId).toBe("m-1");
        expect(channel.callsOf("publish")).toHaveLength(1);
        const [{ row, onDuplicate }] = historyRows(database) as [ReturnType<typeof historyRows>[number]];
        expect(row).toMatchObject({
            messageId: "m-1",
            type: "OrderCreated",
            body: { id: 1 },
            exchange: "app.integration",
            routingKey: "",
            status: "sent",
            attempts: 1,
            error: null,
            failedAt: null,
        });
        expect(row.sentAt).toBeInstanceOf(Date);
        expect(row.headers).toMatchObject({ type: "OrderCreated", message_id: "m-1", "x-exchange": "app.integration" });
        // publishing the same id again updates the row and counts one more attempt
        expect(onDuplicate).toMatchObject({ status: "sent", error: null });
        expect(onDuplicate.attempts).toBeDefined();
    });

    test("records a failed publication with its error and throws it to the caller", async () => {
        const { resilience, channel, database } = await started();
        channel.failPublish = true;
        const originalError = console.error;
        console.error = () => {};
        try {
            await expect(
                resilience.sendCommand({ type: "ReceiveVolume", payload: {}, routingKey: "logistics.volume-reception.commands", options: { messageId: "c-1" } }),
            ).rejects.toThrow("publish failed");
        } finally {
            console.error = originalError;
        }

        const [{ row }] = historyRows(database) as [ReturnType<typeof historyRows>[number]];
        expect(row).toMatchObject({
            messageId: "c-1",
            exchange: "app.direct",
            routingKey: "logistics.volume-reception.commands",
            status: "failed",
            error: "publish failed",
            sentAt: null,
        });
        expect(row.failedAt).toBeInstanceOf(Date);
    });

    test("records tactical events too", async () => {
        const { resilience, database } = await started();

        await resilience.publishTacticalEvent({
            type: "VolumeReceived",
            payload: {},
            routingKey: "logistics.volume-reception.volume-received",
            options: { messageId: "t-1" },
        });

        expect(historyRows(database).map(({ row }) => [row.messageId, row.exchange, row.status])).toEqual([["t-1", "app.tactical", "sent"]]);
    });

    test("before start() there is no database yet: it publishes without recording", async () => {
        const channel = fakeAmqp.reset();
        const database = createFakeDatabase();
        const resilience = new RabbitMQResilience(withDb(buildServiceConfig(), database.db));

        await resilience.publishEvent({ type: "OrderCreated", payload: {} });

        expect(channel.callsOf("publish")).toHaveLength(1);
        expect(database.opsNamed("insert")).toEqual([]);
    });

    test("if the history cannot be written the publication still succeeds and the problem is logged", async () => {
        const { resilience, channel, database } = await started();
        database.failInsert = new Error("Deadlock found");
        const logged: string[] = [];
        const originalError = console.error;
        console.error = (line: unknown) => void logged.push(String(line));
        try {
            const messageId = await resilience.publishEvent({ type: "OrderCreated", payload: {}, options: { messageId: "m-1" } });
            expect(messageId).toBe("m-1");
        } finally {
            console.error = originalError;
        }

        expect(channel.callsOf("publish")).toHaveLength(1);
        expect(logged).toEqual(["[rbtmq-resilience] could not record OrderCreated m-1 in the outbox history: Deadlock found"]);
    });
});
