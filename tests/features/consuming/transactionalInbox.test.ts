import { afterEach, describe, expect, test } from "bun:test";
import type { HandlerContextDto, RabbitMQResilience } from "@/index";
import { buildConsumeMessage } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { createFakeTransactions } from "@tests/helpers/fakeTransactions";
import { decodePublish, EVENTS_QUEUE, startConsuming } from "./consuming.helper";

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

const INBOX_INSERT = "INSERT INTO inbox (message_id, handler_name, payload) VALUES (?, ?, ?)";

describe("transactional inbox", () => {
    test("runs the handler and the inbox insert in the same transaction and hands it to the handler", async () => {
        const fake = createFakeTransactions();
        const contexts: Array<HandlerContextDto | undefined> = [];
        const started = await startConsuming({
            inboxTransaction: fake.config,
            eventsToProcess: [eventProcess("OrderCreated", "sendEmail", async (_event, context) => void contexts.push(context))],
        });
        current = started.resilience;

        await started.channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", payload: { id: 1 } }),
        );

        expect(fake.transactions).toHaveLength(1);
        const [transaction] = fake.transactions;
        expect(transaction).toMatchObject({ committed: true, rolledBack: false });
        expect(transaction?.executed).toEqual([{ sql: INBOX_INSERT, params: ["m-1", "sendEmail", '{"id":1}'] }]);
        expect(contexts).toHaveLength(1);
        expect(contexts[0]?.transaction).toBe(transaction);
        expect(contexts[0]).toMatchObject({ serviceName: "checkout-service", consumerName: EVENTS_QUEUE });
        // the inbox is only written through the service's transaction, never by the library on its own
        expect(started.database.opsNamed("values")).toEqual([]);
        expect(started.channel.callsOf("ack")).toEqual([["m-1"]]);
    });

    test("rolls back and opens a new transaction for every immediate retry when the handler fails", async () => {
        const fake = createFakeTransactions();
        const started = await startConsuming({
            inboxTransaction: fake.config,
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    throw new Error("boom");
                }),
            ],
        });
        current = started.resilience;

        await started.channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        // immediateRetryAttempts: 2 → first attempt + 2 immediate retries, each in its own transaction
        expect(fake.transactions.map((transaction) => transaction.rolledBack)).toEqual([true, true, true]);
        expect(fake.transactions.every((transaction) => transaction.executed.length === 0)).toBe(true);
        const [published] = started.channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-retry");
    });

    test("a concurrent copy that committed the inbox first: rolled back, not retried, message rejected", async () => {
        const fake = createFakeTransactions({ failExecute: new Error("Duplicate entry 'm-1-h' for key 'uq_inbox_message_handler'") });
        let handlerRuns = 0;
        const started = await startConsuming({
            inboxTransaction: fake.config,
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void handlerRuns++)],
        });
        current = started.resilience;
        // first check: not processed yet; after the failed insert: the other copy already committed it
        started.database.selectResults.push([], [{ id: 1 }]);

        await started.channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(handlerRuns).toBe(1);
        expect(fake.transactions.map((transaction) => transaction.rolledBack)).toEqual([true]);
        expect(started.channel.callsOf("publish")).toEqual([]);
        expect(started.channel.callsOf("ack")).toEqual([]);
        expect(started.channel.callsOf("reject")).toEqual([["m-1", false]]);
    });

    test("rolls back the handler's work when the inbox insert fails and the inbox still does not have it", async () => {
        const fake = createFakeTransactions({ failExecute: new Error("Duplicate entry 'm-1-h'") });
        let handlerRuns = 0;
        const started = await startConsuming({
            inboxTransaction: fake.config,
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void handlerRuns++)],
        });
        current = started.resilience;

        await started.channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(handlerRuns).toBe(3);
        expect(fake.transactions.every((transaction) => transaction.rolledBack)).toBe(true);
        const [published] = started.channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-retry");
    });

    test("skips the handler without opening a transaction and rejects the message when the inbox already has it", async () => {
        const fake = createFakeTransactions();
        let handlerRuns = 0;
        const started = await startConsuming({
            inboxTransaction: fake.config,
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void handlerRuns++)],
        });
        current = started.resilience;
        started.database.selectResults.push([{ id: 1 }]);

        await started.channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(handlerRuns).toBe(0);
        expect(fake.transactions).toEqual([]);
        expect(started.channel.callsOf("reject")).toEqual([["m-1", false]]);
    });

});
