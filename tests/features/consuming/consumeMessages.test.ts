import { afterEach, describe, expect, test } from "bun:test";
import type { RabbitMQMessageDto, RabbitMQResilience } from "@/index";
import { buildConsumeMessage } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { decodePublish, EVENTS_QUEUE, INBOX_INSERT, startConsuming } from "./consuming.helper";

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

async function start(...args: Parameters<typeof startConsuming>) {
    const started = await startConsuming(...args);
    current = started.resilience;
    return started;
}

describe("consumeMessages", () => {
    test("applies prefetch and subscribes to the primary queue", async () => {
        const { channel } = await start();

        expect(channel.callsOf("prefetch")).toEqual([[10]]);
        expect(channel.callsOf("consume")).toEqual([[EVENTS_QUEUE]]);
    });

    test("ignores null deliveries", async () => {
        const { channel } = await start();

        await channel.consumers.get(EVENTS_QUEUE)?.(null);

        expect(channel.callsOf("ack")).toEqual([]);
    });

    test("handles a message, records it in the inbox and acks it", async () => {
        const received: RabbitMQMessageDto[] = [];
        const { resilience, channel, transactions } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "sendEmail", async (event) => void received.push(event))],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                payload: { id: 1 },
                headers: { trace: "abc" },
                routingKey: "rk",
            }),
        );

        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
        expect(received).toHaveLength(1);
        expect(JSON.parse(received[0]!.content.toString("utf8"))).toEqual({ id: 1 });
        expect(received[0]!.fields).toEqual({
            consumerTag: "",
            deliveryTag: 0,
            exchange: "",
            routingKey: "rk",
            redelivered: false,
        });
        expect(received[0]!.properties).toEqual({
            contentType: "application/json",
            contentEncoding: "utf-8",
            headers: { trace: "abc" },
            deliveryMode: 2,
            messageId: "m-1",
            type: "OrderCreated",
        });
        // the inbox record is written in the same transaction as the handler
        expect(transactions).toHaveLength(1);
        expect(transactions[0]).toMatchObject({ committed: true, rolledBack: false });
        expect(transactions[0]!.executed).toEqual([{ sql: INBOX_INSERT, params: ["m-1", "sendEmail", '{"id":1}'] }]);
    });

    test("reads message id and type from headers when properties are missing", async () => {
        let calls = 0;
        const { channel } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void calls++)],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ headers: { message_id: Buffer.from("hdr-1"), type: "OrderCreated" } }),
        );

        expect(calls).toBe(1);
    });

    test("rejects without requeue a message every handler already processed", async () => {
        let calls = 0;
        const { resilience, channel, database } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void calls++)],
        });
        database.selectResults.push([{ id: 1 }]);

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(calls).toBe(0);
        expect(channel.callsOf("reject")).toEqual([["m-1", false]]);
        expect(channel.callsOf("ack")).toEqual([]);
    });

    test("discards messages without id or type", async () => {
        const { resilience, channel } = await start();

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ type: "OrderCreated" }));

        expect(channel.callsOf("ack")).toEqual([[undefined]]);
        expect(channel.callsOf("publish")).toEqual([]);
    });

    test("discards messages without a matching handler", async () => {
        const { resilience, channel } = await start({
            eventsToProcess: [eventProcess("Other", "h", async () => {})],
        });

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
    });

    test("discards retries that belong to another service", async () => {
        let calls = 0;
        const { channel } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void calls++)],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                headers: { redelivery_count: 1, retry_endpoint: "other-service" },
            }),
        );

        expect(calls).toBe(0);
        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
        expect(channel.callsOf("publish")).toEqual([]);
    });

    test("retries immediately, then publishes to the retry queue with diagnostics", async () => {
        let attempts = 0;
        const { resilience, channel } = await start({
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    attempts += 1;
                    throw new Error("boom");
                }),
            ],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", payload: { id: 1 }, headers: { trace: "t" } }),
        );

        // immediateRetryAttempts: 2 → first attempt + 2 immediate retries
        expect(attempts).toBe(3);
        const published = channel.callsOf("publish").map(decodePublish);
        expect(published).toHaveLength(1);
        const { options, ...rest } = published[0]!;
        expect(rest).toEqual({
            exchange: "app.direct",
            routingKey: "commerce.orders.checkout.events-retry",
            payload: { id: 1 },
        });
        expect(typeof options.timestamp).toBe("number");
        expect({ ...options, timestamp: 0 } as Record<string, unknown>).toEqual({
            type: "OrderCreated",
            messageId: "m-1",
            appId: "checkout-service",
            contentType: "application/json",
            persistent: true,
            timestamp: 0,
            headers: { trace: "t", redelivery_count: 1, retry_endpoint: "checkout-service" },
            expiration: "2000",
            // retry/error queues are ours: an unroutable retry fails instead of being lost
            mandatory: true,
        });
        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
    });

    test("counts delayed retries from x-death when redelivery_count is absent", async () => {
        const { channel } = await start({
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    throw new Error("boom");
                }),
            ],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                headers: { "x-death": [{ count: 1 }, { count: "1" }, null] },
            }),
        );

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-retry");
        expect((published?.options.headers as Record<string, unknown>).redelivery_count).toBe(3);
    });

    test("sends to the error queue after the delayed attempts are exhausted", async () => {
        const { resilience, channel } = await start({
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    throw new TypeError("still failing");
                }),
            ],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                payload: { id: 1 },
                routingKey: "rk",
                exchange: "app.direct",
                headers: { redelivery_count: 3, retry_endpoint: "checkout-service" },
            }),
        );

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.exchange).toBe("app.direct");
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-error");
        expect(published?.payload).toEqual({ payload: { id: 1 } });
        const headers = published?.options.headers as Record<string, unknown>;
        expect(headers.endpoint).toEqual({
            name: "checkout-service",
            delivery_metadata: { exchange: "app.direct", routing_key: "commerce.orders.checkout.events", message_type: "OrderCreated" },
        });
        // one object with exception_type, stack_trace and failed_at: the shape Failed Messages Monitoring reads
        expect(headers.exception_details).toEqual({
            exception_type: "TypeError",
            stack_trace: expect.stringContaining("TypeError: still failing"),
            failed_at: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
        });

        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
    });

    test("a message with invalid JSON goes through the delayed retries like any failure, keeping its raw body", async () => {
        const { resilience, channel } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => {})],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                rawContent: "{not json",
                exchange: "app.integration",
                headers: { trace: "t" },
            }),
        );

        expect(channel.callsOf("nack")).toEqual([]);
        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
        const [args] = channel.callsOf("publish") as [[string, string, Buffer, Record<string, unknown>]];
        const [exchange, routingKey, body, options] = args;
        expect([exchange, routingKey]).toEqual(["app.direct", "commerce.orders.checkout.events-retry"]);
        expect(body.toString("utf8")).toBe("{not json");
        expect(options.headers).toMatchObject({ trace: "t", redelivery_count: 1, retry_endpoint: "checkout-service" });
    });

    test("a message with invalid JSON reaches the error queue once the delayed retries are exhausted", async () => {
        const { resilience, channel } = await start({
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => {})],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                rawContent: "{not json",
                exchange: "app.direct",
                headers: { trace: "t", redelivery_count: 3, retry_endpoint: "checkout-service" },
            }),
        );

        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
        const published = channel.callsOf("publish").map(decodePublish);
        expect(published).toHaveLength(1);
        expect(published[0]?.routingKey).toBe("commerce.orders.checkout.events-error");
        expect(published[0]?.payload).toEqual({ payload: "{not json" });
        const headers = published[0]?.options.headers as Record<string, unknown>;
        expect(headers.trace).toBe("t");
        expect(headers.endpoint).toEqual({
            name: "checkout-service",
            delivery_metadata: { exchange: "app.direct", routing_key: "commerce.orders.checkout.events", message_type: "OrderCreated" },
        });
        expect(headers.exception_details).toMatchObject({
            exception_type: "SyntaxError",
            stack_trace: expect.stringContaining("JSON"),
        });
    });

    test("discards a message with invalid JSON when no handler is registered for its type", async () => {
        const { resilience, channel } = await start({
            eventsToProcess: [eventProcess("Other", "h", async () => {})],
        });

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", rawContent: "{not json" }),
        );

        expect(channel.callsOf("ack")).toEqual([["m-1"]]);
        expect(channel.callsOf("publish")).toEqual([]);
    });

    test("requeues the message when it cannot be routed to the error queue (transient broker failure)", async () => {
        const originalError = console.error;
        const logged: unknown[][] = [];
        console.error = (...args: unknown[]) => void logged.push(args);
        try {
            const { channel } = await start({
                eventsToProcess: [eventProcess("OrderCreated", "h", async () => {})],
            });
            channel.failPublish = true;

            await channel.deliver(
                EVENTS_QUEUE,
                buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", rawContent: "{not json" }),
            );

            expect(channel.callsOf("nack")).toEqual([["m-1", false, true]]);
            expect(channel.callsOf("ack")).toEqual([]);
            // logged even with logs: false, because the message could not be acknowledged
            expect(logged).toHaveLength(1);
            expect(logged[0]?.[0]).toMatch(/^\[rbtmq-resilience\] requeued OrderCreated m-1 \(\d+ ms\): publish failed$/);
        } finally {
            console.error = originalError;
        }
    });

    test("runs the handler once when immediate retries are configured as 0", async () => {
        let attempts = 0;
        const { resilience, channel } = await start({
            eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 3, delayInMs: 1000 },
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    attempts += 1;
                    throw new Error("boom");
                }),
            ],
        });

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(attempts).toBe(1);
        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-retry");
    });

    test("with immediate retries configured as 0, a successful handler is recorded in the inbox", async () => {
        let attempts = 0;
        const { resilience, channel, transactions } = await start({
            eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 3, delayInMs: 1000 },
            eventsToProcess: [eventProcess("OrderCreated", "h", async () => void attempts++)],
        });

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(attempts).toBe(1);
        expect(transactions.map((transaction) => transaction.executed.length)).toEqual([1]);
    });
});

describe("consumeMessages with runtime handlers", () => {
    test("a validation error is retried immediately and then through the retry queue", async () => {
        const { fakeAmqp } = await import("@tests/helpers/fakeAmqp");
        const { createFakeDatabase } = await import("@tests/helpers/fakeDatabase");
        const { RabbitMQResilience, MessageHandlerDto } = await import("@/index");
        const channel = fakeAmqp.reset();
        const database = createFakeDatabase();
        let handled = 0;
        let validations = 0;
        const resilience = new RabbitMQResilience({
            serviceName: "svc",
            amqp: { url: "amqp://x" },
            exchanges: { integration: { name: "i" }, tactical: { name: "t" }, direct: { name: "d" } },
            retry: { immediateRetries: 3, delayedAttempts: 5, delayMs: 100, prefetch: 4 },
            db: database.db,
            inboxTransaction: (await import("@tests/helpers/fakeTransactions")).createFakeTransactions().config,
            consumers: [
                {
                    name: "c",
                    kind: "events",
                    queue: {
                        primary: { name: "q", routingKey: "q" },
                        retry: { name: "q-retry", routingKey: "q-retry" },
                        error: { name: "q-error", routingKey: "q-error" },
                    },
                    handlers: [
                        new MessageHandlerDto<unknown>(
                            "T",
                            "strict",
                            () => {
                                validations += 1;
                                throw new Error("invalid payload");
                            },
                            async () => void handled++,
                        ),
                    ],
                },
            ],
        });
        current = resilience;
        await resilience.start();

        await channel.deliver("q", buildConsumeMessage({ messageId: "m-1", type: "T" }));

        expect(channel.callsOf("prefetch")).toEqual([[4]]);
        expect(handled).toBe(0);
        // initial handling + 3 immediate retries, then the retry queue (not the error queue)
        expect(validations).toBe(4);
        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("q-retry");
        expect((published?.options.headers as Record<string, unknown>).redelivery_count).toBe(1);
    });
});
