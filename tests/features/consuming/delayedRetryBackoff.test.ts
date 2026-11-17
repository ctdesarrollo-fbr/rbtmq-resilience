import { afterEach, describe, expect, test } from "bun:test";
import type { RabbitMQResilience } from "@/index";
import { buildConsumeMessage } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { decodePublish, EVENTS_QUEUE, startConsuming } from "./consuming.helper";

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

const failing = eventProcess("OrderCreated", "h", async () => {
    throw new Error("boom");
});

async function retryOptions(backoffInMs: number | undefined, redeliveryCount: number) {
    const started = await startConsuming({
        eventResilienceHandlerConfig: { immediateRetryAttempts: 1, delayedRetryAttempts: 5, delayInMs: 10000, backoffInMs },
        eventsToProcess: [failing],
    });
    current = started.resilience;
    await started.channel.deliver(
        EVENTS_QUEUE,
        buildConsumeMessage({
            messageId: "m-1",
            type: "OrderCreated",
            headers: redeliveryCount > 0 ? { redelivery_count: redeliveryCount, retry_endpoint: "checkout-service" } : {},
        }),
    );
    const [published] = started.channel.callsOf("publish").map(decodePublish);
    expect(published?.routingKey).toBe("commerce.orders.checkout.events-retry");
    return published!.options;
}

describe("delayed retry backoff", () => {
    test("sets expiration = redelivery_count * backoffInMs on each delayed retry", async () => {
        expect((await retryOptions(2000, 0)).expiration).toBe("2000");
        expect((await retryOptions(2000, 1)).expiration).toBe("4000");
        expect((await retryOptions(2000, 2)).expiration).toBe("6000");
    });

    test("uses the default 2000 ms backoff when backoffInMs is not configured", async () => {
        expect((await retryOptions(undefined, 0)).expiration).toBe("2000");
        expect((await retryOptions(undefined, 1)).expiration).toBe("4000");
    });

    test("keeps the queue TTL only (no expiration) when backoffInMs is 0", async () => {
        const options = await retryOptions(0, 1);
        expect("expiration" in options).toBe(false);
    });

    test("supports the runtime config (retry.backoffMs)", async () => {
        const { fakeAmqp } = await import("@tests/helpers/fakeAmqp");
        const { createFakeDatabase } = await import("@tests/helpers/fakeDatabase");
        const { RabbitMQResilience, MessageHandlerDto } = await import("@/index");
        const channel = fakeAmqp.reset();
        const resilience = new RabbitMQResilience({
            serviceName: "svc",
            amqp: { url: "amqp://x" },
            exchanges: { integration: { name: "i" }, tactical: { name: "t" }, direct: { name: "d" } },
            retry: { immediateRetries: 1, delayedAttempts: 3, delayMs: 100, prefetch: 1, backoffMs: 1500 },
            db: createFakeDatabase().db,
            inboxTransaction: (await import("@tests/helpers/fakeTransactions")).createFakeTransactions().config,
            consumers: [
                {
                    name: "c",
                    kind: "commands",
                    queue: {
                        primary: { name: "q", routingKey: "q" },
                        retry: { name: "q-retry", routingKey: "q-retry" },
                        error: { name: "q-error", routingKey: "q-error" },
                    },
                    handlers: [
                        new MessageHandlerDto<unknown>("T", "h", (payload) => payload, async () => {
                            throw new Error("boom");
                        }),
                    ],
                },
            ],
        });
        current = resilience;
        await resilience.start();

        await channel.deliver("q", buildConsumeMessage({ messageId: "m-1", type: "T" }));

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.options.expiration).toBe("1500");
    });

    test("never sets expiration on the error queue message", async () => {
        const started = await startConsuming({
            eventResilienceHandlerConfig: { immediateRetryAttempts: 1, delayedRetryAttempts: 1, delayInMs: 10000, backoffInMs: 2000 },
            eventsToProcess: [failing],
        });
        current = started.resilience;

        await started.channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                headers: { redelivery_count: 1, retry_endpoint: "checkout-service" },
            }),
        );

        const [published] = started.channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-error");
        expect("expiration" in published!.options).toBe(false);
    });
});
