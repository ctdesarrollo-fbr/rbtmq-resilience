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

async function start(delayedRetryAttempts: number) {
    const started = await startConsuming({
        eventResilienceHandlerConfig: { immediateRetryAttempts: 1, delayedRetryAttempts, delayInMs: 1000 },
        eventsToProcess: [
            eventProcess("OrderCreated", "h", async () => {
                throw new Error("boom");
            }),
        ],
    });
    current = started.resilience;
    return started;
}

describe("sender in the delivery metadata", () => {
    test("the error message reports the original publisher (appId) as sender", async () => {
        const { channel } = await start(0);

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", appId: "sales.orders-booking", routingKey: "rk" }),
        );

        const [published] = channel.callsOf("publish").map(decodePublish);
        const headers = published?.options.headers as Record<string, unknown>;
        expect(headers.endpoint).toEqual({
            name: "checkout-service",
            delivery_metadata: {
                exchange: "app.direct",
                routing_key: "commerce.orders.checkout.events",
                message_type: "OrderCreated",
                sender: "sales.orders-booking",
            },
        });
    });

    test("the retry message carries the original sender, because its appId becomes this service", async () => {
        const { channel } = await start(3);

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: "m-1", type: "OrderCreated", appId: "sales.orders-booking" }),
        );

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.options.appId).toBe("checkout-service");
        expect((published?.options.headers as Record<string, unknown>).sender).toBe("sales.orders-booking");
    });

    test("after delayed retries, the error message still reports the original sender", async () => {
        const { channel } = await start(1);

        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({
                messageId: "m-1",
                type: "OrderCreated",
                appId: "checkout-service",
                headers: { redelivery_count: 1, retry_endpoint: "checkout-service", sender: "sales.orders-booking" },
            }),
        );

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("commerce.orders.checkout.events-error");
        const endpoint = (published?.options.headers as Record<string, Record<string, Record<string, unknown>>>).endpoint;
        expect(endpoint?.delivery_metadata?.sender).toBe("sales.orders-booking");
    });

    test("omits sender when the publisher is unknown", async () => {
        const { channel } = await start(0);

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        const [published] = channel.callsOf("publish").map(decodePublish);
        const endpoint = (published?.options.headers as Record<string, Record<string, Record<string, unknown>>>).endpoint;
        expect("sender" in endpoint!.delivery_metadata!).toBe(false);
    });
});
