import { afterEach, describe, expect, test } from "bun:test";
import type { RabbitMQResilience } from "@/index";
import { buildConsumeMessage } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { decodePublish, setupPublishing } from "./publishing.helper";
import { EVENTS_QUEUE, startConsuming } from "../consuming/consuming.helper";

// Durable queues only survive a broker restart with their messages if the messages are persistent.
describe("persistent messages", () => {
    test("events, tactical events and commands are published as persistent", async () => {
        const { resilience, channel } = setupPublishing();

        await resilience.publishEvent({ type: "OrderCreated", payload: {} });
        await resilience.publishTacticalEvent({ type: "OrderCreated", payload: {}, routingKey: "commerce.orders.created" });
        await resilience.sendCommand({ type: "ShipOrder", payload: {}, routingKey: "logistics.shipping.commands" });

        const published = channel.callsOf("publish").map(decodePublish);
        expect(published).toHaveLength(3);
        for (const message of published) expect(message.options.persistent).toBe(true);
    });

    let current: RabbitMQResilience | undefined;
    afterEach(async () => {
        await current?.stop();
        current = undefined;
    });

    async function failingConsumer(delayedRetryAttempts: number) {
        const started = await startConsuming({
            eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts, delayInMs: 1000 },
            eventsToProcess: [
                eventProcess("OrderCreated", "h", async () => {
                    throw new Error("boom");
                }),
            ],
        });
        current = started.resilience;
        return started.channel;
    }

    test("the retry queue copy is persistent", async () => {
        const channel = await failingConsumer(3);

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        const [retry] = channel.callsOf("publish").map(decodePublish);
        expect(retry?.routingKey).toEndWith("-retry");
        expect(retry?.options.persistent).toBe(true);
    });

    test("the poison message in the error queue is persistent", async () => {
        const channel = await failingConsumer(0);

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        const [error] = channel.callsOf("publish").map(decodePublish);
        expect(error?.routingKey).toEndWith("-error");
        expect(error?.options.persistent).toBe(true);
    });
});
