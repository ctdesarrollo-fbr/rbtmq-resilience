import { describe, expect, test } from "bun:test";
import { decodePublish, setupPublishing } from "./publishing.helper";

describe("publishTacticalEvent", () => {
    test("requires a routing key", async () => {
        const { resilience } = setupPublishing();

        await expect(resilience.publishTacticalEvent({ type: "T", payload: {} })).rejects.toThrow(
            "publishTacticalEvent requires a routingKey.",
        );
    });

    test("requires a routing key like domain.{subdomain|boundedContext}.messageType", async () => {
        const { resilience, channel } = setupPublishing();

        for (const routingKey of ["commerce.orders", "commerce.orders.checkout.created", "commerce.#"]) {
            await expect(resilience.publishTacticalEvent({ type: "T", payload: {}, routingKey })).rejects.toThrow(
                `publishTacticalEvent requires a routing key like domain.{subdomain|boundedContext}.messageType (got "${routingKey}").`,
            );
        }
        expect(channel.callsOf("publish")).toEqual([]);
    });

    test("publishes to the tactical exchange", async () => {
        const { resilience, channel } = setupPublishing();

        await resilience.publishTacticalEvent({
            type: "T",
            payload: { a: 1 },
            routingKey: "commerce.orders.x",
            options: { messageId: "t-1" },
        });

        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published).toEqual({
            exchange: "app.tactical",
            routingKey: "commerce.orders.x",
            payload: { a: 1 },
            options: {
                type: "T",
                messageId: "t-1",
                appId: "checkout-service",
                contentType: "application/json",
                persistent: true,
            },
        });
    });
});
