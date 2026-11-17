import { describe, expect, test } from "bun:test";
import { RabbitMQMessageDto } from "@/index";
import { decodePublish, setupPublishing } from "./publishing.helper";

describe("publishEvent", () => {
    test("publishes to the integration exchange without headers or timestamp", async () => {
        const { resilience, channel } = setupPublishing();

        const messageId = await resilience.publishEvent({
            type: "OrderCreated",
            payload: { id: 1 },
            routingKey: "orders",
            options: { messageId: "m-1", headers: { ignored: true } },
        });

        expect(messageId).toBe("m-1");
        expect(channel.callsOf("publish").map(decodePublish)).toEqual([
            {
                exchange: "app.integration",
                routingKey: "orders",
                payload: { id: 1 },
                options: {
                    type: "OrderCreated",
                    messageId: "m-1",
                    appId: "checkout-service",
                    contentType: "application/json",
                    persistent: true,
                },
            },
        ]);
    });

    test("defaults the routing key to empty and generates a message id", async () => {
        const { resilience, channel } = setupPublishing();

        const messageId = await resilience.publishEvent({ type: "OrderCreated", payload: {} });

        expect(messageId).toMatch(/^[0-9a-f-]{36}$/);
        const [published] = channel.callsOf("publish").map(decodePublish);
        expect(published?.routingKey).toBe("");
        expect(published?.options.messageId).toBe(messageId);
    });

    test("accepts a RabbitMQMessageDto (legacy event manager shape)", async () => {
        const { resilience, channel } = setupPublishing();

        const messageId = await resilience.publishEvent(
            new RabbitMQMessageDto(
                Buffer.from(JSON.stringify({ id: 7 })),
                { consumerTag: "", deliveryTag: 0, exchange: "", routingKey: "rk", redelivered: false },
                { type: "Legacy", messageId: "legacy-1", headers: { h: 1 } },
            ),
        );

        expect(messageId).toBe("legacy-1");
        expect(channel.callsOf("publish").map(decodePublish)).toEqual([
            {
                exchange: "app.integration",
                routingKey: "rk",
                payload: { id: 7 },
                options: {
                    type: "Legacy",
                    messageId: "legacy-1",
                    appId: "checkout-service",
                    contentType: "application/json",
                    persistent: true,
                },
            },
        ]);
    });

    test("rejects a message without type or payload, without publishing it", async () => {
        const { resilience, channel } = setupPublishing();

        await expect(resilience.publishEvent({ type: "", payload: undefined })).rejects.toThrow(
            "Invalid message: Type is required, Payload is required",
        );
        expect(channel.callsOf("publish")).toEqual([]);
    });
});
