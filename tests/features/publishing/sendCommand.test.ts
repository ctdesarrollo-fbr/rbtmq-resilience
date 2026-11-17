import { describe, expect, test } from "bun:test";
import { decodePublish, setupPublishing } from "./publishing.helper";

describe("sendCommand", () => {
    test("requires a routing key", async () => {
        const { resilience } = setupPublishing();

        await expect(resilience.sendCommand({ type: "C", payload: {} })).rejects.toThrow(
            "sendCommand requires a routingKey.",
        );
    });

    test("publishes to the direct exchange", async () => {
        const { resilience, channel } = setupPublishing();

        await resilience.sendCommand({
            type: "C",
            payload: { b: 2 },
            routingKey: "svc.commands",
            options: { messageId: "c-1" },
        });

        expect(channel.callsOf("publish").map(decodePublish)).toEqual([
            {
                exchange: "app.direct",
                routingKey: "svc.commands",
                payload: { b: 2 },
                options: {
                    type: "C",
                    messageId: "c-1",
                    appId: "checkout-service",
                    contentType: "application/json",
                    persistent: true,
                    // a command must reach its queue: unroutable commands fail instead of being lost
                    mandatory: true,
                },
            },
        ]);
    });
});
