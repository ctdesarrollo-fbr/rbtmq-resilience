import { beforeEach, describe, expect, test } from "bun:test";
import {
    RabbitMQResilience,
    createRabbitMqResilience,
    RabbitMqResilience,
    type RbtmqResilienceRuntimeConfigDto,
} from "@/index";
import { fakeAmqp, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { buildServiceConfig } from "@tests/helpers/config";

describe("mapConfig", () => {
    let channel: FakeChannel;

    beforeEach(() => {
        channel = fakeAmqp.reset();
    });

    test("builds the AMQP url from connection options, encoding credentials and vhost", async () => {
        await new RabbitMQResilience(buildServiceConfig()).declareTopology();

        expect(fakeAmqp.connectedUrls).toEqual(["amqp://user%40x:p%2Fss@rabbit.local:5672/%2F"]);
    });

    test("uses a string connection as-is", async () => {
        await new RabbitMQResilience(
            buildServiceConfig({ rabbitMQConfigConnect: "amqp://guest:guest@localhost:5672" }),
        ).declareTopology();

        expect(fakeAmqp.connectedUrls).toEqual(["amqp://guest:guest@localhost:5672"]);
    });

    test("falls back to default exchange kinds when the configured type is unknown", async () => {
        await new RabbitMQResilience(
            buildServiceConfig({ typeExchange: "weird", typeDirectExchange: "nope" }),
        ).declareTopology();

        expect(channel.callsOf("assertExchange")).toEqual([
            // the service does not consume topic events, so no topic exchange is declared
            ["app.integration", "fanout", { durable: true }],
            ["app.direct", "direct", { durable: true }],
        ]);
    });

    test("rejects the same exchange name configured with incompatible types", () => {
        expect(
            () => new RabbitMQResilience(buildServiceConfig({ directExchange: "app.integration" })),
        ).toThrow(
            'RabbitMQ exchange "app.integration" is configured with incompatible types: ' +
                '"fanout" and "direct". RabbitMQ does not allow one exchange ' +
                "name to be fanout/topic/direct at the same time. Use different exchange " +
                "names or configure both usages with the same type.",
        );
    });

    test("rejects a tactical exchange that reuses the integration exchange name", () => {
        expect(
            () => new RabbitMQResilience(buildServiceConfig({ tacticalExchange: "app.integration" })),
        ).toThrow('"fanout" and "topic"');
    });

    test("accepts a runtime config without mapping it", async () => {
        const runtime: RbtmqResilienceRuntimeConfigDto = {
            serviceName: "svc",
            amqp: { url: "amqp://runtime" },
            exchanges: {
                integration: { name: "i" },
                tactical: { name: "t" },
                direct: { name: "d" },
            },
            retry: { immediateRetries: 1, delayedAttempts: 1, delayMs: 100, prefetch: 1 },
        };

        await createRabbitMqResilience(runtime).declareTopology();

        expect(fakeAmqp.connectedUrls).toEqual(["amqp://runtime"]);
        expect(channel.callsOf("assertExchange")).toEqual([
            ["i", "fanout", { durable: true }],
            ["t", "topic", { durable: true }],
            ["d", "direct", { durable: true }],
        ]);
    });

    test("exposes the same class through every public alias", () => {
        expect(RabbitMqResilience).toBe(RabbitMQResilience);
        expect(RabbitMQResilience.initialize(buildServiceConfig())).toBeInstanceOf(RabbitMQResilience);
    });
});
