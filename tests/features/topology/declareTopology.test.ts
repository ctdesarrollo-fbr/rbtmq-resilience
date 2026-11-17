import { beforeEach, describe, expect, test } from "bun:test";
import { RabbitMQResilience } from "@/index";
import { fakeAmqp, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { buildServiceConfig } from "@tests/helpers/config";

describe("declareTopology", () => {
    let channel: FakeChannel;

    beforeEach(() => {
        channel = fakeAmqp.reset();
    });

    test("declares exchanges, resilient queues and bindings for a minimal service config", async () => {
        const resilience = new RabbitMQResilience(buildServiceConfig());

        await resilience.declareTopology();

        expect(channel.calls).toMatchSnapshot();
    });

    test("declares commands, tactical events, tactical bindings, extra consumers and bindings", async () => {
        const resilience = new RabbitMQResilience(
            buildServiceConfig({
                tacticalExchange: "app.tactical",
                commands: {
                    name: "commands",
                    kind: "commands",
                    queue: "svc.commands",
                    routingKey: "svc.commands",
                    retryQueue: "svc.commands-retry",
                    retryRoutingKey: "svc.commands-retry",
                    deadLetterQueue: "svc.commands-error",
                    deadLetterRoutingKey: "svc.commands-error",
                    messageTTL: 5000,
                    processes: [],
                },
                tacticalEvents: {
                    name: "tactical",
                    kind: "tacticalEvents",
                    queue: "svc.tactical",
                    routingKey: "svc.tactical",
                    retryQueue: "svc.tactical-retry",
                    retryRoutingKey: "svc.tactical-retry",
                    deadLetterQueue: "svc.tactical-error",
                    deadLetterRoutingKey: "svc.tactical-error",
                    messageTTL: 5000,
                    processes: [],
                    bindingRoutingKey: "commerce.orders.#",
                },
                tacticalBindings: [
                    { queue: "svc.tactical", domain: "commerce", level: "domain" },
                    { queue: "svc.tactical", domain: "commerce", subdomain: "orders", level: "subdomain" },
                    {
                        queue: "svc.tactical",
                        domain: "commerce",
                        boundedContext: "checkout",
                        level: "boundedContext",
                    },
                ],
                additionalConsumers: [
                    {
                        name: "extra",
                        kind: "events",
                        queue: "svc.extra",
                        routingKey: "svc.extra",
                        retryQueue: "svc.extra-retry",
                        retryRoutingKey: "svc.extra-retry",
                        deadLetterQueue: "svc.extra-error",
                        deadLetterRoutingKey: "svc.extra-error",
                        messageTTL: 1000,
                        processes: [],
                        prefetch: 3,
                    },
                ],
                bindings: [{ queue: "svc.extra", exchange: "other.exchange" }],
            }),
        );

        await resilience.declareTopology();

        expect(channel.calls).toMatchSnapshot();
    });

    test("wraps exchange declaration errors with an actionable message", async () => {
        channel.failAssertExchange = true;
        const resilience = new RabbitMQResilience(buildServiceConfig());

        const error = await resilience.declareTopology().catch((caught: Error) => caught);

        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toBe(
            'RabbitMQ exchange "app.integration" could not be declared as "fanout". ' +
                "If it already exists in the broker with another type, RabbitMQ will " +
                "reject the declaration. Use a different exchange name, match the " +
                "existing exchange type, or delete/recreate the exchange in RabbitMQ.",
        );
        expect(((error as Error).cause as Error).message).toBe("PRECONDITION_FAILED");
    });
});
