import { describe, expect, test } from "bun:test";
import { RabbitMQResilience, RbtmqConsumerConfigDto, RbtmqNaming, QueueNamePartsDto } from "@/index";
import { buildServiceConfig } from "@tests/helpers/config";

const parts = new QueueNamePartsDto("logistics", "volume-reception");

function consumer(name: string, kind: RbtmqConsumerConfigDto["kind"], names = RbtmqNaming.consumerQueues(parts, kind)) {
    return { name, kind, ...names, messageTTL: 10000, processes: [] } as RbtmqConsumerConfigDto;
}

describe("validateConfig", () => {
    test("rejects two consumers that share a retry routing key", () => {
        const events = RbtmqNaming.consumerQueues(parts, "events");
        expect(
            () => new RabbitMQResilience(buildServiceConfig({
                queue: events.queue,
                routingKey: "logistics.volume-reception.events-direct",
                retryRoutingKey: events.retryRoutingKey,
                additionalConsumers: [consumer("copy", "events", { ...events, queue: "other.events" })],
            })),
        ).toThrow('retry routing key "logistics.volume-reception.delayed-retry" is used by consumers');
    });

    test("accepts an events queue and a commands queue of the same service", () => {
        const events = RbtmqNaming.consumerQueues(parts, "events");
        expect(
            () => new RabbitMQResilience(buildServiceConfig({
                queue: events.queue,
                routingKey: events.routingKey,
                retryQueue: events.retryQueue,
                retryRoutingKey: events.retryRoutingKey,
                commands: consumer("commands", "commands"),
            })),
        ).not.toThrow();
    });

    test("a separate tactical events queue next to the events queue is rejected, pointing to tacticalBindings", () => {
        const events = RbtmqNaming.consumerQueues(parts, "events");
        expect(
            () => new RabbitMQResilience(buildServiceConfig({
                queue: events.queue,
                routingKey: events.routingKey,
                retryQueue: events.retryQueue,
                retryRoutingKey: events.retryRoutingKey,
                tacticalEvents: { ...consumer("tactical", "tacticalEvents"), queue: "logistics.volume-reception.topic" },
            })),
        ).toThrow("use one events queue bound to both exchanges (tacticalBindings with that queue)");
    });

    test("requires inboxTransaction when the service consumes messages", () => {
        expect(() => new RabbitMQResilience(buildServiceConfig({ inboxTransaction: undefined }))).toThrow(
            "rbtmq-resilience: inboxTransaction is required to consume messages.",
        );
    });

    test("a service that only publishes does not need inboxTransaction", () => {
        const publishOnly = buildServiceConfig({ inboxTransaction: undefined, queue: undefined, eventsToProcess: undefined });
        expect(() => new RabbitMQResilience(publishOnly)).not.toThrow();
    });

    test("warns when poison messages do not go to the domain's dead-letter queue", () => {
        const warnings = new RabbitMQResilience(buildServiceConfig()).getConfigWarnings();
        expect(warnings).toContainEqual(expect.stringContaining('sends poison messages to "commerce.orders.checkout.events-error"'));

        const ok = new RabbitMQResilience(buildServiceConfig({
            deadLetterQueue: "commerce.dead-letter",
            deadLetterRoutingKey: "commerce.dead-letter",
        }));
        expect(ok.getConfigWarnings().some((warning) => warning.includes("poison messages"))).toBe(false);
    });
});
