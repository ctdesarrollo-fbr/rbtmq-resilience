import { afterEach, describe, expect, test } from "bun:test";
import {
    QueueNamePartsDto,
    RabbitMQResilience,
    RbtmqNaming,
    type RbtmqConsumerConfigDto,
    type RbtmqResilienceConfigDto,
} from "@/index";
import { fakeAmqp } from "@tests/helpers/fakeAmqp";
import { createFakeDatabase } from "@tests/helpers/fakeDatabase";
import { createFakeTransactions } from "@tests/helpers/fakeTransactions";

/**
 * A service consumes any combination of fanout events, topic events and commands (direct).
 * Only what it configures is declared; the direct exchange always is (retry and error queues).
 */

const FANOUT = "sagittarius-a";
const TOPIC = "sagittarius-a-topic";
const DIRECT = "sagittarius-a-direct";

const parts = new QueueNamePartsDto("logistics", "volume-reception");
const events = RbtmqNaming.consumerQueues(parts, "events");
const commandNames = RbtmqNaming.consumerQueues(parts, "commands");

/** Only what every service needs: connection, service name, direct exchange, retries, database. */
function baseConfig(overrides: Partial<RbtmqResilienceConfigDto> = {}): RbtmqResilienceConfigDto {
    return {
        rabbitMQConfigConnect: "amqp://rabbit",
        retryEndpoint: "volume-reception-service",
        prefetch: 1,
        directExchange: DIRECT,
        typeDirectExchange: "direct",
        eventResilienceHandlerConfig: { immediateRetryAttempts: 5, delayedRetryAttempts: 3, delayInMs: 10000 },
        mysql: { host: "db", user: "u", password: "p", database: "d" },
        db: createFakeDatabase().db,
        logs: false,
        inboxTransaction: createFakeTransactions().config,
        ...overrides,
    } as RbtmqResilienceConfigDto;
}

const fanout: Partial<RbtmqResilienceConfigDto> = { exchange: FANOUT, typeExchange: "fanout" };
const topic: Partial<RbtmqResilienceConfigDto> = { tacticalExchange: TOPIC, tacticalTypeExchange: "topic" };
const eventsQueue: Partial<RbtmqResilienceConfigDto> = {
    queue: events.queue,
    routingKey: events.routingKey,
    retryQueue: events.retryQueue,
    retryRoutingKey: events.retryRoutingKey,
    deadLetterQueue: events.deadLetterQueue,
    deadLetterRoutingKey: events.deadLetterRoutingKey,
    messageTTL: 10000,
    eventsToProcess: [],
};
const topicBinding: Partial<RbtmqResilienceConfigDto> = {
    tacticalBindings: [{ queue: events.queue, domain: "trade-logistics", level: "domain" }],
};
const commands: Partial<RbtmqResilienceConfigDto> = {
    commands: { name: "commands", kind: "commands", ...commandNames, messageTTL: 10000, processes: [] } as RbtmqConsumerConfigDto,
};

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

async function startWith(overrides: Partial<RbtmqResilienceConfigDto>) {
    const channel = fakeAmqp.reset();
    current = new RabbitMQResilience(baseConfig(overrides));
    await current.start();
    return {
        exchanges: channel.callsOf("assertExchange").map(([name, type]) => `${name}:${type}`),
        consumed: channel.callsOf("consume").map(([queue]) => queue),
        bindings: channel.callsOf("bindQueue").map(([queue, exchange, key]) => `${queue} -> ${exchange} (${key})`),
    };
}

describe("any combination of fanout, topic and direct", () => {
    test.each([
        ["only direct (commands)", { ...commands }, [`${DIRECT}:direct`], [commandNames.queue]],
        ["only fanout", { ...fanout, ...eventsQueue }, [`${FANOUT}:fanout`, `${DIRECT}:direct`], [events.queue]],
        ["only topic", { ...topic, ...eventsQueue, ...topicBinding }, [`${TOPIC}:topic`, `${DIRECT}:direct`], [events.queue]],
        ["fanout + topic, no direct", { ...fanout, ...topic, ...eventsQueue, ...topicBinding }, [`${FANOUT}:fanout`, `${TOPIC}:topic`, `${DIRECT}:direct`], [events.queue]],
        ["direct + fanout", { ...fanout, ...eventsQueue, ...commands }, [`${FANOUT}:fanout`, `${DIRECT}:direct`], [events.queue, commandNames.queue]],
        ["direct + topic", { ...topic, ...eventsQueue, ...topicBinding, ...commands }, [`${TOPIC}:topic`, `${DIRECT}:direct`], [events.queue, commandNames.queue]],
        ["direct + fanout + topic", { ...fanout, ...topic, ...eventsQueue, ...topicBinding, ...commands }, [`${FANOUT}:fanout`, `${TOPIC}:topic`, `${DIRECT}:direct`], [events.queue, commandNames.queue]],
        ["publishing only, no consumers", { ...fanout }, [`${FANOUT}:fanout`, `${DIRECT}:direct`], []],
    ] as const)("%s", async (_name, overrides, expectedExchanges, expectedQueues) => {
        const { exchanges, consumed } = await startWith(overrides);

        expect(exchanges).toEqual([...expectedExchanges]);
        expect(consumed).toEqual([...expectedQueues]);
    });

    test("only direct: the commands queue gets its direct bindings and nothing else", async () => {
        const { bindings } = await startWith({ ...commands });

        expect(bindings.sort()).toEqual([
            `${commandNames.deadLetterQueue} -> ${DIRECT} (logistics.dead-letter)`,
            `${commandNames.queue} -> ${DIRECT} (logistics.volume-reception.commands)`,
            `${commandNames.queue} -> ${DIRECT} (logistics.volume-reception.commands-retry)`,
            `${commandNames.retryQueue} -> ${DIRECT} (logistics.volume-reception.commands-delayed-retry)`,
        ]);
    });

    test("only topic: the events queue is bound to the topic exchange and never to a fanout one", async () => {
        const { bindings } = await startWith({ ...topic, ...eventsQueue, ...topicBinding });

        expect(bindings).toContain(`${events.queue} -> ${TOPIC} (trade-logistics.#)`);
        expect(bindings.some((binding) => binding.includes(` -> ${FANOUT} (`))).toBe(false);
    });

    test("only topic through a tacticalEvents consumer, without an events queue", async () => {
        const tactical = RbtmqNaming.consumerQueues(parts, "tacticalEvents");
        const { exchanges, consumed, bindings } = await startWith({
            ...topic,
            tacticalEvents: {
                name: "tactical",
                kind: "tacticalEvents",
                ...tactical,
                bindingRoutingKey: "trade-logistics.customs.#",
                messageTTL: 10000,
                processes: [],
            } as RbtmqConsumerConfigDto,
        });

        expect(exchanges).toEqual([`${TOPIC}:topic`, `${DIRECT}:direct`]);
        expect(consumed).toEqual([tactical.queue]);
        expect(bindings).toContain(`${tactical.queue} -> ${TOPIC} (trade-logistics.customs.#)`);
    });
});

describe("incomplete combinations fail with a clear message", () => {
    test("topic events need tacticalExchange when there is no integration exchange", () => {
        expect(() => new RabbitMQResilience(baseConfig({ ...eventsQueue, ...topicBinding }))).toThrow(
            "tacticalExchange is required to consume topic events",
        );
    });

    test("an events queue needs its retry loop names", () => {
        expect(() => new RabbitMQResilience(baseConfig({ ...fanout, queue: events.queue, routingKey: events.routingKey }))).toThrow(
            `the events queue "${events.queue}" also needs retryQueue, retryRoutingKey, deadLetterQueue, deadLetterRoutingKey, messageTTL`,
        );
    });

    test("eventsToProcess without an events queue", () => {
        expect(
            () => new RabbitMQResilience(baseConfig({ ...fanout, eventsToProcess: [{ eventType: "T", processes: [] }] })),
        ).toThrow("eventsToProcess requires queue");
    });

    test("publishing to an exchange the service did not configure", async () => {
        const resilience = new RabbitMQResilience(baseConfig({ ...commands }));
        fakeAmqp.reset();

        await expect(resilience.publishEvent({ type: "T", payload: {} })).rejects.toThrow(
            "publishEvent requires the integration exchange (config.exchange).",
        );
        await expect(resilience.publishTacticalEvent({ type: "T", payload: {}, routingKey: "a.b.c" })).rejects.toThrow(
            "publishTacticalEvent requires the topic exchange (config.tacticalExchange).",
        );
    });
});
