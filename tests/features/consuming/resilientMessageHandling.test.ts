import { afterEach, describe, expect, test } from "bun:test";
import type { ConsumeMessage } from "amqplib";
import { QueueNamePartsDto, RbtmqNaming, type RabbitMQResilience } from "@/index";
import { buildConsumeMessage, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { decodePublish, startConsuming } from "./consuming.helper";

/**
 * End-to-end checks of resilient message handling (inbox, immediate and delayed retries, error
 * queue) and of the contract with the failed-messages-monitoring service, using the queue names
 * RbtmqNaming generates.
 */

const DIRECT = "sagittarius-a-direct";
const names = RbtmqNaming.consumerQueues(new QueueNamePartsDto("logistics", "volume-reception"), "events");

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

async function start(handler: () => Promise<void>, overrides: Parameters<typeof startConsuming>[0] = {}) {
    const started = await startConsuming({
        exchange: "sagittarius-a",
        typeExchange: "fanout",
        directExchange: DIRECT,
        retryEndpoint: "volume-reception-service",
        queue: names.queue,
        routingKey: names.routingKey,
        retryQueue: names.retryQueue,
        retryRoutingKey: names.retryRoutingKey,
        deadLetterQueue: names.deadLetterQueue,
        deadLetterRoutingKey: names.deadLetterRoutingKey,
        messageTTL: 10000,
        eventResilienceHandlerConfig: { immediateRetryAttempts: 2, delayedRetryAttempts: 3, delayInMs: 10000 },
        eventsToProcess: [eventProcess("ManifestFinalized", "updateVolumes", handler)],
        ...overrides,
    });
    current = started.resilience;
    return started;
}

/**
 * What RabbitMQ does when the retry queue's TTL expires: it dead-letters the message back to the
 * primary queue through the direct exchange, with the retry queue's x-dead-letter-routing-key
 * and the headers the library set.
 */
function deadLetterFromRetryQueue(publishArgs: unknown[]): ConsumeMessage {
    const { payload, options } = decodePublish(publishArgs);
    return buildConsumeMessage({
        messageId: options.messageId as string,
        type: options.type as string,
        appId: options.appId as string,
        payload,
        headers: options.headers as Record<string, unknown>,
        exchange: DIRECT,
        routingKey: names.routingKey,
    });
}

/** Delivers a message and follows every delayed retry until it is not sent to the retry queue again. */
async function runUntilSettled(channel: FakeChannel, first: ConsumeMessage) {
    await channel.deliver(names.queue, first);
    for (;;) {
        const last = channel.callsOf("publish").at(-1);
        if (!last || decodePublish(last).routingKey !== names.retryRoutingKey) return last;
        await channel.deliver(names.queue, deadLetterFromRetryQueue(last));
    }
}

describe("resilient message handling", () => {
    test("a body that is not valid JSON also goes through every delayed retry before the error queue", async () => {
        const { channel } = await start(async () => {});
        type RawPublish = [string, string, Buffer, Record<string, unknown>];

        await channel.deliver(names.queue, buildConsumeMessage({
            messageId: "m-1", type: "ManifestFinalized", rawContent: "{not json", exchange: "sagittarius-a",
        }));
        for (;;) {
            const [, routingKey, body, options] = channel.callsOf("publish").at(-1) as RawPublish;
            if (routingKey !== names.retryRoutingKey) break;
            expect(body.toString("utf8")).toBe("{not json"); // raw body kept on every hop
            await channel.deliver(names.queue, buildConsumeMessage({
                messageId: options.messageId as string,
                type: options.type as string,
                appId: options.appId as string,
                rawContent: body.toString("utf8"),
                headers: options.headers as Record<string, unknown>,
                exchange: DIRECT,
                routingKey: names.routingKey,
            }));
        }

        const routingKeys = channel.callsOf("publish").map((args) => (args as RawPublish)[1]);
        expect(routingKeys).toEqual([
            names.retryRoutingKey, names.retryRoutingKey, names.retryRoutingKey, "logistics.dead-letter",
        ]);
    });

    test("total attempts = (immediate retries + 1) * (delayed retries + 1), then the error queue", async () => {
        let runs = 0;
        const { channel, resilience } = await start(async () => {
            runs += 1;
            throw new Error("still failing");
        });

        const last = await runUntilSettled(
            channel,
            buildConsumeMessage({ messageId: "m-1", type: "ManifestFinalized", payload: { id: 1 }, exchange: "sagittarius-a" }),
        );

        expect(runs).toBe((2 + 1) * (3 + 1));
        expect(decodePublish(last!).routingKey).toBe("logistics.dead-letter");
        // 3 delayed retries, then the error queue
        const routingKeys = channel.callsOf("publish").map((args) => decodePublish(args).routingKey);
        expect(routingKeys).toEqual([names.retryRoutingKey, names.retryRoutingKey, names.retryRoutingKey, "logistics.dead-letter"]);
        // every delivery is acknowledged: nothing is left unacked or requeued
        expect(channel.callsOf("ack")).toHaveLength(4);
        expect(channel.callsOf("nack")).toEqual([]);
    });

    test("delayed retries: redelivery_count += 1, expiration = redelivery_count * 2000, retry_endpoint = service", async () => {
        const { channel } = await start(async () => {
            throw new Error("boom");
        });

        await runUntilSettled(channel, buildConsumeMessage({ messageId: "m-1", type: "ManifestFinalized" }));

        const retries = channel
            .callsOf("publish")
            .map(decodePublish)
            .filter((published) => published.routingKey === names.retryRoutingKey);
        expect(retries.map((retry) => retry.exchange)).toEqual([DIRECT, DIRECT, DIRECT]);
        expect(retries.map((retry) => retry.options.expiration)).toEqual(["2000", "4000", "6000"]);
        expect(retries.map((retry) => (retry.options.headers as Record<string, unknown>).redelivery_count)).toEqual([1, 2, 3]);
        expect(retries.every((retry) => (retry.options.headers as Record<string, unknown>).retry_endpoint === "volume-reception-service")).toBe(true);
    });

    test("declares the retry and error topology on the direct exchange", async () => {
        const { channel } = await start(async () => {});

        expect(channel.callsOf("assertQueue")).toContainEqual([
            names.retryQueue,
            { durable: true, messageTtl: 10000, deadLetterExchange: DIRECT, deadLetterRoutingKey: "logistics.volume-reception.retry" },
        ]);
        expect(channel.callsOf("assertQueue")).toContainEqual(["logistics.dead-letter", { durable: true }]);
        // only the retry queue has x-dead-letter-* arguments
        expect(channel.callsOf("assertQueue")).toContainEqual([names.queue, { durable: true }]);
        expect(channel.callsOf("bindQueue")).toEqual(expect.arrayContaining([
            [names.queue, DIRECT, "logistics.volume-reception.retry"],
            [names.retryQueue, DIRECT, "logistics.volume-reception.delayed-retry"],
            ["logistics.dead-letter", DIRECT, "logistics.dead-letter"],
        ]));
    });

    test("fanout and topic at the same time: one primary queue bound to both exchanges, one retry loop", async () => {
        let runs = 0;
        const { channel } = await start(
            async () => {
                runs += 1;
                throw new Error("boom");
            },
            {
                tacticalExchange: "sagittarius-a-topic",
                tacticalTypeExchange: "topic",
                tacticalBindings: [{ queue: names.queue, domain: "trade-logistics", level: "domain" }],
                eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 3, delayInMs: 10000 },
            },
        );

        expect(channel.callsOf("assertExchange")).toEqual(expect.arrayContaining([
            ["sagittarius-a", "fanout", { durable: true }],
            ["sagittarius-a-topic", "topic", { durable: true }],
        ]));
        expect(channel.callsOf("bindQueue")).toEqual(expect.arrayContaining([
            [names.queue, "sagittarius-a", names.routingKey],
            [names.queue, "sagittarius-a-topic", "trade-logistics.#"],
        ]));
        // only one primary queue is consumed
        expect(channel.callsOf("consume")).toEqual([[names.queue]]);

        await channel.deliver(names.queue, buildConsumeMessage({
            messageId: "m-topic",
            type: "ManifestFinalized",
            exchange: "sagittarius-a-topic",
            routingKey: "trade-logistics.shipment.manifest-finalized",
        }));

        expect(runs).toBe(1);
        const retries = channel.callsOf("publish").map(decodePublish);
        expect(retries.map((retry) => [retry.exchange, retry.routingKey])).toEqual([[DIRECT, "logistics.volume-reception.delayed-retry"]]);
    });
});

/**
 * What failed-messages-monitoring does with a message of the error queue
 * (message-bus/inbox-message-handler.js and models/poison-message.js), and how it replays it
 * (message-bus/workers/producer.js). Kept in sync by hand with that service.
 */
type PoisonMessage = {
    message_id?: string;
    endpoint?: { name?: string; delivery_metadata?: Record<string, string> };
    exception_details?: unknown;
    payload?: unknown;
};

const monitoring = {
    store(message: { content: Buffer; properties: { messageId?: string; headers?: Record<string, unknown> } }): PoisonMessage {
        const parsedMessage = JSON.parse(message.content.toString());
        const headers = message.properties.headers ?? {};
        return {
            message_id: message.properties.messageId,
            endpoint: headers.endpoint as PoisonMessage["endpoint"],
            exception_details: headers.exception_details,
            ...parsedMessage,
        };
    },
    replay(poisonMessage: PoisonMessage) {
        const deliveryMetadata = poisonMessage.endpoint?.delivery_metadata;
        if (!deliveryMetadata || typeof deliveryMetadata !== "object") throw new Error("Delivery metadata does not exist.");
        if (!("message_type" in deliveryMetadata)) throw new Error("Delivery metadata does not have a message_type property.");
        if (!("exchange" in deliveryMetadata)) throw new Error("Delivery metadata does not have an exchange property.");
        if (!poisonMessage.message_id) throw new Error("Message ID is invalid.");
        return {
            exchange: deliveryMetadata.exchange,
            routingKey: deliveryMetadata.routing_key ?? "",
            payload: poisonMessage.payload,
            properties: {
                messageId: poisonMessage.message_id,
                appId: poisonMessage.endpoint?.name,
                type: deliveryMetadata.message_type,
            },
        };
    },
};

describe("contract with failed-messages-monitoring", () => {
    async function poisonMessageOf(channel: FakeChannel) {
        const last = await runUntilSettled(
            channel,
            buildConsumeMessage({ messageId: "m-1", type: "ManifestFinalized", payload: { id: 7 }, exchange: "sagittarius-a", appId: "shipment" }),
        );
        const [, , body, options] = last as [string, string, Buffer, { messageId: string; headers: Record<string, unknown> }];
        return monitoring.store({ content: body, properties: { messageId: options.messageId, headers: options.headers } });
    }

    test("the error queue message has what the monitoring service stores", async () => {
        const { channel } = await start(async () => {
            throw new TypeError("volume not found");
        });

        const poisonMessage = await poisonMessageOf(channel);

        expect(poisonMessage.message_id).toBe("m-1");
        expect(poisonMessage.payload).toEqual({ id: 7 });
        expect(poisonMessage.endpoint?.name).toBe("volume-reception-service");
        expect(poisonMessage.endpoint?.delivery_metadata).toMatchObject({ message_type: "ManifestFinalized", exchange: DIRECT });
        // the shape Failed Messages Monitoring reads
        expect(poisonMessage.exception_details).toEqual({
            exception_type: "TypeError",
            stack_trace: expect.stringContaining("TypeError: volume not found"),
            failed_at: expect.stringMatching(/^\d{4}-\d{2}-\d{2}T/),
        });
        expect(Object.keys(poisonMessage.endpoint?.delivery_metadata ?? {}).sort()).toEqual(
            ["exchange", "message_type", "routing_key", "sender"],
        );
    });

    test("a replayed message reaches only this service's primary queue and is handled as a new message", async () => {
        let fail = true;
        let handled = 0;
        const { channel, resilience } = await start(async () => {
            if (fail) throw new Error("db down");
            handled += 1;
        });
        const replay = monitoring.replay(await poisonMessageOf(channel));

        // after the delayed retries the message came from the direct exchange with the primary's key,
        // so the replay targets this consumer only, not every subscriber of the fanout exchange
        expect(replay.exchange).toBe(DIRECT);
        expect(replay.routingKey).toBe(names.routingKey);

        fail = false;
        await channel.deliver(names.queue, buildConsumeMessage({
            ...replay.properties,
            payload: replay.payload,
            exchange: replay.exchange,
            routingKey: replay.routingKey,
        }));

        expect(handled).toBe(1);
    });

    /** Poison message straight from the fanout exchange, without going through the retry queue. */
    async function firstDeliveryPoisonMessage(channel: FakeChannel, input: Parameters<typeof buildConsumeMessage>[0]) {
        await channel.deliver(names.queue, buildConsumeMessage({
            messageId: "m-1",
            type: "ManifestFinalized",
            exchange: "sagittarius-a",
            routingKey: "",
            appId: "shipment",
            ...input,
        }));
        const [error] = channel.callsOf("publish");
        const [, routingKey, body, options] = error as [string, string, Buffer, { messageId: string; headers: Record<string, unknown> }];
        expect(routingKey).toBe(names.deadLetterRoutingKey);
        return monitoring.store({ content: body, properties: { messageId: options.messageId, headers: options.headers } });
    }

    test("an invalid JSON body that reaches the error queue on its first delivery is replayed only to this service", async () => {
        const { channel } = await start(async () => {}, {
            eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 0, delayInMs: 10000 },
        });

        const replay = monitoring.replay(await firstDeliveryPoisonMessage(channel, { rawContent: "{not json" }));

        expect(replay.exchange).toBe(DIRECT);
        expect(replay.routingKey).toBe(names.routingKey);
    });

    test("with no delayed retries, the first failure is replayed only to this service", async () => {
        const { channel } = await start(async () => {
            throw new Error("db down");
        }, { eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 0, delayInMs: 10000 } });

        const poisonMessage = await firstDeliveryPoisonMessage(channel, { payload: { id: 7 } });
        const replay = monitoring.replay(poisonMessage);

        expect(replay.exchange).toBe(DIRECT);
        expect(replay.routingKey).toBe(names.routingKey);
        expect(poisonMessage.endpoint?.delivery_metadata?.sender).toBe("shipment");
    });
});
