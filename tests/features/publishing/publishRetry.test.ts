import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { getTableName } from "drizzle-orm";
import { RabbitMQResilience, type RbtmqResilienceConfigDto } from "@/index";
import { AmqpPublisher, PublishMessageDto, UnroutableMessageError } from "@/amqp";
import { PublishRetryPolicyDto } from "@/features/publishing/publishRetry.dto";
import { PublishRetry } from "@/features/publishing/publishRetry";
import { fakeAmqp } from "@tests/helpers/fakeAmqp";
import { createFakeDatabase } from "@tests/helpers/fakeDatabase";
import { buildServiceConfig, withDb } from "@tests/helpers/config";
import { decodePublish } from "./publishing.helper";

/**
 * Publications are retried by the library itself (services do nothing): a nack, a closed
 * channel or a lost connection is retried with backoff, always with the same message id.
 */

let current: RabbitMQResilience | undefined;
const silenced = { error: console.error, warn: console.warn };
beforeEach(() => {
    console.error = () => {};
    console.warn = () => {};
});
afterEach(async () => {
    console.error = silenced.error;
    console.warn = silenced.warn;
    await current?.stop();
    current = undefined;
});

async function started(overrides: Partial<RbtmqResilienceConfigDto> = {}) {
    const channel = fakeAmqp.reset();
    const database = createFakeDatabase();
    current = new RabbitMQResilience(
        withDb(buildServiceConfig({ tacticalExchange: "app.tactical", ...overrides }), database.db),
    );
    await current.start();
    channel.calls.length = 0;
    return { resilience: current, channel, database };
}

function outboxStatuses(database: ReturnType<typeof createFakeDatabase>): string[] {
    return database.operations
        .map((operation, index) => ({ operation, index }))
        .filter(({ operation }) => operation.op === "insert" && getTableName(operation.args[0] as never) === "outbox")
        .map(({ index }) => (database.operations[index + 1]!.args[0] as { status: string }).status);
}

describe("publication retries", () => {
    test("a nacked event is published again with the same message id until the broker confirms it", async () => {
        const { resilience, channel, database } = await started();
        channel.nackNextPublishes = 2;

        const messageId = await resilience.publishEvent({ type: "OrderCreated", payload: { id: 1 } });

        const attempts = channel.callsOf("publish").map(decodePublish);
        expect(attempts).toHaveLength(3);
        expect(new Set(attempts.map((attempt) => attempt.options.messageId))).toEqual(new Set([messageId]));
        expect(outboxStatuses(database)).toEqual(["sent"]);
    });

    test("tactical events and commands are retried too", async () => {
        const { resilience, channel } = await started();

        channel.nackNextPublishes = 1;
        await resilience.publishTacticalEvent({ type: "PaymentAuthorized", routingKey: "commerce.orders.payment-authorized", payload: {} });
        channel.nackNextPublishes = 1;
        await resilience.sendCommand({ type: "ReserveStock", routingKey: "commerce.stock.commands", payload: {} });

        expect(channel.callsOf("publish").map((call) => decodePublish(call).exchange)).toEqual([
            "app.tactical",
            "app.tactical",
            "app.direct",
            "app.direct",
        ]);
    });

    test("gives up after the configured attempts, records it as failed once and throws", async () => {
        const { resilience, channel, database } = await started({ publishRetry: { attempts: 3, delayInMs: 0 } });
        channel.nackPublish = true;

        await expect(resilience.publishEvent({ type: "OrderCreated", payload: {} })).rejects.toThrow("message nacked");

        expect(channel.callsOf("publish")).toHaveLength(3);
        expect(outboxStatuses(database)).toEqual(["failed"]);
    });

    test("recovers when the channel was closed (e.g. RabbitMQ restarted)", async () => {
        // waits 10 ms, 20 ms, 40 ms…: the channel is back after ~15 ms, so the third attempt works
        const { resilience, channel } = await started({ publishRetry: { attempts: 6, delayInMs: 10 } });
        channel.failPublish = true;
        setTimeout(() => {
            channel.failPublish = false;
        }, 15);

        await resilience.publishEvent({ type: "OrderCreated", payload: {} });

        expect(channel.callsOf("publish")).toHaveLength(1);
    });

    test("attempts: 1 turns the retries off", async () => {
        const { resilience, channel } = await started({ publishRetry: { attempts: 1 } });
        channel.nackPublish = true;

        await expect(resilience.publishEvent({ type: "OrderCreated", payload: {} })).rejects.toThrow("message nacked");
        expect(channel.callsOf("publish")).toHaveLength(1);
    });

    test("a command no queue is bound to is not retried: retrying cannot fix it", async () => {
        const { resilience, channel } = await started();
        channel.unroutableKeys.add("commerce.stock.commands");

        await expect(
            resilience.sendCommand({ type: "ReserveStock", routingKey: "commerce.stock.commands", payload: {} }),
        ).rejects.toThrow("no queue is bound");
        expect(channel.callsOf("publish")).toHaveLength(1);
    });

    test("an invalid message is rejected without publishing or retrying", async () => {
        const { resilience, channel } = await started();

        await expect(resilience.publishEvent({ type: "", payload: {} })).rejects.toThrow("Invalid message");
        expect(channel.callsOf("publish")).toHaveLength(0);
    });
});

describe("PublishRetryPolicyDto", () => {
    test("defaults to 6 attempts waiting 1 s, 2 s, 4 s, 8 s and 16 s", () => {
        const policy = PublishRetryPolicyDto.create();

        expect(policy).toEqual(new PublishRetryPolicyDto(6, 1000, 16000));
        expect([1, 2, 3, 4, 5].map((retry) => policy.delayBeforeRetry(retry))).toEqual([1000, 2000, 4000, 8000, 16000]);
    });

    test("keeps what the service configures", () => {
        expect(PublishRetryPolicyDto.create({ attempts: 3, delayInMs: 500, maxDelayInMs: 2000 })).toEqual(
            new PublishRetryPolicyDto(3, 500, 2000),
        );
    });

    test("never fewer than 1 attempt", () => {
        expect(PublishRetryPolicyDto.create({ attempts: 0 }).attempts).toBe(1);
    });
});

describe("PublishRetry", () => {
    const message = new PublishMessageDto("app.integration", "", "OrderCreated", {}, "m-1");

    /** A publisher that fails the first `times` publications with `error`. */
    function failingPublisher(times: number, error: Error = new Error("channel closed")) {
        const publisher = { calls: 0 };
        const publishRaw = async () => {
            publisher.calls += 1;
            if (publisher.calls <= times) throw error;
            return message.messageId;
        };
        return Object.assign(publisher, { publishRaw }) as unknown as AmqpPublisher & { calls: number };
    }

    function publishRetry(publisher: AmqpPublisher, waits: number[] = []) {
        return new PublishRetry(publisher, new PublishRetryPolicyDto(4, 1000, 3000), async (ms) => {
            waits.push(ms);
        });
    }

    test("doubles the wait on every retry, up to maxDelayInMs", async () => {
        const publisher = failingPublisher(3);
        const waits: number[] = [];

        const messageId = await publishRetry(publisher, waits).publish(message, false);

        expect(messageId).toBe("m-1");
        expect(publisher.calls).toBe(4);
        expect(waits).toEqual([1000, 2000, 3000]);
    });

    test("throws the last error once the attempts run out", async () => {
        const publisher = failingPublisher(10);

        await expect(publishRetry(publisher).publish(message, false)).rejects.toThrow("channel closed");
        expect(publisher.calls).toBe(4);
    });

    test("does not retry an unroutable command", async () => {
        const publisher = failingPublisher(10, new UnroutableMessageError("app.direct", "rk"));
        const waits: number[] = [];

        await expect(publishRetry(publisher, waits).publish(message, true)).rejects.toThrow(UnroutableMessageError);
        expect(publisher.calls).toBe(1);
        expect(waits).toEqual([]);
    });
});
