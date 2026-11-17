import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { getTableName } from "drizzle-orm";
import type { RabbitMQResilience } from "@/index";
import { buildConsumeMessage, fakeAmqp, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { decodePublish, EVENTS_QUEUE, startConsuming } from "./consuming.helper";

/**
 * What happens when RabbitMQ fails: reconnection (topology and consumers again) and publisher
 * confirms (a message the broker did not take is never treated as sent or acked).
 */

let current: RabbitMQResilience | undefined;
const original = { log: console.log, error: console.error, warn: console.warn };
let logged: string[] = [];

beforeEach(() => {
    logged = [];
    console.log = console.warn = () => {};
    console.error = (line: unknown) => void logged.push(String(line));
});

afterEach(async () => {
    Object.assign(console, original);
    await current?.stop();
    current = undefined;
});

async function start(handler: () => Promise<void> = async () => {}, overrides: Parameters<typeof startConsuming>[0] = {}) {
    const started = await startConsuming({
        eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 3, delayInMs: 1000 },
        eventsToProcess: [eventProcess("OrderCreated", "h", handler)],
        ...overrides,
    });
    current = started.resilience;
    return started;
}

async function waitFor(condition: () => boolean): Promise<void> {
    for (let i = 0; i < 100 && !condition(); i += 1) await new Promise((resolve) => setTimeout(resolve, 5));
    expect(condition()).toBe(true);
}

describe("reconnection", () => {
    test("when the channel closes, it reconnects, declares the topology and registers the consumers again", async () => {
        let handled = 0;
        const { channel: lost } = await start(async () => void handled++);
        const connections = fakeAmqp.connectedUrls.length;

        const fresh: FakeChannel = fakeAmqp.nextChannel();
        lost.simulateClose();
        await waitFor(() => fresh.callsOf("consume").length > 0);

        expect(fakeAmqp.connectedUrls).toHaveLength(connections + 1);
        expect(fresh.callsOf("assertExchange").length).toBeGreaterThan(0);
        expect(fresh.callsOf("assertQueue").length).toBeGreaterThan(0);
        expect(fresh.callsOf("prefetch")).toEqual([[10]]);
        expect(fresh.callsOf("consume")).toEqual([[EVENTS_QUEUE]]);
        expect(logged).toContain("[rbtmq-resilience] RabbitMQ connection lost; reconnecting.");

        // and it keeps consuming on the new channel
        await fresh.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));
        expect(handled).toBe(1);
        expect(fresh.callsOf("ack")).toEqual([["m-1"]]);
    });

    test("a message whose channel closed while it was handled is not acked on another channel", async () => {
        let lost: FakeChannel | undefined;
        const started = await start(async () => lost?.simulateClose());
        lost = started.channel;
        const fresh = fakeAmqp.nextChannel();

        await lost.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));
        await waitFor(() => fresh.callsOf("consume").length > 0);

        // the delivery tag only exists on the closed channel: RabbitMQ redelivers the message itself
        expect(lost.callsOf("ack")).toEqual([]);
        expect(fresh.callsOf("ack")).toEqual([]);
        expect(logged.some((line) => line.includes("requeued OrderCreated m-1") && line.includes("channel closed before the ack"))).toBe(true);
    });

    test("stop() does not reconnect", async () => {
        const { resilience } = await start();
        const connections = fakeAmqp.connectedUrls.length;

        await resilience.stop();
        current = undefined;
        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(fakeAmqp.connectedUrls).toHaveLength(connections);
    });
});

describe("publisher confirms", () => {
    test("a delayed retry the broker did not take is requeued, never acked", async () => {
        const { channel } = await start(async () => {
            throw new Error("boom");
        });
        channel.nackPublish = true;

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        expect(channel.callsOf("ack")).toEqual([]);
        expect(channel.callsOf("nack")).toEqual([["m-1", false, true]]);
        expect(logged.some((line) => line.includes("requeued OrderCreated m-1") && line.includes("message nacked"))).toBe(true);
    });

    test("a retry no queue received (wrong retry key) is requeued instead of lost", async () => {
        const { channel } = await start(async () => {
            throw new Error("boom");
        });
        channel.unroutableKeys.add("commerce.orders.checkout.events-retry");

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: "m-1", type: "OrderCreated" }));

        const [retry] = channel.callsOf("publish").map(decodePublish);
        expect(retry?.options.mandatory).toBe(true);
        expect(channel.callsOf("ack")).toEqual([]);
        expect(channel.callsOf("nack")).toEqual([["m-1", false, true]]);
        expect(logged.some((line) => line.includes('no queue is bound to exchange "app.direct" with routing key "commerce.orders.checkout.events-retry"'))).toBe(true);
    });

    test("a command no service queue receives fails and is recorded as failed", async () => {
        const { resilience, channel, database } = await start();
        channel.unroutableKeys.add("billing.invoices.commands");

        await expect(
            resilience.sendCommand({ type: "CreateInvoice", payload: {}, routingKey: "billing.invoices.commands", options: { messageId: "c-1" } }),
        ).rejects.toThrow('no queue is bound to exchange "app.direct" with routing key "billing.invoices.commands"');

        const history = database.operations.findIndex((operation) => operation.op === "insert" && getTableName(operation.args[0] as never) === "outbox");
        expect(database.operations[history + 1]!.args[0]).toMatchObject({ messageId: "c-1", status: "failed" });
    });

    test("an event nobody is subscribed to yet is still published (events are not mandatory)", async () => {
        const { resilience, channel } = await start();
        channel.unroutableKeys.add("");

        await expect(resilience.publishEvent({ type: "OrderCreated", payload: {}, options: { messageId: "e-1" } })).resolves.toBe("e-1");
        expect(decodePublish(channel.callsOf("publish")[0]!).options.mandatory).toBeUndefined();
    });

    test("a publication the broker nacks fails and is not recorded as sent", async () => {
        const { resilience, channel, database } = await start();
        channel.nackPublish = true;

        await expect(resilience.publishEvent({ type: "OrderCreated", payload: {}, options: { messageId: "e-1" } })).rejects.toThrow("message nacked");

        const history = database.operations.findIndex((operation) => operation.op === "insert" && getTableName(operation.args[0] as never) === "outbox");
        expect(database.operations[history + 1]!.args[0]).toMatchObject({ messageId: "e-1", status: "failed", error: "message nacked" });
    });
});
