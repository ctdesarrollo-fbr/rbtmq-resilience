import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import type { RabbitMQResilience } from "@/index";
import { buildConsumeMessage } from "@tests/helpers/fakeAmqp";
import { eventProcess } from "@tests/helpers/config";
import { EVENTS_QUEUE, startConsuming } from "./consuming.helper";

/** One short line per message: status, type, id, duration and, when useful, target and reason. */

const ID = "3fa85f64-5717-4562-b3fc-2c963f66afa6";
const DURATION = String.raw`\(\d+ ms\)`;
/** Start of every line, followed by one space. */
const PREFIX = String.raw`\[rbtmq-resilience\] `;

type Captured = { level: "log" | "warn" | "error"; line: string };
let captured: Captured[] = [];
const original = { log: console.log, warn: console.warn, error: console.error };
let current: RabbitMQResilience | undefined;

beforeEach(() => {
    captured = [];
    for (const level of ["log", "warn", "error"] as const) {
        console[level] = (...args: unknown[]) => void captured.push({ level, line: String(args[0]) });
    }
});

afterEach(async () => {
    Object.assign(console, original);
    await current?.stop();
    current = undefined;
});

async function consume(handler: () => Promise<void>, overrides: Parameters<typeof startConsuming>[0] = {}) {
    const started = await startConsuming({
        logs: true,
        eventResilienceHandlerConfig: { immediateRetryAttempts: 0, delayedRetryAttempts: 3, delayInMs: 1000 },
        eventsToProcess: [eventProcess("OrderCreated", "h", handler)],
        ...overrides,
    });
    current = started.resilience;
    captured = []; // ignore the startup warnings
    return started;
}

/** Lines written while consuming, without the "published" lines of retries. */
const lines = () => captured.filter(({ line }) => !line.includes(" published "));

describe("message logs", () => {
    test("handled", async () => {
        const { channel } = await consume(async () => {});
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "OrderCreated" }));

        expect(lines()).toHaveLength(1);
        expect(lines()[0]!.level).toBe("log");
        expect(lines()[0]!.line).toMatch(new RegExp(`^${PREFIX}handled OrderCreated ${ID} ${DURATION}$`));
    });

    test("retry, with the attempt number and the reason", async () => {
        const { channel } = await consume(async () => {
            throw new Error("db down");
        });
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "OrderCreated" }));

        expect(lines()).toEqual([
            { level: "warn", line: expect.stringMatching(new RegExp(`^${PREFIX}retry 1/3 OrderCreated ${ID} ${DURATION}: Error: db down$`)) },
        ]);
    });

    test("rejected to the error queue, with the queue and the reason", async () => {
        const { channel } = await consume(async () => {
            throw new TypeError("volume not found");
        });
        await channel.deliver(
            EVENTS_QUEUE,
            buildConsumeMessage({ messageId: ID, type: "OrderCreated", headers: { redelivery_count: 3, retry_endpoint: "checkout-service" } }),
        );

        expect(lines()).toEqual([
            {
                level: "error",
                line: expect.stringMatching(new RegExp(
                    `^${PREFIX}rejected OrderCreated ${ID} ${DURATION} -> commerce.orders.checkout.events-error: TypeError: volume not found$`,
                )),
            },
        ]);
    });

    test("a body that is not valid JSON is retried, then rejected", async () => {
        const { channel } = await consume(async () => {});
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "OrderCreated", rawContent: "{not json" }));
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({
            messageId: ID,
            type: "OrderCreated",
            rawContent: "{not json",
            headers: { redelivery_count: 3, retry_endpoint: "checkout-service" },
        }));

        expect(lines()[0]!.line).toMatch(/ retry 1\/3 OrderCreated .*: invalid JSON body$/);
        expect(lines()[1]!.line).toMatch(/ rejected OrderCreated .* -> commerce\.orders\.checkout\.events-error: invalid JSON body$/);
    });

    test("discarded and skipped", async () => {
        const { channel, database } = await consume(async () => {});

        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "Unknown" }));
        database.selectResults.push([{ id: 1 }]);
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "OrderCreated" }));

        expect(lines().map(({ line }) => line.replace(/\(\d+ ms\)/, "(N ms)"))).toEqual([
            `[rbtmq-resilience] discarded Unknown ${ID} (N ms): no handler`,
            `[rbtmq-resilience] skipped OrderCreated ${ID} (N ms): already handled`,
        ]);
    });

    test("published", async () => {
        const { resilience } = await consume(async () => {});
        await resilience.publishEvent({ type: "OrderCreated", payload: {}, options: { messageId: ID } });

        expect(captured.map(({ line }) => line)).toEqual([
            expect.stringMatching(new RegExp(`^${PREFIX}published OrderCreated ${ID} ${DURATION} -> app.integration$`)),
        ]);
    });

    test("logs: false turns them off", async () => {
        const { channel, resilience } = await consume(async () => {}, { logs: false });
        await channel.deliver(EVENTS_QUEUE, buildConsumeMessage({ messageId: ID, type: "OrderCreated" }));
        await resilience.publishEvent({ type: "OrderCreated", payload: {} });

        expect(captured).toEqual([]);
    });
});

describe("formatDuration", () => {
    test("ms below one second, s with one decimal above", async () => {
        const { default: utils } = await import("@/shared/utils");
        expect(utils.formatDuration(12.4)).toBe("12 ms");
        expect(utils.formatDuration(999)).toBe("999 ms");
        expect(utils.formatDuration(1234)).toBe("1.2 s");
        expect(utils.formatDuration(65000)).toBe("65.0 s");
    });
});
