import { afterEach, describe, expect, test } from "bun:test";
import { getTableName } from "drizzle-orm";
import { RabbitMQResilience, inbox, mysqlMigration, outbox } from "@/index";
import { fakeAmqp } from "@tests/helpers/fakeAmqp";
import { createFakeDatabase } from "@tests/helpers/fakeDatabase";
import { buildServiceConfig, withDb } from "@tests/helpers/config";

let current: RabbitMQResilience | undefined;
afterEach(async () => {
    await current?.stop();
    current = undefined;
});

describe("database", () => {
    test("the migration SQL is unchanged", () => {
        expect(mysqlMigration).toMatchSnapshot();
    });

    test("exposes the inbox and outbox tables", () => {
        expect(getTableName(inbox)).toBe("inbox");
        expect(getTableName(outbox)).toBe("outbox");
        expect(Object.keys(inbox)).toEqual(expect.arrayContaining(["id", "messageId", "handlerName", "payload", "processedAt"]));
        expect(Object.keys(outbox)).toEqual(
            expect.arrayContaining([
                "id", "messageId", "type", "headers", "body", "exchange", "routingKey",
                "status", "attempts", "error", "sentAt", "failedAt", "createdAt",
            ]),
        );
    });

    test("runs every migration statement against a provided db on start", async () => {
        fakeAmqp.reset();
        const database = createFakeDatabase();
        current = new RabbitMQResilience(withDb(buildServiceConfig(), database.db));

        await current.start();

        expect(database.executed).toHaveLength(2);
        expect(database.executed[0]).toStartWith("CREATE TABLE IF NOT EXISTS inbox");
        expect(database.executed[1]).toStartWith("CREATE TABLE IF NOT EXISTS outbox");
    });

    test("requires mysql or db config", async () => {
        fakeAmqp.reset();
        const config = buildServiceConfig();
        const resilience = new RabbitMQResilience({ ...config, mysql: undefined } as never);

        await expect(resilience.start()).rejects.toThrow("rbtmq-resilience requires config.mysql or config.db.");
        expect(await resilience.isReady()).toBe(false);
    });
});
