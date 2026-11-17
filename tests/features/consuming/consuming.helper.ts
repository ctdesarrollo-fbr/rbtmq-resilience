import { RabbitMQResilience, type RbtmqResilienceConfigDto } from "@/index";
import { fakeAmqp, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { createFakeDatabase } from "@tests/helpers/fakeDatabase";
import { createFakeTransactions } from "@tests/helpers/fakeTransactions";
import { buildServiceConfig, withDb } from "@tests/helpers/config";

export const EVENTS_QUEUE = "commerce.orders.checkout.events";

/** `transactions` lists every transaction the library opened (inbox in the same Tx as the handler). */
export async function startConsuming(overrides: Partial<RbtmqResilienceConfigDto> = {}) {
    const channel: FakeChannel = fakeAmqp.reset();
    const database = createFakeDatabase();
    const fake = createFakeTransactions();
    const resilience = new RabbitMQResilience(
        withDb(buildServiceConfig({ inboxTransaction: fake.config, ...overrides }), database.db),
    );
    await resilience.start();
    return { resilience, channel, database, transactions: fake.transactions };
}

export const INBOX_INSERT = "INSERT INTO inbox (message_id, handler_name, payload) VALUES (?, ?, ?)";

export function decodePublish(args: unknown[]) {
    const [exchange, routingKey, body, options] = args as [string, string, Buffer, Record<string, unknown>];
    return { exchange, routingKey, payload: JSON.parse(body.toString("utf8")), options };
}
