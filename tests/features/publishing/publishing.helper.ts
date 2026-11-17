import { RabbitMQResilience } from "@/index";
import { fakeAmqp, type FakeChannel } from "@tests/helpers/fakeAmqp";
import { buildServiceConfig } from "@tests/helpers/config";

export function setupPublishing(): { resilience: RabbitMQResilience; channel: FakeChannel } {
    const channel = fakeAmqp.reset();
    const resilience = new RabbitMQResilience(
        buildServiceConfig({ tacticalExchange: "app.tactical" }),
    );
    return { resilience, channel };
}

/** Decodes the arguments of a `channel.publish` call. */
export function decodePublish(args: unknown[]) {
    const [exchange, routingKey, body, options] = args as [string, string, Buffer, Record<string, unknown>];
    return { exchange, routingKey, payload: JSON.parse(body.toString("utf8")), options };
}
