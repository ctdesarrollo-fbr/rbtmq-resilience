import { AmqpConnection } from "@/amqp";
import { RbtmqResilienceRuntimeConfigDto, ResilientQueueConfigDto } from "@/shared/dtos";
import { ExchangeKind } from "@/shared/primitives";

export class DeclareTopologyUseCase {
    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        private readonly amqpConnection: AmqpConnection,
    ) {}

    public async execute(): Promise<void> {
        // only the exchanges the service uses; the direct one always (retry and error queues)
        const { integration, tactical, direct } = this.config.exchanges;
        if (integration) await this.assertExchange(integration.name, integration.type ?? "fanout");
        if (tactical) await this.assertExchange(tactical.name, tactical.type ?? "topic");
        await this.assertExchange(direct.name, direct.type ?? "direct");

        for (const consumer of this.config.consumers ?? []) {
            await this.assertResilientQueue(consumer.queue);
        }

        const channel = await this.amqpConnection.ensureChannel();
        for (const binding of this.config.bindings ?? []) {
            await channel.bindQueue(binding.queue, binding.exchange, binding.routingKey ?? "");
        }
    }

    private async assertExchange(name: string, type: ExchangeKind): Promise<void> {
        const channel = await this.amqpConnection.ensureChannel();
        try {
            await channel.assertExchange(name, type, { durable: true });
        } catch (error) {
            throw new Error(
                `RabbitMQ exchange "${name}" could not be declared as "${type}". ` +
                    "If it already exists in the broker with another type, RabbitMQ will " +
                    "reject the declaration. Use a different exchange name, match the " +
                    "existing exchange type, or delete/recreate the exchange in RabbitMQ.",
                { cause: error },
            );
        }
    }

    private async assertResilientQueue(queue: ResilientQueueConfigDto): Promise<void> {
        const channel = await this.amqpConnection.ensureChannel();
        const directExchange = this.config.exchanges.direct.name;

        await channel.assertQueue(queue.error.name, {
            durable: queue.error.durable ?? true,
        });
        // only the retry queue has x-dead-letter-* arguments (it sends the message back here when
        // its TTL expires); the primary queue has none, so a rejected message is simply dropped
        await channel.assertQueue(queue.primary.name, {
            durable: queue.primary.durable ?? true,
        });
        await channel.assertQueue(queue.retry.name, {
            durable: queue.retry.durable ?? true,
            messageTtl: queue.retry.messageTtlMs ?? this.config.retry.delayMs,
            deadLetterExchange: directExchange,
            deadLetterRoutingKey: queue.primary.routingKey,
        });

        await channel.bindQueue(queue.primary.name, directExchange, queue.primary.routingKey);
        await channel.bindQueue(queue.retry.name, directExchange, queue.retry.routingKey);
        await channel.bindQueue(queue.error.name, directExchange, queue.error.routingKey);
    }
}
