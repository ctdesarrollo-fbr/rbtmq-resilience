import { AmqpPublisher, PublishMessageDto } from "@/amqp";
import { PublishedMessageDto, PublishHistoryRecorder, RabbitMQMessageDto, RbtmqResilienceRuntimeConfigDto } from "@/shared/dtos";
import { JsonObject } from "@/shared/primitives";
import utils from "@/shared/utils";
import { PublishEnvelopeDto } from "./publishing.dto";
import { PublishRetryPolicyDto } from "./publishRetry.dto";
import { PublishRetry } from "./publishRetry";
import { PublishTargetDto } from "./publishTarget.dto";

/** Integration event, to the integration (fanout) exchange. */
export class PublishEventUseCase {
    private readonly publishToExchange: PublishToExchange;

    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        publisher: AmqpPublisher,
        history: PublishHistoryRecorder,
    ) {
        this.publishToExchange = new PublishToExchange(config, publisher, history);
    }

    public async execute(envelope: PublishEnvelopeDto | RabbitMQMessageDto): Promise<string> {
        const integration = this.config.exchanges.integration;
        if (!integration) throw new Error("publishEvent requires the integration exchange (config.exchange).");
        const exchange = integration.name;

        if (envelope instanceof RabbitMQMessageDto) {
            const routingKey = envelope.fields.routingKey;
            return this.publishToExchange.execute(new PublishTargetDto(exchange, routingKey), {
                type: String(envelope.properties.type),
                payload: JSON.parse(envelope.content.toString("utf8")) as JsonObject,
                routingKey,
                options: {
                    messageId: envelope.properties.messageId,
                    headers: envelope.properties.headers,
                },
            });
        }

        return this.publishToExchange.execute(new PublishTargetDto(exchange, envelope.routingKey ?? ""), envelope);
    }
}

/** Tactical (private) event, to the topic exchange with routing key domain.{subdomain|boundedContext}.messageType. */
export class PublishTacticalEventUseCase {
    private readonly publishToExchange: PublishToExchange;

    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        publisher: AmqpPublisher,
        history: PublishHistoryRecorder,
    ) {
        this.publishToExchange = new PublishToExchange(config, publisher, history);
    }

    public async execute(envelope: PublishEnvelopeDto): Promise<string> {
        if (!envelope.routingKey) throw new Error("publishTacticalEvent requires a routingKey.");
        if (!utils.isTacticalRoutingKey(envelope.routingKey)) {
            throw new Error(
                `publishTacticalEvent requires a routing key like domain.{subdomain|boundedContext}.messageType (got "${envelope.routingKey}").`,
            );
        }
        const tactical = this.config.exchanges.tactical;
        if (!tactical) throw new Error("publishTacticalEvent requires the topic exchange (config.tacticalExchange).");

        return this.publishToExchange.execute(new PublishTargetDto(tactical.name, envelope.routingKey), envelope);
    }
}

/** Command, to the direct exchange. Mandatory: fails if the target service's queue does not exist. */
export class SendCommandUseCase {
    private readonly publishToExchange: PublishToExchange;

    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        publisher: AmqpPublisher,
        history: PublishHistoryRecorder,
    ) {
        this.publishToExchange = new PublishToExchange(config, publisher, history);
    }

    public async execute(envelope: PublishEnvelopeDto): Promise<string> {
        if (!envelope.routingKey) throw new Error("sendCommand requires a routingKey.");

        const target = new PublishTargetDto(this.config.exchanges.direct.name, envelope.routingKey, true);
        return this.publishToExchange.execute(target, envelope);
    }
}

/**
 * Shared by the three use cases: validates the envelope, publishes it directly to the exchange
 * and records the result in the outbox history.
 * If the broker does not confirm it (nack, closed channel, lost connection) it is retried with
 * backoff (config.publishRetry), always with the same message id so consumers deduplicate it in
 * their inbox. An invalid envelope or an unroutable command is not retried. Once the attempts run
 * out it is recorded as failed and the error is thrown to the caller.
 */
class PublishToExchange {
    private readonly publishRetry: PublishRetry;

    constructor(
        config: RbtmqResilienceRuntimeConfigDto,
        publisher: AmqpPublisher,
        private readonly history: PublishHistoryRecorder,
    ) {
        this.publishRetry = new PublishRetry(publisher, PublishRetryPolicyDto.create(config.publishRetry));
    }

    public async execute(target: PublishTargetDto, envelope: PublishEnvelopeDto): Promise<string> {
        const [errors, envelopeDto] = PublishEnvelopeDto.create(envelope);
        if (errors || !envelopeDto) throw new Error(`Invalid message: ${errors?.join(", ")}`);

        const message = new PublishMessageDto(
            target.exchange,
            target.routingKey,
            envelopeDto.type,
            envelopeDto.payload,
            envelopeDto.options?.messageId,
        );
        const published = new PublishedMessageDto(
            message.messageId,
            message.type,
            message.payload,
            target.exchange,
            target.routingKey,
            envelopeDto.options?.headers,
        );

        try {
            await this.publishRetry.publish(message, target.mandatory);
        } catch (error) {
            const reason = error instanceof Error ? error.message : String(error);
            await this.record(() => this.history.recordFailed(published, reason), published);
            throw error;
        }

        await this.record(() => this.history.recordSent(published), published);
        return message.messageId;
    }

    /** The history never breaks a publication: if it cannot be written, it is only logged. */
    private async record(write: () => Promise<void>, message: PublishedMessageDto): Promise<void> {
        try {
            await write();
        } catch (error) {
            const reason = error instanceof Error ? error.message : String(error);
            console.error(`[rbtmq-resilience] could not record ${message.type} ${message.messageId} in the outbox history: ${reason}`);
        }
    }
}
