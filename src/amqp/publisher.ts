import constant from "@/shared/constant";
import utils from "@/shared/utils";
import { RbtmqResilienceRuntimeConfigDto } from "@/shared/dtos";
import { AmqpConnection } from "./connection";
import { PublishMessageDto } from "./publisher.dto";

/** A mandatory message no queue received: retrying it cannot help until the topology changes. */
export class UnroutableMessageError extends Error {
    constructor(exchange: string, routingKey: string) {
        super(`no queue is bound to exchange "${exchange}" with routing key "${routingKey}"; the message was not delivered.`);
        this.name = "UnroutableMessageError";
    }
}

/**
 * Low-level publication shared by publishing and consuming (retry/error queues).
 * Every publication waits for the broker's confirmation; a nack, a closed channel or (with
 * `mandatory`) a message no queue received makes it throw, so the caller never treats it as sent.
 */
export class AmqpPublisher {
    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        private readonly amqpConnection: AmqpConnection,
    ) {}

    /**
     * Normal publication (events, tactical events, commands): always headerless, without expiration or timestamp.
     * `mandatory` (commands): fail if no queue is bound to the routing key, instead of losing the message.
     * Logs e.g. `[rbtmq-resilience] published OrderCreated 3fa85f64-... (3 ms) -> sagittarius-a`.
     */
    public async publishRaw(message: PublishMessageDto, mandatory = false): Promise<string> {
        const startedAt = performance.now();
        const messageWithoutHeaders = new PublishMessageDto(
            message.exchange,
            message.routingKey,
            message.type,
            message.payload,
            message.messageId,
        );
        const target = `${message.exchange}${message.routingKey ? ` (${message.routingKey})` : ""}`;

        try {
            const messageId = await this.publishMessage(messageWithoutHeaders, false, mandatory);
            if (this.config.logs !== false) {
                const duration = utils.formatDuration(performance.now() - startedAt);
                console.log(`[rbtmq-resilience] published ${message.type} ${messageId} (${duration}) -> ${target}`);
            }
            return messageId;
        } catch (error) {
            // a failed publication is always logged
            const duration = utils.formatDuration(performance.now() - startedAt);
            const reason = error instanceof Error ? error.message : String(error);
            console.error(`[rbtmq-resilience] publish failed ${message.type} ${message.messageId} (${duration}) -> ${target}: ${reason}`);
            throw error;
        }
    }

    /**
     * Internal retry/DLQ messages keep resilience diagnostics in headers. Always `mandatory`:
     * the retry and error queues are ours, so a message nobody received is a lost message.
     */
    public async publishDiagnosticMessage(message: PublishMessageDto): Promise<string> {
        return this.publishMessage(message, true, true);
    }

    private async publishMessage(message: PublishMessageDto, includeTimestamp: boolean, mandatory: boolean): Promise<string> {
        const { exchange, routingKey, type, payload, messageId, headers, expiration } = message;
        const channel = await this.amqpConnection.ensureChannel();
        // a Buffer is sent byte for byte (a retry keeps the body it was delivered with)
        const body = Buffer.isBuffer(payload) ? payload : Buffer.from(JSON.stringify(payload));

        const options: Record<string, unknown> = {
            type,
            messageId,
            appId: this.config.serviceName,
            contentType: constant.CONTENT_TYPE.JSON,
            // stored on disk (delivery mode 2): durable queues keep their messages across a broker restart
            persistent: true,
        };
        if (includeTimestamp) options.timestamp = Math.floor(Date.now() / 1000);
        if (headers) options.headers = headers;
        if (expiration) options.expiration = expiration;
        if (mandatory) options.mandatory = true;

        // publisher confirm: resolves when the broker acks the message, rejects on nack or closed channel
        await new Promise<void>((resolve, reject) => {
            try {
                channel.publish(exchange, routingKey, body, options, (error) => (error ? reject(error) : resolve()));
            } catch (error) {
                reject(error);
            }
        });

        // the broker sends basic.return before basic.ack, so by now we know if it was unroutable
        if (mandatory && this.amqpConnection.wasReturned(messageId)) {
            throw new UnroutableMessageError(exchange, routingKey);
        }

        return messageId;
    }
}
