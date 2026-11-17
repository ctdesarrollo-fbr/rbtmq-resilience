import { ConsumeMessage } from "amqplib";
import { AmqpPublisher, PublishMessageDto } from "@/amqp";
import constant from "@/shared/constant";
import {
    ConsumerConfigDto,
    ExceptionDetailsDto,
    HandlerFailureDto,
    ParsedMessageDto,
    RbtmqResilienceRuntimeConfigDto,
} from "@/shared/dtos";
import utils from "@/shared/utils";

const HEADERS = constant.RABBITMQ_HEADERS;

/** Sends failed messages to the consumer's retry queue or error queue. */
export class DeadLetter {
    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        private readonly publisher: AmqpPublisher,
    ) {}

    /**
     * Delayed retry: the message comes back to the primary queue after the retry TTL.
     * The body is re-sent byte for byte, so even a body that is not valid JSON survives every retry.
     */
    async sendToRetry(consumer: ConsumerConfigDto, original: ConsumeMessage, parsed: ParsedMessageDto): Promise<void> {
        const redeliveryCount = parsed.redeliveryCount + 1;
        const sender = this.originalSender(original, parsed);

        const headers: Record<string, unknown> = {
            ...parsed.headers,
            [HEADERS.REDELIVERY_COUNT]: redeliveryCount,
            [HEADERS.RETRY_ENDPOINT]: this.config.serviceName,
        };
        if (sender) headers[HEADERS.SENDER] = sender;

        await this.publisher.publishDiagnosticMessage(new PublishMessageDto(
            this.config.exchanges.direct.name,
            consumer.queue.retry.routingKey,
            parsed.type,
            original.content,
            parsed.messageId,
            headers,
            this.retryExpiration(redeliveryCount),
        ));
    }

    /**
     * Error queue, with the failure details in the headers.
     * `payload` defaults to the parsed body; messages with invalid JSON pass their raw content.
     */
    async sendToError(
        consumer: ConsumerConfigDto,
        original: ConsumeMessage,
        parsed: ParsedMessageDto,
        failures: HandlerFailureDto[],
        payload: unknown = parsed.payload,
    ): Promise<void> {
        const sender = this.originalSender(original, parsed);

        // Replay target for Failed Messages Monitoring: always this consumer's primary queue through
        // the direct exchange, never the fanout/topic exchange it first came from (that would
        // redeliver it to every subscriber). Same values a delayed retry comes back with.
        const deliveryMetadata: Record<string, unknown> = {
            exchange: this.config.exchanges.direct.name,
            routing_key: consumer.queue.primary.routingKey,
            message_type: parsed.type,
        };
        if (sender) deliveryMetadata.sender = sender;

        const headers = {
            ...parsed.headers,
            // a poison message carries one exception: Failed Messages Monitoring stores only the first failure
            [HEADERS.EXCEPTION_DETAILS]: ExceptionDetailsDto.fromFailure(failures[0]!),
            [HEADERS.ENDPOINT]: {
                name: this.config.serviceName,
                delivery_metadata: deliveryMetadata,
            },
        };

        await this.publisher.publishDiagnosticMessage(new PublishMessageDto(
            this.config.exchanges.direct.name,
            consumer.queue.error.routingKey,
            parsed.type,
            { payload },
            parsed.messageId,
            headers,
        ));
    }

    /**
     * The service that first published the message. Retries are re-published by this
     * service (so their appId changes), which is why the original sender travels in a header.
     */
    private originalSender(original: ConsumeMessage, parsed: ParsedMessageDto): string | undefined {
        return utils.stringHeader(parsed.headers[HEADERS.SENDER]) ?? original.properties.appId;
    }

    /** Exponential backoff: expiration = redelivery_count * backoffMs. 0 means queue TTL only. */
    private retryExpiration(redeliveryCount: number): string | undefined {
        const backoffMs = this.config.retry.backoffMs ?? constant.DEFAULTS.RETRY_BACKOFF_MS;
        if (backoffMs <= 0) return undefined;
        return String(redeliveryCount * backoffMs);
    }
}
