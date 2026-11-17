import { Channel, ConsumeMessage } from "amqplib";
import { AmqpConnection, AmqpPublisher } from "@/amqp";
import {
    ConsumerConfigDto,
    HandlerContextDto,
    HandlerFailureDto,
    HandlerMessageDto,
    InboxTransactionConfigDto,
    MessageHandlerDto,
    ParsedMessageDto,
    RbtmqResilienceRuntimeConfigDto,
} from "@/shared/dtos";
import utils from "@/shared/utils";
import { InboxRepository, inboxInsertParams, inboxInsertSql } from "@/features/consuming/database";
import { FailedMessageDto, MessageOutcomeDto } from "./consumeMessages.dto";
import { DeadLetter } from "./deadLetter";
import { InvalidMessageContentError, parseMessage } from "./parseMessage";

const PARSE_MESSAGE_HANDLER_NAME = "parseMessage";
/** runHandler result: another copy of the same message was handled first by this handler. */
const ALREADY_HANDLED = Symbol("alreadyHandled");

export class ConsumeMessagesUseCase {
    private readonly deadLetter: DeadLetter;

    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
        private readonly amqpConnection: AmqpConnection,
        private readonly inboxRepository: InboxRepository,
        publisher: AmqpPublisher,
    ) {
        this.deadLetter = new DeadLetter(config, publisher);
    }

    /** Registers every consumer on the current channel (on start, and again after a reconnection). */
    public async execute(): Promise<void> {
        for (const consumer of this.config.consumers ?? []) {
            await this.startConsumer(consumer);
        }
    }

    private async startConsumer(consumer: ConsumerConfigDto): Promise<void> {
        const channel = await this.amqpConnection.ensureChannel();
        await channel.prefetch(consumer.prefetch ?? this.config.retry.prefetch);
        await channel.consume(consumer.queue.primary.name, async (message) => {
            if (!message) return;
            await this.processMessage(consumer, channel, message);
        });
    }

    /**
     * Acks the message once it has been handled, retried, sent to the error queue or discarded.
     * A message every handler already processed (inbox) is rejected without requeue: the primary
     * queue has no dead-letter exchange, so the broker simply drops it.
     * If something breaks before that (e.g. the broker is down), it is requeued so it is not lost.
     * Ack, reject and nack always go to the channel that delivered the message: a delivery tag is
     * only valid there. If that channel is already closed, RabbitMQ redelivers the message itself.
     */
    private async processMessage(consumer: ConsumerConfigDto, channel: Channel, message: ConsumeMessage): Promise<void> {
        const startedAt = performance.now();

        let outcome: MessageOutcomeDto;
        try {
            outcome = await this.handleMessage(consumer, message);
        } catch (error) {
            // could not route it to retry/error (e.g. the broker is down): requeue so it is not lost; always logged
            this.settle(() => channel.nack(message, false, true));
            const reason = error instanceof Error ? error.message : String(error);
            console.error(this.logLine(message, startedAt, new MessageOutcomeDto("requeued", undefined, reason)));
            return;
        }

        const settled = this.settle(() => {
            if (outcome.status === "skipped") channel.reject(message, false);
            else channel.ack(message);
        });
        if (!settled) {
            const reason = "channel closed before the ack; RabbitMQ redelivers it";
            console.error(this.logLine(message, startedAt, new MessageOutcomeDto("requeued", undefined, reason)));
            return;
        }
        this.log(message, startedAt, outcome);
    }

    /** ack / reject / nack throw when the channel is closed; false means it could not be done. */
    private settle(action: () => void): boolean {
        try {
            action();
            return true;
        } catch {
            return false;
        }
    }

    private async handleMessage(consumer: ConsumerConfigDto, message: ConsumeMessage): Promise<MessageOutcomeDto> {
        const [invalidContent, parsed] = this.parse(message);
        if (invalidContent) return this.handleInvalidContent(consumer, message, invalidContent);
        if (!parsed) {
            return new MessageOutcomeDto("discarded", undefined, "missing id/type or another service's retry");
        }

        const handlers = consumer.handlers.filter((handler) => handler.messageType === parsed.type);
        if (handlers.length === 0) {
            return new MessageOutcomeDto("discarded", undefined, "no handler");
        }

        const { failures, handled } = await this.runHandlers(consumer, handlers, parsed);
        if (failures.length === 0) {
            return handled > 0 ? new MessageOutcomeDto("handled") : new MessageOutcomeDto("skipped", undefined, "already handled");
        }

        const reason = utils.describeFailure(failures[0]!);
        return this.routeFailure(new FailedMessageDto(consumer, message, parsed, failures, reason));
    }

    /**
     * Once the immediate retries are exhausted: a message redelivered fewer than N times goes to
     * the retry queue with redelivery_count + 1; otherwise it goes to the error queue as a poison
     * message. Every failure takes this path, whatever its cause.
     */
    private async routeFailure(failed: FailedMessageDto): Promise<MessageOutcomeDto> {
        const { consumer, message, parsed, failures, reason, errorPayload } = failed;

        if (parsed.redeliveryCount >= this.config.retry.delayedAttempts) {
            await this.deadLetter.sendToError(consumer, message, parsed, failures, errorPayload);
            return new MessageOutcomeDto("rejected", consumer.queue.error.name, reason);
        }

        await this.deadLetter.sendToRetry(consumer, message, parsed);
        const attempt = `${parsed.redeliveryCount + 1}/${this.config.retry.delayedAttempts}`;
        return new MessageOutcomeDto("retry", attempt, reason);
    }

    private log(message: ConsumeMessage, startedAt: number, outcome: MessageOutcomeDto): void {
        if (this.config.logs === false) return;
        const line = this.logLine(message, startedAt, outcome);
        if (outcome.status === "rejected") console.error(line);
        else if (outcome.status === "retry") console.warn(line);
        else console.log(line);
    }

    /** e.g. `[rbtmq-resilience] retry 1/3 OrderCreated 3fa85f64-... (35 ms): Error: db down` */
    private logLine(message: ConsumeMessage, startedAt: number, outcome: MessageOutcomeDto): string {
        const type = message.properties.type ?? "-";
        const id = message.properties.messageId ?? "-";
        const duration = utils.formatDuration(performance.now() - startedAt);

        const status = outcome.status === "retry" ? `retry ${outcome.target}` : outcome.status;
        let line = `[rbtmq-resilience] ${status} ${type} ${id} (${duration})`;
        if (outcome.status === "rejected" && outcome.target) line += ` -> ${outcome.target}`;
        if (outcome.reason) line += `: ${outcome.reason}`;
        return line;
    }

    /** Same idea as the DTOs' create(): returns [error] or [undefined, parsed]. */
    private parse(message: ConsumeMessage): [InvalidMessageContentError?, ParsedMessageDto?] {
        try {
            return [undefined, parseMessage(message, this.config.serviceName)];
        } catch (error) {
            if (error instanceof InvalidMessageContentError) return [error];
            throw error;
        }
    }

    /** Runs every handler that has not processed this message yet. */
    private async runHandlers(
        consumer: ConsumerConfigDto,
        handlers: MessageHandlerDto[],
        parsed: ParsedMessageDto,
    ): Promise<{ failures: HandlerFailureDto[]; handled: number }> {
        const failures: HandlerFailureDto[] = [];
        let handled = 0;

        for (const handler of handlers) {
            const alreadyProcessed = await this.inboxRepository.hasProcessed(parsed.messageId, handler.handlerName);
            if (alreadyProcessed) continue;

            const error = await this.runHandler(consumer, handler, parsed);
            // a concurrent copy committed first: this handler counts as already processed
            if (error === ALREADY_HANDLED) continue;
            if (error) {
                failures.push(utils.toHandlerFailure(handler.handlerName, error));
                continue;
            }

            handled += 1;
        }

        return { failures, handled };
    }

    /**
     * Runs one handler with immediate retries, each attempt in its own transaction together with
     * the inbox record. Returns the last error, undefined on success, or ALREADY_HANDLED when an
     * attempt failed because another copy of the message committed the inbox row first (the
     * duplicate key on the inbox): its writes were rolled back and it is not retried.
     */
    private async runHandler(
        consumer: ConsumerConfigDto,
        handler: MessageHandlerDto,
        parsed: ParsedMessageDto,
    ): Promise<unknown> {
        const retries = this.config.retry.immediateRetries;
        // required whenever there are consumers (checked when the library is created)
        const inboxTransaction = this.config.inboxTransaction!;

        // validating the payload is part of running the handler, so a validation error is retried
        // like any other handler error
        const attempt = async () => {
            const payload = await handler.validate(parsed.payload);
            const handlerMessage = new HandlerMessageDto(
                parsed.messageId,
                parsed.type,
                payload,
                parsed.headers,
                parsed.routingKey,
                parsed.redeliveryCount,
            );
            await this.handleInTransaction(inboxTransaction, consumer, handler, handlerMessage, parsed);
        };

        return runWithImmediateRetries(
            retries,
            attempt,
            () => this.inboxRepository.hasProcessed(parsed.messageId, handler.handlerName),
        );
    }

    /**
     * One attempt = one transaction: the handler's writes and the inbox insert commit together,
     * or both roll back (handler error, inbox insert error, duplicate delivery).
     */
    private async handleInTransaction(
        inboxTransaction: InboxTransactionConfigDto,
        consumer: ConsumerConfigDto,
        handler: MessageHandlerDto,
        handlerMessage: HandlerMessageDto,
        parsed: ParsedMessageDto,
    ): Promise<void> {
        await inboxTransaction.run(async (transaction) => {
            const context = new HandlerContextDto(this.config.serviceName, consumer.name, transaction);
            await handler.handle(handlerMessage, context);
            await inboxTransaction.execute(
                transaction,
                inboxInsertSql,
                inboxInsertParams(parsed.messageId, handler.handlerName, parsed.payload),
            );
        });
    }

    /**
     * A body that is not valid JSON. If this consumer does not handle its type it is discarded as
     * usual. Otherwise it is treated like any failed message: the handler cannot run, so
     * every immediate retry would fail the same way, and it goes to the retry queue (with its raw
     * body) until it was redelivered N times, then to the error queue with its raw content.
     */
    private async handleInvalidContent(
        consumer: ConsumerConfigDto,
        message: ConsumeMessage,
        error: InvalidMessageContentError,
    ): Promise<MessageOutcomeDto> {
        const handled = consumer.handlers.some((handler) => handler.messageType === error.parsed.type);
        if (!handled) {
            return new MessageOutcomeDto("discarded", undefined, "no handler");
        }

        const failures = [utils.toHandlerFailure(PARSE_MESSAGE_HANDLER_NAME, error.parseError)];
        return this.routeFailure(
            new FailedMessageDto(consumer, message, error.parsed, failures, "invalid JSON body", error.rawContent),
        );
    }
}

/**
 * Runs the action once, plus up to `retries` immediate retries if it keeps failing.
 * Returns the last error, or undefined on success.
 * After a failure, `alreadyHandled` checks the inbox: if another copy got there first, it stops
 * and returns ALREADY_HANDLED (no driver-specific duplicate-key detection needed).
 * Values below 0 mean "no immediate retries", never "skip the handler".
 */
async function runWithImmediateRetries(
    retries: number,
    action: () => Promise<void>,
    alreadyHandled: () => Promise<boolean>,
): Promise<unknown | undefined> {
    const totalAttempts = 1 + Math.max(0, retries);
    let lastError: unknown;

    for (let attempt = 0; attempt < totalAttempts; attempt += 1) {
        try {
            await action();
            return undefined;
        } catch (error) {
            lastError = error;
            if (await alreadyHandled().catch(() => false)) return ALREADY_HANDLED;
        }
    }

    return lastError;
}
