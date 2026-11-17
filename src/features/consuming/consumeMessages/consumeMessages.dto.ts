import { ConsumeMessage } from "amqplib";
import { ConsumerConfigDto, HandlerFailureDto, ParsedMessageDto } from "@/shared/dtos";

/**
 * What happened to a consumed message, for the one-line log:
 * handled, skipped (already in the inbox: rejected without requeue), discarded (not for this consumer),
 * retry (sent to the retry queue), rejected (sent to the error queue) or
 * requeued (it could not be acknowledged, e.g. the broker is down).
 */
export class MessageOutcomeDto {
    constructor(
        public status: "handled" | "skipped" | "discarded" | "retry" | "rejected" | "requeued",
        /** Queue the message was sent to (retry or error queue). */
        public target?: string,
        public reason?: string,
    ) {}
}

/**
 * A message whose handlers failed after the immediate retries, on its way to the retry or error queue.
 * `errorPayload`: what the error queue receives instead of the parsed body (the raw content of an invalid JSON).
 */
export class FailedMessageDto {
    constructor(
        public consumer: ConsumerConfigDto,
        public message: ConsumeMessage,
        public parsed: ParsedMessageDto,
        public failures: HandlerFailureDto[],
        public reason: string,
        public errorPayload?: unknown,
    ) {}
}
