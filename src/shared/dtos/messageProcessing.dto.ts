import { JsonObject } from "@/shared/primitives";

export class ParsedMessageDto {
    constructor(
        public messageId: string,
        public type: string,
        public payload: JsonObject,
        public headers: JsonObject,
        public routingKey: string,
        public redeliveryCount: number,
    ) {}
}

/**
 * `exception_details` header of a poison message, the shape Failed Messages Monitoring reads:
 * { "exception_type": "MailServerUnavailableError",
 *   "stack_trace": "MailServerUnavailableError: Mail Server is not available.\n    at ...",
 *   "failed_at": "2024-05-25T23:14:06.982Z" }
 */
export class ExceptionDetailsDto {
    constructor(
        /** The error class name (error.name), e.g. "TypeError". The message is in the stack trace's first line. */
        public exception_type: string,
        public stack_trace: string,
        public failed_at: string,
    ) {}

    static fromFailure(failure: HandlerFailureDto): ExceptionDetailsDto {
        return new ExceptionDetailsDto(
            failure.exception_type,
            failure.stack_trace ?? "",
            failure.failed_at,
        );
    }
}

/** One failed handler run (internal: logs and the error queue's exception_details). */
export class HandlerFailureDto {
    constructor(
        public handler_name: string,
        public exception_type: string,
        public message: string,
        public failed_at: string,
        public stack_trace?: string,
    ) {}
}

/** A message the service published, as it is stored in the outbox history. */
export class PublishedMessageDto {
    constructor(
        public messageId: string,
        public type: string,
        public body: unknown,
        public exchange: string,
        public routingKey: string,
        public headers?: JsonObject,
    ) {}
}

/** Where publishing records each message it sent or failed to send (the outbox table). */
export interface PublishHistoryRecorder {
    recordSent(message: PublishedMessageDto): Promise<void>;
    recordFailed(message: PublishedMessageDto, error: string): Promise<void>;
}
