import { ConsumeMessage } from "amqplib";
import constant from "@/shared/constant";
import { ParsedMessageDto } from "@/shared/dtos";
import { JsonObject } from "@/shared/primitives";
import utils from "@/shared/utils";

/**
 * The message has a valid envelope (id, type, headers) but its body is not valid JSON.
 * It is a poison message: retrying will never fix it.
 */
export class InvalidMessageContentError extends Error {
    constructor(
        public readonly parsed: ParsedMessageDto,
        public readonly rawContent: string,
        public readonly parseError: unknown,
    ) {
        super("Message content is not valid JSON.");
        this.name = "InvalidMessageContentError";
    }
}

/**
 * Returns undefined when the message must be discarded.
 * Throws InvalidMessageContentError when the body is not valid JSON.
 */
export function parseMessage(message: ConsumeMessage, serviceName: string): ParsedMessageDto | undefined {
    const headers = utils.normalizeHeaders(message.properties.headers);
    const messageId =
        message.properties.messageId ??
        utils.stringHeader(headers[constant.RABBITMQ_HEADERS.MESSAGE_ID]);
    const type =
        message.properties.type ??
        utils.stringHeader(headers[constant.RABBITMQ_HEADERS.TYPE]);

    if (!messageId || !type) return undefined;

    const retryEndpoint = utils.stringHeader(
        headers[constant.RABBITMQ_HEADERS.RETRY_ENDPOINT],
    );
    const legacyRedeliveryCount = utils.numberHeader(
        headers[constant.RABBITMQ_HEADERS.REDELIVERY_COUNT],
    );
    const redeliveryCount = legacyRedeliveryCount || utils.xDeathCount(headers);
    if (
        legacyRedeliveryCount > 0 &&
        (retryEndpoint ?? message.properties.appId) !== serviceName
    ) {
        return undefined;
    }

    const rawContent = message.content.toString("utf8");
    let payload: JsonObject;
    try {
        payload = JSON.parse(rawContent) as JsonObject;
    } catch (error) {
        throw new InvalidMessageContentError(
            new ParsedMessageDto(messageId, type, {}, headers, message.fields.routingKey, redeliveryCount),
            rawContent,
            error,
        );
    }

    return new ParsedMessageDto(
        messageId,
        type,
        payload,
        headers,
        message.fields.routingKey,
        redeliveryCount,
    );
}
