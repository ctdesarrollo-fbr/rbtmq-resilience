import { JsonObject } from "@/shared/primitives";

export class PublishOptionsDto {
    constructor(
        public messageId?: string,
        public headers?: JsonObject,
    ) {}
}

export class PublishEnvelopeDto {
    constructor(
        public type: string,
        public payload: unknown,
        public routingKey?: string,
        public options?: PublishOptionsDto,
    ) {}

    static create(object: { [key: string]: any }): [string[]?, PublishEnvelopeDto?] {
        const { type, payload, routingKey, options } = object;
        const errors: string[] = [];

        if (!type) errors.push("Type is required");
        else if (typeof type !== "string") errors.push("Type must be a string");
        if (payload === undefined) errors.push("Payload is required");
        if (routingKey !== undefined && typeof routingKey !== "string") errors.push("Routing key must be a string");

        if (errors.length > 0) return [errors];

        return [undefined, new PublishEnvelopeDto(
            type,
            payload,
            routingKey,
            options,
        )];
    }
}
