import { JsonObject } from "@/shared/primitives";

export class PublishMessageDto {
    constructor(
        public exchange: string,
        public routingKey: string,
        public type: string,
        public payload: unknown,
        public messageId: string = crypto.randomUUID(),
        public headers?: JsonObject,
        public expiration?: string,
    ) {}
}
