import { JsonObject } from "@/shared/primitives";

export class HandlerMessageDto<TPayload = unknown> {
    constructor(
        public messageId: string,
        public type: string,
        public payload: TPayload,
        public headers: JsonObject,
        public routingKey: string,
        public redeliveryCount: number,
    ) {}
}

export class HandlerContextDto {
    constructor(
        public serviceName: string,
        public consumerName: string,
        /**
         * The service's own transaction (e.g. a Sequelize Transaction) when `inboxTransaction`
         * is configured. Use it for the handler's writes so they commit together with the inbox.
         */
        public transaction?: unknown,
    ) {}
}

export class MessageHandlerDto<TPayload = unknown> {
    constructor(
        public messageType: string,
        public handlerName: string,
        public validate: (payload: unknown) => TPayload | Promise<TPayload>,
        public handle: (message: HandlerMessageDto<TPayload>, context: HandlerContextDto) => Promise<void>,
    ) {}
}
