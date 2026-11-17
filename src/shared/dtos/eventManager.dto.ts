export class MessageFieldsDto {
    constructor(
        public consumerTag: string | number,
        public deliveryTag: number | boolean,
        public exchange: string,
        public routingKey: string,
        public redelivered = false,
    ) {}
}

export class MessagePropertiesDto {
    constructor(
        public contentType?: string,
        public contentEncoding?: string,
        public headers?: Record<string, unknown>,
        public deliveryMode?: number,
        public priority?: number,
        public correlationId?: string,
        public replyTo?: string,
        public expiration?: string,
        public timestamp?: number | string,
        public messageId?: string,
        public type?: string,
        public userId?: string,
        public appId?: string,
        public clusterId?: string,
    ) {}
}

export class RabbitMQMessageDto {
    constructor(
        public content: Buffer,
        public fields: MessageFieldsDto,
        public properties: MessagePropertiesDto,
    ) {}
}
