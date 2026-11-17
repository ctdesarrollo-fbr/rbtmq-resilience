export class QueueNamePartsDto {
    constructor(
        public domain: string,
        public subdomain?: string,
        public boundedContext?: string,
        public messageType?: string,
    ) {}
}

/**
 * Queue names and routing keys of one consumer.
 * The field names match RbtmqConsumerConfigDto, so they can be spread into a consumer config.
 */
export class ConsumerQueueNamesDto {
    constructor(
        /** Primary queue. */
        public queue: string,
        /** Key the primary queue is bound with on the direct exchange; the retry queue dead-letters back with it. */
        public routingKey: string,
        /** Retry queue (delay capabilities). */
        public retryQueue: string,
        /** Key used to send a failed message to the retry queue. */
        public retryRoutingKey: string,
        /** Error queue shared by the whole domain, consumed by Failed Messages Monitoring. */
        public deadLetterQueue: string,
        public deadLetterRoutingKey: string,
        /** Commands only: key producers use to send commands to this queue on the direct exchange. */
        public bindingRoutingKey?: string,
    ) {}
}
