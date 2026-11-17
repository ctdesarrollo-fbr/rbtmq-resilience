/** Where a publication goes. `mandatory` (commands): fail if no queue is bound to the routing key. */
export class PublishTargetDto {
    constructor(
        public exchange: string,
        public routingKey: string,
        public mandatory: boolean = false,
    ) {}
}
