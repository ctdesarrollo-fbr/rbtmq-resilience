export type JsonObject = Record<string, unknown>;
export type ExchangeKind = "fanout" | "topic" | "direct";
export type ConsumerKind = "events" | "tacticalEvents" | "commands";
export type TacticalBindingLevel = "domain" | "subdomain" | "boundedContext";
