import type { ResilienceDatabase } from "@/index";

export type DbOperation = { op: string; args: unknown[] };

/**
 * Minimal stand-in for the drizzle query builder used by the inbox/outbox
 * repositories. Every builder step is recorded; `selectResults` feeds the
 * rows returned by the next select queries, in order.
 */
export function createFakeDatabase() {
    const operations: DbOperation[] = [];
    const selectResults: unknown[][] = [];
    const executed: string[] = [];

    const chain = (terminal: () => unknown) => {
        const builder: Record<string, unknown> = {};
        for (const step of ["from", "where", "orderBy", "limit", "values", "set", "onDuplicateKeyUpdate"]) {
            builder[step] = (...args: unknown[]) => {
                operations.push({ op: step, args });
                return builder;
            };
        }
        builder.then = (resolve: (value: unknown) => unknown, reject: (error: unknown) => unknown) =>
            Promise.resolve().then(terminal).then(resolve, reject);
        return builder;
    };

    const db = {
        select: (...args: unknown[]) => {
            operations.push({ op: "select", args });
            return chain(() => selectResults.shift() ?? []);
        },
        insert: (...args: unknown[]) => {
            operations.push({ op: "insert", args });
            return chain(() => {
                if (fake.failInsert) throw fake.failInsert;
                return undefined;
            });
        },
        update: (...args: unknown[]) => {
            operations.push({ op: "update", args });
            return chain(() => undefined);
        },
        execute: async (query: { queryChunks?: Array<{ value?: string[] }> }) => {
            const text = (query.queryChunks ?? []).flatMap((chunk) => chunk.value ?? []).join("");
            executed.push(text);
            return [];
        },
    };

    const fake = {
        db: db as unknown as ResilienceDatabase,
        /** When set, every insert fails with this error. */
        failInsert: undefined as Error | undefined,
        operations,
        selectResults,
        executed,
        opsNamed(name: string): unknown[][] {
            return operations.filter((operation) => operation.op === name).map((operation) => operation.args);
        },
    };
    return fake;
}
