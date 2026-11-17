import type { InboxTransactionConfigDto } from "@/index";

export type FakeTransaction = {
    id: number;
    committed: boolean;
    rolledBack: boolean;
    executed: Array<{ sql: string; params: unknown[] }>;
};

/** In-memory transaction runner that mimics `sequelize.transaction(work)` (commit / rollback). */
export function createFakeTransactions(options: { failExecute?: Error } = {}) {
    const transactions: FakeTransaction[] = [];
    const config: InboxTransactionConfigDto = {
        async run<T>(work: (transaction: unknown) => Promise<T>): Promise<T> {
            const transaction: FakeTransaction = {
                id: transactions.length + 1,
                committed: false,
                rolledBack: false,
                executed: [],
            };
            transactions.push(transaction);
            try {
                const result = await work(transaction);
                transaction.committed = true;
                return result;
            } catch (error) {
                transaction.rolledBack = true;
                throw error;
            }
        },
        async execute(transaction: unknown, sql: string, params: unknown[]): Promise<unknown> {
            if (options.failExecute) throw options.failExecute;
            (transaction as FakeTransaction).executed.push({ sql, params });
            return undefined;
        },
    };
    return { config, transactions };
}
