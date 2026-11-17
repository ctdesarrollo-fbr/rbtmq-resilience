import { drizzle, MySql2Database } from "drizzle-orm/mysql2";
import { sql } from "drizzle-orm";
import mysql from "mysql2/promise";
import { mysqlMigration } from "./registry";
import { MySqlConnectionConfigDto, RbtmqResilienceRuntimeConfigDto } from "@/shared/dtos";

export type ResilienceDatabase = MySql2Database<Record<string, never>>;

export class DatabaseService {
    private mysqlPool?: mysql.Pool;
    private db?: ResilienceDatabase;

    constructor(
        private readonly config: RbtmqResilienceRuntimeConfigDto,
    ) {}

    public async init(): Promise<ResilienceDatabase> {
        if (this.db) return this.db;

        if (this.config.db) {
            this.db = this.config.db;
        } else if (this.config.mysql) {
            this.mysqlPool = this.createPool(this.config.mysql);
            this.db = drizzle(this.mysqlPool);
        } else {
            throw new Error("rbtmq-resilience requires config.mysql or config.db.");
        }

        await this.ensureSchema(this.db);
        return this.db;
    }

    /** The database once init() has run; undefined before that. */
    public getDb(): ResilienceDatabase | undefined {
        return this.db;
    }

    public async close(): Promise<void> {
        await this.mysqlPool?.end();
    }

    private createPool(config: MySqlConnectionConfigDto): mysql.Pool {
        return mysql.createPool({
            host: config.host,
            port: config.port ?? 3306,
            user: config.user,
            password: config.password,
            database: config.database,
        });
    }

    private async ensureSchema(db: ResilienceDatabase): Promise<void> {
        const statements = mysqlMigration
            .split(";")
            .map((statement) => statement.trim())
            .filter((statement) => statement.length > 0);

        for (const statement of statements) {
            await db.execute(sql.raw(statement));
        }
    }
}
