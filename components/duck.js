
import dotenv from 'dotenv';
dotenv.config();
import { Database } from 'duckdb-async';
import fs from 'fs/promises';
import path from 'path';
import { tmpdir } from 'os';
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");

let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);

// Initialize DuckDB... won't want persistence, but we do want spill to disk
// so we can handle large datasets
// await fs.unlink(path.join(TEMP_DIR, 'duckdb.db'))
const db = await Database.create(path.join(TEMP_DIR, 'duckdb.db'));
const connection = await db.connect();


/**
 * Reset the database by dropping all tables, views, and procedures
 */
export async function resetDatabase() {
	// Drop all tables and views
	const tables = await connection.all("SELECT table_name FROM information_schema.tables WHERE table_schema='main'");
	const ignoreViews = ["duckdb_", "sqlite_", "pragma_"];
	const views = (await connection.all("SELECT table_name FROM information_schema.views WHERE table_schema='main'"))
		.filter(({ table_name }) => !ignoreViews.some((prefix) => table_name.startsWith(prefix)));


	for (const { table_name } of tables) {
		await connection.run(`DROP TABLE IF EXISTS ${table_name}`);
	}


	for (const { table_name } of views) {
		await connection.run(`DROP VIEW IF EXISTS ${table_name}`);
	}

	if (NODE_ENV === 'dev') console.log('Database reset: all tables and views dropped');
}

/**
 * Run an SQL query with optional logging for development mode
 */
export async function runSQL(sql, msg) {
	const result = await connection.run(sql);
	if (NODE_ENV === 'dev') console.log(`${msg}`, `Statement Complete`);
	return result;
}

/**
 * Load JSONL files into DuckDB tables
 */
export async function loadJsonlToTable(filePath, tableName) {
	if (!filePath) throw new Error('filePath is required');
	if (!tableName) throw new Error('tableName is required');
	const exists = await tableExists(tableName);
	if (exists) {
		await connection.run(`DROP TABLE ${tableName}`);
	}
	const load = await connection.run(`CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM read_json_auto('${filePath}')`);
	if (NODE_ENV === 'dev') console.log(`Loaded ${filePath} into ${tableName}`);
	return load;
}

/**
 * Join two tables on a specified column
 */
export async function joinTables(table1, table2, joinOn, joinedTableName) {
	if (!table1 || !table2 || !joinOn || !joinedTableName) {
		throw new Error('All parameters (table1, table2, joinOn, joinedTableName) are required');
	}
	const join = await connection.run(`
	  CREATE OR REPLACE TABLE ${joinedTableName} AS
	  SELECT t1.*, t2.*
	  FROM ${table1} AS t1
	  JOIN ${table2} AS t2 ON t1.${joinOn} = t2.${joinOn}
	`);
	if (NODE_ENV === 'dev') console.log(`Joined ${table1} and ${table2} into ${joinedTableName}`);
	return join;
}

/**
 * Write a table to a CSV file in the specified path
 */
export async function writeTableToJson(tableName, tablePath = TEMP_DIR) {
	if (!tableName) throw new Error('tableName is required');
	await fs.mkdir(path.dirname(tablePath), { recursive: true });
	const write = await connection.run(`COPY ${tableName} TO '${tablePath}' (FORMAT 'JSON'`);
	if (NODE_ENV === 'dev') console.log(`Saved ${tablePath}`);
	return write;
}

/**
 * Check if a table exists in the database
 */
export async function tableExists(tableName) {
	const result = await connection.all(`SELECT table_name FROM information_schema.tables WHERE table_name = '${tableName}'`);
	return result.length > 0;
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	await resetDatabase();
	if (NODE_ENV === 'dev') debugger;
}
