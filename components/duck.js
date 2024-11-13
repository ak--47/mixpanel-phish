
import dotenv from 'dotenv';
dotenv.config();
import { Database } from 'duckdb-async';
import fs from 'fs/promises';
import path from 'path';
import { tmpdir } from 'os';
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import { ls } from "ak-tools";

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
	const tables = await listAllTables();
	const views = await listAllViews();



	for (const { table_name } of tables) {
		await connection.run(`DROP TABLE IF EXISTS ${table_name}`);
	}


	for (const { table_name } of views) {
		await connection.run(`DROP VIEW IF EXISTS ${table_name}`);
	}

	if (NODE_ENV === 'dev') console.log('Database reset: all tables and views dropped');
}

export async function reloadDatabase() {
	await resetDatabase();
	const csvs = (await ls(TEMP_DIR)).filter(f => f.endsWith('.csv'));
	const jsons = (await ls(TEMP_DIR)).filter(f => f.endsWith('.json'));
	const loadCSVPromises = csvs.map(csv => {
		const tableName = path.basename(csv).replace('.csv', '');
		return loadCsvToTable(csv, tableName);
	});
	const loadJsonPromises = jsons.map(json => {
		const tableName = path.basename(json).replace('.json', '');
		return loadJsonlToTable(json, tableName);
	});
	const loadPromises = loadCSVPromises.concat(loadJsonPromises);

	const results = await Promise.all(loadPromises);
	return results;

}

/**
 * Run an SQL query with optional logging for development mode
 */
export async function runSQL(sql, msg) {
	const result = await connection.run(sql);
	if (NODE_ENV === 'dev') console.log(`${msg}`, `Statement Complete`);
	return result;
}


export async function loadCsvToTable(filePath, tableName) {
	if (!filePath) throw new Error('filePath is required');
	if (!(filePath.startsWith(TEMP_DIR))) filePath = path.resolve(TEMP_DIR, filePath);
	if (!tableName) throw new Error('tableName is required');
	const exists = await tableExists(tableName);
	if (exists) {
		await connection.run(`DROP TABLE ${tableName}`);
	}
	await connection.run(`
		CREATE OR REPLACE TABLE ${tableName} AS 
		SELECT * FROM read_csv('${filePath}', header=true)
	`);
	const schema = await getSchema(tableName);
	if (NODE_ENV === 'dev') console.log(`Loaded ${filePath} into TABLE ${tableName} (${schema.length || 0} columns)`);
	return schema;
}


/**
 * Load JSONL files into DuckDB tables
 */
export async function loadJsonlToTable(filePath, tableName) {
	if (!filePath) throw new Error('filePath is required');
	if (!(filePath.startsWith(TEMP_DIR))) filePath = path.resolve(TEMP_DIR, filePath);
	if (!tableName) throw new Error('tableName is required');

	const exists = await tableExists(tableName);
	if (exists) {
		await connection.run(`DROP TABLE ${tableName}`);
	}


	await connection.run(`
		CREATE OR REPLACE TABLE ${tableName} AS
    	SELECT * FROM read_json('${filePath}', 
		format='newline_delimited',  auto_detect=true, map_inference_threshold=-1
		);
		`);

	const schema = await getSchema(tableName);

	if (NODE_ENV === 'dev') {
		console.log(`Loaded ${filePath} into TABLE ${tableName} (${schema.length || 0} columns)`);

	}

	return schema;
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
	const write = await connection.run(`COPY ${tableName} TO '${tablePath}' (FORMAT 'JSON', records=true`);
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

export async function listAllTables() {
	const tables = await connection.all("SELECT table_name FROM information_schema.tables WHERE table_schema='main'");
	return tables;

}

export async function listAllViews(ignoreInternal = true) {
	const ignoreViews = ["duckdb_", "sqlite_", "pragma_"];
	const views = await connection.all("SELECT table_name FROM information_schema.views WHERE table_schema='main'");
	if (ignoreInternal) return views.filter(({ table_name }) => !ignoreViews.some((prefix) => table_name.startsWith(prefix)));
	return views;
}

export async function getSchema(tableName) {
	const schema = await connection.all(`PRAGMA table_info(${tableName})`);
	return schema;
}


// First, let's load both files with explicit show of schema
export async function debugJsonLoading(conn = connection) {
	// Try loading with explicit JSON parsing and inspecting structure
	await conn.run(`
        CREATE OR REPLACE TABLE performances_debug AS 
        SELECT * FROM read_json('${TEMP_DIR}/performances-small.json',  format='newline_delimited', records='auto', auto_detect=true)
    `);

	// Compare with a direct read_json_auto
	await conn.run(`
        CREATE OR REPLACE TABLE performances_auto AS 
        SELECT * FROM read_json_auto('${TEMP_DIR}/performances-small.json',
		format='newline_delimited', records='auto', auto_detect=true)
    `);

	// Check the schemas
	const debugSchema = await conn.all(`
        DESCRIBE performances_debug
    `);

	const autoSchema = await conn.all(`
        DESCRIBE performances_auto
    `);

	await conn.run(`
        CREATE OR REPLACE TABLE songs_debug AS 
        SELECT * FROM read_json('${TEMP_DIR}/songs.json', format='newline_delimited', records='true')
    `);
	const songsDebugSchema = await conn.all(`
		DESCRIBE songs_debug
	`);

	console.log('Debug Schema:', debugSchema);
	console.log('Auto Schema:', autoSchema);
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	// await resetDatabase();
	// await reloadDatabase();
	// await debugJsonLoading();
	// const tables = await listAllTables();
	// const views = await listAllViews();
	// const schemas = [];
	// for (const { table_name } of tables) {
	// 	const schema = await getSchema(table_name);
	// 	schemas.push({ table_name, schema });
	// }
	if (NODE_ENV === 'dev') debugger;
}
