import { Database } from 'duckdb-async';
import fs from 'fs/promises';
import { createReadStream } from 'fs';
import readline from 'readline';
import path from 'path';
const TEMP_DIR = path.resolve('./tmp');
const db = await Database.create(path.join(TEMP_DIR, 'fixduck.db'));
const connection = await db.connect();

// create sample data, one with 24 columns and the other with 25 columns
// {col1, col2, ... col24}
// {col1, col2, ... col25}

const dataWithManyCols = [];

Array.from({ length: 1000 }, (_, outer) => {
	const many = {};


	Array.from({ length: 500 }, (_, inner) => {
		many[`col_${inner}`] = makeName();
	});
	dataWithManyCols.push(many);

});

await fs.writeFile(path.join(TEMP_DIR, 'manys.json'), toNdjson(dataWithManyCols));


// load both data sets into tables
await connection.all(`
 CREATE OR REPLACE TABLE lots_columns AS
	 SELECT * FROM read_json('${path.join(TEMP_DIR, 'manys.json')}',
	 auto_detect=true, map_inference_threshold=-1
	 )
	`);


const schemaMany = await connection.all(`DESCRIBE lots_columns`);

debugger;
function convertSchemaToStruct(schema) {
	return `{${Object.entries(schema).map(([key, type]) => `${key}: ${type}`).join(', ')}}`;
}

async function generateSchemaFromFile(filePath, numRows = 10) {
	const data = await loadFirstNRows(filePath, numRows);
	const schema = {};

	data.forEach((row) => {
		Object.keys(row).forEach((key) => {
			const value = row[key];

			// Determine the DuckDB type
			let duckType;
			if (value === null || value === undefined) {
				duckType = 'NULL';
			} else if (typeof value === 'string') {
				duckType = 'VARCHAR';
			} else if (typeof value === 'boolean') {
				duckType = 'BOOLEAN';
			} else if (typeof value === 'number') {
				duckType = Number.isInteger(value) ? 'BIGINT' : 'DOUBLE';
			} else if (Array.isArray(value)) {
				duckType = 'LIST';
			} else if (typeof value === 'object') {
				duckType = 'STRUCT';
			} else {
				throw new Error(`Unsupported type: ${typeof value}`);
			}

			// Update the schema with the appropriate type
			if (!schema[key]) {
				schema[key] = duckType;
			} else if (schema[key] !== duckType) {
				// Handle type promotion for mixed types
				if ((schema[key] === 'BIGINT' && duckType === 'DOUBLE') || (schema[key] === 'DOUBLE' && duckType === 'BIGINT')) {
					schema[key] = 'DOUBLE';  // Promote to DOUBLE if mixed numeric types
				} else if (schema[key] === 'NULL') {
					schema[key] = duckType;  // Promote NULL to first actual type encountered
				} else if (duckType !== 'NULL') {
					throw new Error(`Incompatible types for key "${key}": ${schema[key]} vs ${duckType}`);
				}
			}
		});
	});

	return schema;
}


async function loadFirstNRows(filePath, numRows) {
	const readStream = createReadStream(path.join(TEMP_DIR, filePath));
	const rl = readline.createInterface({
		input: readStream,
		crlfDelay: Infinity
	});

	const data = [];
	return new Promise((resolve, reject) => {
		rl.on('line', (line) => {
			if (data.length >= numRows) {
				rl.close();
			}
			else {
				data.push(JSON.parse(line));
			}
		});

		rl.on('close', () => {
			resolve(data);
		});

		rl.on('error', (err) => {
			reject(err);
		});
	});
}




/**
 * helpers
 */

function toNdjson(data) {
	return data.map((row) => JSON.stringify(row)).join('\n');
}

function makeName(words = 3, separator = "-") {
	const adjs = [
		"dark", "grim", "swift", "brave", "bold", "fiery", "arcane",
		"rugged", "calm", "wild", "brisk", "dusty", "mighty", "sly",
		"old", "ghostly", "frosty", "gilded", "murky", "grand", "sly",
		"quick", "cruel", "meek", "glum", "drunk", "slick", "bitter",
		"nimble", "sweet", "tart", "tough"
	];

	const nouns = [
		"mage", "inn", "imp", "bard", "witch", "drake", "knight", "brew",
		"keep", "blade", "beast", "spell", "tome", "crown", "ale", "bard",
		"joke", "maid", "elf", "orc", "throne", "quest", "scroll", "fey",
		"pixie", "troll", "giant", "vamp", "ogre", "cloak", "gem", "axe",
		"armor", "fort", "bow", "lance", "moat", "den"
	];

	const verbs = [
		"cast", "charm", "brawl", "brew", "haunt", "sail", "storm", "quest",
		"joust", "feast", "march", "scheme", "raid", "guard", "duel",
		"trick", "flee", "prowl", "forge", "explore", "vanish", "summon",
		"banish", "bewitch", "sneak", "chase", "ride", "fly", "dream", "dance"
	];

	const adverbs = [
		"boldly", "bravely", "slyly", "wisely", "fiercely", "stealthily", "proudly", "eagerly",
		"quietly", "loudly", "heroically", "craftily", "defiantly", "infamously", "cleverly", "dastardly"
	];

	const continuations = [
		"and", "of", "in", "on", "under", "over", "beyond", "within", "while", "during", "after", "before",
		"beneath", "beside", "betwixt", "betwain", "because", "despite", "although", "however", "nevertheless"
	];

	let string;
	const cycle = [adjs, nouns, verbs, adverbs, continuations];
	for (let i = 0; i < words; i++) {
		const index = i % cycle.length;
		const word = cycle[index][Math.floor(Math.random() * cycle[index].length)];
		if (!string) {
			string = word;
		} else {
			string += separator + word;
		}
	}

	return string;
};