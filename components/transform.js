import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import path from 'path';
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);


import { ls, load } from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';

dayjs.extend(utc);

import { reloadDatabase, runSQL, listAllViews, loadJsonlToTable, writeFromTableToDisk } from "./duck.js";
import { write } from 'fs';

async function main() {
	const reloadResults = await reloadDatabase();

	// construct our views
	const eventModels = (await ls('./models/events'))
		.filter(f => f.endsWith('.sql'));
	const profileModels = (await ls('./models/profiles'))
		.filter(f => f.endsWith('.sql'));
	const models = [...eventModels, ...profileModels];
	const modelResults = [];

	//run all the models
	for (const model of models) {
		const modelName = path.basename(model);
		const result = await runSQL(await load(model));
		modelResults.push({ modelName, result });
	}

	//unload all the views to JSON
	const views = await listAllViews();
	const viewResults = [];

	for (const { table_name } of views) {
		try {		
		const writeResult = await writeFromTableToDisk(table_name)
		viewResults.push({ table_name, writeResult });
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	}



	debugger;
}



if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	await main();
}
