import dotenv from 'dotenv';
import { tmpdir } from 'os';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import path from 'path';
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);


import { ls, load, rm } from 'ak-tools';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
dayjs.extend(utc);
import { reloadDatabase, runSQL, listAllViews, writeFromTableToDisk } from "./duck.js";

export default async function main(modelName = "") {
	const reloadResults = await reloadDatabase();

	if (NODE_ENV === 'dev') console.log('\n----------------------------------\n');

	// construct our views
	const eventModels = (await ls('./models/events'))
		.filter(f => f.endsWith('.sql'));
	const profileModels = (await ls('./models/profiles'))
		.filter(f => f.endsWith('.sql'));
	let models = [...eventModels, ...profileModels];
	if (modelName) models = models.filter(m => m.includes(modelName));
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
			const writeResult = await writeFromTableToDisk(table_name);
			viewResults.push({ table_name, writeResult });
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	}
	if (NODE_ENV === 'dev') console.log('\n----------------------------------\n');

	//summarize operations
	const viewsSummary = [];
	for (const result of viewResults) {
		const summary = { ...result };
		const model = modelResults.find(m => m.modelName?.startsWith(result.table_name));
		if (model) {
			summary.modelName = model.modelName;
			summary.sample = model.result;
		}
		viewsSummary.push(summary);
	}

	return { raw: reloadResults, views: viewsSummary };
}



if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	// to re-fetch the data...
	// const files = await ls(TEMP_DIR);
	// for (const file of files) {
	// 	await rm(file);
	// }
	let result;
	try {
		result = await main();
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
	}

	if (NODE_ENV === 'dev') debugger;
}
