
import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = ""} = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import {  } from './function.js';
import u from 'ak-tools';


/** @type {mixpanel.Creds} */
const creds = {
	token: "101566bc27adba2573acf060c0f541b6" //MIXPANEL_TOKEN,
};


/** @type {mixpanel.Options} */
const extraOptions = {
	dryRun: true,
	verbose: false,
	flattenData: true,
	scrubProps: ["Sale.OperatorName"],
	tags: { "$source": "mixpanel-mcd-pos-new-relic-poc" },
};

async function main() {
	try {
		const files = (await u.ls('./data')).filter(f => f.endsWith('.json'));

		for (const file of files) {
			const importedData = await new_relic_pipeline(creds, file, extraOptions);
			const { dryRun: DATA } = importedData;
			debugger;
		}

		return 'done';
	} catch (error) {
		debugger;
	}
};


await main();




