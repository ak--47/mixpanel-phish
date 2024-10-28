
import { tmpdir } from 'os';
import path from 'path';
import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "", MIXPANEL_TOKEN = "" } = process.env;
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);
if (!NODE_ENV) throw new Error("NODE_ENV is required");
if (!MIXPANEL_TOKEN) throw new Error("MIXPANEL_TOKEN is required");

import u from 'ak-tools';
import { songsToGroupProfiles } from "./transform.js";
import mp from 'mixpanel-import';

export async function loadToMixpanel() {
	try {
		const profiles = await songsToGroupProfiles();

		/** @type {mp.Creds} */
		const importCreds = {
			token: MIXPANEL_TOKEN,
			groupKey: "$song_id",
		};

		/** @type {mp.Options} */
		const importImportOptions = {
			recordType: "group",
			groupKey: "$song_id",
			verbose: true,
			fixData: true,
		};

		const importResults = await mp(importCreds, profiles, importImportOptions);
		const importLog = await u.touch(`${TEMP_DIR}/import-log.json`, importResults, true);
		return importLog;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const importLog = await loadToMixpanel();
	if (NODE_ENV === 'dev') debugger;
}