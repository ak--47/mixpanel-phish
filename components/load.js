
import { tmpdir } from 'os';
import path from 'path';
import dotenv from 'dotenv';
import { rm, ls, makeExist, isDirOrFile, details, touch } from 'ak-tools';
dotenv.config();
const { NODE_ENV = "", MIXPANEL_TOKEN = "", MIXPANEL_SECRET = "", MIXPANEL_PROJECT = "", MIXPANEL_BEARER = "" } = process.env;
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);
if (!NODE_ENV) throw new Error("NODE_ENV is required");
if (!MIXPANEL_TOKEN) throw new Error("MIXPANEL_TOKEN is required");
if (!MIXPANEL_SECRET) throw new Error("MIXPANEL_SECRET is required");
if (!MIXPANEL_PROJECT) throw new Error("MIXPANEL_PROJECT is required");
if (!MIXPANEL_BEARER) throw new Error("MIXPANEL_BEARER is required");
import mp from 'mixpanel-import';
import { annotations } from "./timeline.js";

/** @type {mp.Creds} */
const importCreds = {
	token: MIXPANEL_TOKEN,
	project: MIXPANEL_PROJECT,
	secret: MIXPANEL_SECRET,
	bearer: MIXPANEL_BEARER
};


export default async function main(directory = "output") {
		
	const fileSystem = (await ls(path.resolve(TEMP_DIR, directory)))
		.filter((dir) => isDirOrFile(dir) === 'directory')
		.map((dir) => details(dir))
		.filter(dir => dir.files.length > 0)
		.map((dir) => {
			dir.model = path.basename(dir.path);
			return dir;
		});


	const results = {fileSystem};
	const profileDeletes = await deleteProfiles();
	results.profileDeletes = profileDeletes;


	//we iterate over the directories, and load the data into mixpanel
	loopModels: for (const dir of fileSystem) {
		const { model } = dir;
		/** @type {mp.Options} */
		const commonOpts = {
			recordType: "event",
			streamFormat: "parquet",
			verbose: false,
			showProgress: true,
			removeNulls: true,
			abridged: true,
			strict: true,
			fixData: true,
			workers: 50,
			transformFunc: function cleanUp(record) {

				// $latitude and $longitude are required, but SQL can't have $ in column names
				// https://docs.mixpanel.com/docs/tracking-best-practices/geolocation#define-latitude-and-longitude
				if (record.properties) {
					if (record.properties.latitude) {
						record.properties.$latitude = record.properties.latitude;
						delete record.properties.latitude;
					}
					if (record.properties.longitude) {
						record.properties.$longitude = record.properties.longitude;
						delete record.properties.longitude;
					}
				}
				return record;
			}

		};

		/** @type {mp.Options} */
		let modelOpts = {};

		switch (model) {
			case "attend_events_view":
				modelOpts.epochStart = 946702800; // 2000-01-01
				break;

			case "heardsong_events_view":
				modelOpts.epochStart = 946702800; // 2000-01-01
				break;

			case "review_events_view":
				modelOpts.epochStart = 946702800; // 2000-01-01
				break;

			case "user_profiles_view":
				modelOpts.recordType = "user";
				break;

			case "performance_profiles_view":
				modelOpts.recordType = "group";
				modelOpts.groupKey = "performance_id";
				break;

			case "show_profiles_view":
				modelOpts.recordType = "group";
				modelOpts.groupKey = "show_id";
				break;

			case "venue_profiles_view":
				modelOpts.recordType = "group";
				modelOpts.groupKey = "venue_id";
				break;

			default:
				throw new Error(`Model ${model} not recognized`);
				break;
		}

		const options = { ...commonOpts, ...modelOpts };
		const target = dir.path;
		if (NODE_ENV === 'dev') console.log(`\nImporting ${model} into Mixpanel\n`);
		const importResults = await mp(importCreds, target, options);
		if (NODE_ENV === 'dev') console.log(`\nImported ${model} into Mixpanel\n`);
		results[model] = importResults;
	}
	const loadedAnnotations = await loadChartAnnotations();
	results.annotations = loadedAnnotations?.dryRun || [];
	await touch(path.resolve(TEMP_DIR, '/output/results.json'), results, true, false, false);
	return results;
}

async function deleteProfiles() {
	const { PERF_GROUP_ID, SHOW_GROUP_ID, SONG_GROUP_ID } = process.env;
	if (!PERF_GROUP_ID) throw new Error("PERF_GROUP_ID is required");
	if (!SHOW_GROUP_ID) throw new Error("SHOW_GROUP_ID is required");
	if (!SONG_GROUP_ID) throw new Error("SONG_GROUP_ID is required");

	/** @type {mp.Options} */
	const commonOpts = {
		recordType: "profile-delete",
		verbose: false,
		showProgress: true
	};

	const users = await mp(importCreds, null, { ...commonOpts });
	const shows = await mp(importCreds, null, { ...commonOpts, groupKey: "show_id", dataGroupId: SHOW_GROUP_ID });
	const performances = await mp(importCreds, null, { ...commonOpts, groupKey: "performance_id", dataGroupId: PERF_GROUP_ID });
	const songs = await mp(importCreds, null, { ...commonOpts, groupKey: "song_id", dataGroupId: SONG_GROUP_ID });


	return { users, shows, performances, songs };

}

async function loadChartAnnotations() {

	/** @type {mp.Options} */
	const options = {
		recordType: "annotations",
		verbose: false,
		showProgress: true
	};
	const deleted = await mp(importCreds, null, { ...options, recordType: "delete-annotations" });
	const importedAnnotations = await mp(importCreds, annotations, options);
	return importedAnnotations;
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
		// result = await deleteProfiles();
		// result = await loadChartAnnotations();
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
	}


	if (NODE_ENV === 'dev') debugger;
}