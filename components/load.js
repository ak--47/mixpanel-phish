
import { tmpdir } from 'os';
import path from 'path';
import dotenv from 'dotenv';
import { rm, ls, makeExist, isDirOrFile, details, touch } from 'ak-tools';
dotenv.config();
const { NODE_ENV = "", MIXPANEL_TOKEN = "" } = process.env;
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);
if (!NODE_ENV) throw new Error("NODE_ENV is required");
if (!MIXPANEL_TOKEN) throw new Error("MIXPANEL_TOKEN is required");
import mp from 'mixpanel-import';


// export async function loadToMixpanelOld() {
// 	if (NODE_ENV === 'dev') "loading data into memory";
// 	const {
// 		attendance,
// 		shows,
// 		performances,
// 		songBank,
// 		venues,
// 		reviews,
// 		users,
// 	} = await loadData();
// 	if (NODE_ENV === 'dev') "data loaded into memory";

// 	/** @type {mp.Creds} */
// 	const importCreds = {
// 		token: MIXPANEL_TOKEN,
// 	};

// 	/** @type {mp.Options} */
// 	const commonOptions = {
// 		"recordType": "event",
// 		"verbose": NODE_ENV === 'dev' ? true : false,
// 		showProgress: true, // !todo: NOT FOR PROD
// 		removeNulls: true,
// 		fixData: true,
// 		abridged: true,
// 		strict: true

// 	};

// 	/** @type {mp.Options} */
// 	const attendEvOpts = {
// 		...commonOptions,
// 		transformFunc: attendanceEvents
// 	};

// 	/** @type {mp.Options} */
// 	const reviewEvOpts = {
// 		...commonOptions,
// 		transformFunc: reviewEvents
// 	};

// 	/** @type {mp.Options} */
// 	const perfEvOpts = {
// 		...commonOptions,
// 		transformFunc: performanceEvents
// 	};


// 	/** @type {mp.Options} */
// 	const userProfilesOptions = {
// 		...commonOptions,
// 		recordType: "user",
// 		transformFunc: phanProfiles,
// 	};

// 	/** @type {mp.Options} */
// 	const perfProfOptions = {
// 		...commonOptions,
// 		recordType: "group",
// 		groupKey: "performance_id",
// 		transformFunc: performanceProfiles,
// 	};

// 	/** @type {mp.Options} */
// 	const songProfOptions = {
// 		...perfProfOptions,
// 		groupKey: "song_id",
// 		transformFunc: songProfiles,
// 	};

// 	/** @type {mp.Options} */
// 	const venProfOptions = {
// 		...perfProfOptions,
// 		groupKey: "venue_id",
// 		transformFunc: venueProfiles,
// 	};

// 	/** @type {mp.Options} */
// 	const showProfOptions = {
// 		...perfProfOptions,
// 		groupKey: "show_id",
// 		transformFunc: showProfiles
// 	};


// 	const userProfImport = await mp(importCreds, users, userProfilesOptions);
// 	const perfProfImport = await mp(importCreds, performances, perfProfOptions);
// 	const songProfImport = await mp(importCreds, songBank, songProfOptions);
// 	const venProfImport = await mp(importCreds, venues, venProfOptions);
// 	const showProfImport = await mp(importCreds, shows, showProfOptions);
// 	// const perfEvImport = await mp(importCreds, performances, perfEvOpts);
// 	const reviewEvImport = await mp(importCreds, reviews, reviewEvOpts);
// 	const attendEvImport = await mp(importCreds, attendance, attendEvOpts);



// 	const results = {
// 		attendEvImport,
// 		reviewEvImport,
// 		// perfEvImport,
// 		userProfImport,
// 		perfProfImport,
// 		songProfImport,
// 		venProfImport,
// 		showProfImport
// 	};

// 	return results;
// }

export async function main(directory = "output") {
	const fileSystem = (await ls(path.resolve(TEMP_DIR, directory)))
		.filter((dir) => isDirOrFile(dir) === 'directory')
		.map((dir) => details(dir))
		.filter(dir => dir.files.length > 0)
		.map((dir) => {
			dir.model = path.basename(dir.path);
			return dir;
		});

	/** @type {mp.Creds} */
	const importCreds = {
		token: MIXPANEL_TOKEN,
	};

	const results = {};


	//we iterate over the directories, and load the data into mixpanel
	loopModels: for (const dir of fileSystem) {
		const { model } = dir;
		/** @type {mp.Options} */
		const commonOpts = {
			recordType: "event",
			streamFormat: "parquet",
			verbose: false,
			showProgress: NODE_ENV === 'dev' ? true : false,
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
			}
		};

		/** @type {mp.Options} */
		let modelOpts = {};

		switch (model) {
			case "attend_events_view":
				break;

			case "heardsong_events_view":
				break;

			case "review_events_view":
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
	await touch(path.resolve(TEMP_DIR, 'results.json'), results, true, false, false);
	return results;
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