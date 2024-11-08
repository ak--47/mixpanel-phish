
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

import {
	loadData,
	attendanceEvents,
	performanceEvents,
	performanceProfiles,
	phanProfiles,
	reviewEvents,
	showProfiles,
	songProfiles,
	venueProfiles
} from "./transform.js";
import mp from 'mixpanel-import';


export async function loadToMixpanel() {
	if (NODE_ENV === 'dev') "loading data into memory"
	const {
		attendance,
		shows,
		performances,
		songBank,
		venues,
		reviews,
		users,
	} = await loadData();
	if (NODE_ENV === 'dev') "data loaded into memory"

	/** @type {mp.Creds} */
	const importCreds = {
		token: MIXPANEL_TOKEN,
	};

	/** @type {mp.Options} */
	const commonOptions = {
		"recordType": "event",
		"verbose": NODE_ENV === 'dev' ? true : false,
		showProgress: true, // !todo: NOT FOR PROD
		removeNulls: true,
		fixData: true,
		abridged: true,
		strict: true

	};
	
	/** @type {mp.Options} */
	const attendEvOpts = {
		...commonOptions,
		transformFunc: attendanceEvents
	}
	
	/** @type {mp.Options} */
	const reviewEvOpts = {
		...commonOptions,
		transformFunc: reviewEvents
	}
	
	/** @type {mp.Options} */
	const perfEvOpts = {
		...commonOptions,
		transformFunc: performanceEvents
	}
	
	
	/** @type {mp.Options} */
	const userProfilesOptions = {
		...commonOptions,
		recordType: "user",
		transformFunc: phanProfiles,
	};
	
	/** @type {mp.Options} */
	const perfProfOptions = {
		...commonOptions,
		recordType: "group",
		groupKey: "performance_id",
		transformFunc: performanceProfiles,
	};
	
	/** @type {mp.Options} */
	const songProfOptions = {
		...perfProfOptions,
		groupKey: "song_id",
		transformFunc: songProfiles,
	};
	
	/** @type {mp.Options} */
	const venProfOptions = {
		...perfProfOptions,
		groupKey: "venue_id",
		transformFunc: venueProfiles,
	};
	
	/** @type {mp.Options} */
	const showProfOptions = {
		...perfProfOptions,
		groupKey: "show_id",
		transformFunc: showProfiles
	};
	

	const userProfImport = await mp(importCreds, users, userProfilesOptions);
	const perfProfImport = await mp(importCreds, performances, perfProfOptions);
	const songProfImport = await mp(importCreds, songBank, songProfOptions);
	const venProfImport = await mp(importCreds, venues, venProfOptions);
	const showProfImport = await mp(importCreds, shows, showProfOptions);
	// const perfEvImport = await mp(importCreds, performances, perfEvOpts);
	const reviewEvImport = await mp(importCreds, reviews, reviewEvOpts);
	const attendEvImport = await mp(importCreds, attendance, attendEvOpts);



	const results = {
		attendEvImport,
		reviewEvImport,
		// perfEvImport,
		userProfImport,
		perfProfImport,
		songProfImport,
		venProfImport,
		showProfImport
	};

	return results;
}


if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const importLog = await loadToMixpanel();
	if (NODE_ENV === 'dev') debugger;
}