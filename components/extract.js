
import dotenv from 'dotenv';
dotenv.config();
import path from 'path';
import { tmpdir } from 'os';
const { NODE_ENV = "", API_KEY = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
if (!API_KEY) throw new Error("API_KEY is required");

let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);

import fetch from "ak-fetch";
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import { existsSync } from "fs";
import { loadNDJSON, loadCSV } from './crud.js';
import { loadJsonlToTable, resetDatabase, getSchema } from "./duck.js";
import { rm, ls } from 'ak-tools';
dayjs.extend(utc);

const CONCURRENCY = 30;
const DELAY = 100;
let VERBOSE = false;
if (NODE_ENV === 'dev') VERBOSE = true;


/** Utility function for caching */
async function getCachedData(filename, fetchData) {
	let cachedData;

	//we have the file
	if (existsSync(`${TEMP_DIR}/${filename}`)) {

		if (NODE_ENV === 'dev') console.log(`Loading cached ${filename}`);
		try {
			switch (filename.split('.').pop()?.toLowerCase()) {
				case "json":
					cachedData = await loadNDJSON(filename);
					break;
				case "csv":
					cachedData = await loadCSV(filename);
					break;
				default:
					cachedData = await loadNDJSON(filename);
					break;
			}

			return cachedData;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	}

	//we don't have the file... fetch + load to db
	else {
		const data = await fetchData();
		// const flatData = data.map(FLATTEN);
		// try {
		// 	await touch(filename, data);
		// }
		// catch (e) {
		// 	if (NODE_ENV === 'dev') debugger;
		// }
		// if (NODE_ENV === 'dev') console.log(`Wrote ${filename} to cache`);

		// try {
		// 	await loadJsonlToTable(`${TEMP_DIR}/${filename}`, filename.replace('.json', ''));
		// 	if (NODE_ENV === 'dev') console.log(`Loaded ${filename} to DuckDB`);
		// }
		// catch (e) {
		// 	if (NODE_ENV === 'dev') debugger;
		// }

		return data;
	}
}

/** @type {import('ak-fetch').BatchRequestConfig} */
const COMMON_OPT = {
	method: 'GET',
	verbose: VERBOSE,
	concurrency: CONCURRENCY,
	delay: DELAY,
	searchParams: { apikey: API_KEY },
	headers: { 'User-Agent': 'mixpanel-phish' },
	errorHandler: (e) => {
		if (NODE_ENV === 'dev') debugger;
		return [];
	},
	format: 'ndjson',
};

/** Fetching Users */
export async function getUsers() {
	const filename = 'users.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching users from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getUserOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/users/uid/0.json',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const users = await fetch([getUserOptions]);
		if (!users.length) throw new Error("No data returned from phish.net");
		return users;
	});
}


/** Fetching Shows */
export async function getShows() {
	const filename = 'shows.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching shows from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getShowOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/shows/artist/phish.json',
			searchParams: { apikey: API_KEY, order_by: 'showdate', direction: 'desc' },
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const shows = await fetch([getShowOptions]);
		if (!shows.length) throw new Error("No data returned from phish.net");
		return shows;
	});
}


/** Fetching Venues */
export async function getVenues() {
	const filename = 'venues.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching venues from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getVenueOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/venues',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const venues = await fetch([getVenueOptions]);
		return venues;
	});
}


/** Fetching Songs */
export async function getSongs() {
	const filename = 'songs.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching songs from phish.net");
		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getSongOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/songs/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const songs = await fetch([getSongOptions]);
		if (!songs.length) throw new Error("No data returned from phish.net");
		return songs;
	});
}

/** Fetching Jam Notes */
export async function getJamNotes() {
	const filename = 'notes.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching jam notes from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getJamNoteOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/jamcharts/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData


		};

		const jamNotes = await fetch([getJamNoteOptions]);
		if (!jamNotes.length) throw new Error("No data returned from phish.net");
		return jamNotes;
	});
}



/** Fetching Performances */
export async function getPerformances() {
	const filename = 'performances.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching performances from phish.net");
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getPerformancesOptions = {
			...COMMON_OPT,
			noBatch: true,
			url: 'https://api.phish.net/v5/setlists/showdate/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const getSetlistRequests = showDates.map(date => ({
			...getPerformancesOptions,
			url: `${getPerformancesOptions.url}${date}.json`
		}));

		const setlists = await fetch(getSetlistRequests);
		return setlists;
	});
}


/** Fetching Performance Metadata */
export async function getMetaData() {
	const filename = 'metadata.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching metadata from phish.in");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getPerformanceMetaOptions = {
			...COMMON_OPT,
			url: 'https://phish.in/api/v2/shows/',
			noBatch: true,
			errorHandler: (e) => {
				if (e.status === 404) return {};
				if (NODE_ENV === 'dev') debugger;
			},
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: (r) => {
				return r;
			}

		};

		const getPerformanceMetaRequests = showDates.map(date => ({
			...getPerformanceMetaOptions,
			url: `${getPerformanceMetaOptions.url}${date}.json`
		}));

		const performanceData = await fetch(getPerformanceMetaRequests);
		return performanceData;
	});
}

/** Fetching Attendance */
export async function getAttendance() {
	const filename = 'attendance.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching attendance from phish.net");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getAttendanceOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/attendance/showid/',
			noBatch: true,
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: (r) => {
				return r.data.map(u => {
					const { uid, username, showid, showdate, tour_name, venue, venueid } = u;
					return {
						uid,
						username,
						showid,
						showdate,
						tour_name,
						venue,
						venueid
					};
				});
			}
		};

		const getAttendanceRequests = showIds.map(id => ({
			...getAttendanceOptions,
			url: `${getAttendanceOptions.url}${id}.json`
		}));

		const attendanceData = await fetch(getAttendanceRequests);
		return attendanceData.flat();
	});
}

/** Fetching Reviews */
export async function getReviews() {
	const filename = 'reviews.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching reviews from phish.net");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getReviewOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/reviews/showid/',
			noBatch: true,
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData
		};

		const getReviewRequests = showIds.map(id => ({
			...getReviewOptions,
			url: `${getReviewOptions.url}${id}.json`
		}));

		const reviewData = await fetch(getReviewRequests);
		const reviews = [];
		for (const show of reviewData) {
			if (!show?.data) continue;
			if (!show.data.length) continue;
			reviews.push(show.data);
		}
		return reviews.flat();
	});
}

export async function main() {
	if (NODE_ENV === 'dev') console.log("\nGetting users...");
	const users = await getUsers();
	if (NODE_ENV === 'dev') console.log("Getting shows...");
	const shows = await getShows();
	if (NODE_ENV === 'dev') console.log("Getting venues...");
	const venues = await getVenues();
	if (NODE_ENV === 'dev') console.log("Getting jam notes...");
	const notes = await getJamNotes();
	if (NODE_ENV === 'dev') console.log("Getting songs...");
	const songs = await getSongs();
	if (NODE_ENV === 'dev') console.log("Getting performances, metadata, reviews, and attendance...");
	const [performances, metadata, review, attendance] = await Promise.all([
		getPerformances(),
		getMetaData(),
		getReviews(),
		getAttendance(),
	]);

	if (NODE_ENV === 'dev') console.log("Done fetching data\n");

	return { users, shows, venues, notes, songs, performances, metadata, review, attendance };

}


/**
 * HELPERS
 */


function nestedData(response) {
	if (response.data) {
		if (response.data.length) {
			return response.data;
		}
	}
	return [];

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
