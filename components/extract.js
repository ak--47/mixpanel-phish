
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
import { rm, ls, sleep, load } from 'ak-tools';
dayjs.extend(utc);

const CONCURRENCY = 30;
const DELAY = 100;
let VERBOSE = false;
if (NODE_ENV === 'dev') VERBOSE = true;

//load sample data so we always have a valid API response
const files = await ls('./samples', true);
const loadPromises = Object.keys(files).map(async (filename) => {
    const contents = await load(`./samples/${filename}`, true);
    files[filename] = contents;
});
await Promise.all(loadPromises);
const { 
	'metadata.json': metadataSample,
	'notes.json': noteSample,
	'performances.json': performanceSample,
	'reviews.json': reviewSample,
	'shows.json': showSample,
	'songs.json': songSample,
	'users.json': userSample,
	'venues.json': venueSample
} = files;


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
export async function getUsers(date) {
	const filename = 'users.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching users from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getUserOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/users/uid/0.json',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			hook: (result) => {
				if (date) {
					result = result.filter(u => dayjs(u.date_joined).isAfter(dayjs(date)));
				}
				if (!result.length) return userSample;
				return result;
			}
		};

		const users = await fetch([getUserOptions]);
		if (!users.length) throw new Error("No data returned from phish.net");
		return users;
	});
}


/** Fetching Shows */
export async function getShows(date) {
	const filename = 'shows.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching shows from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getShowOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/shows/artist/phish.json',
			searchParams: { apikey: API_KEY, order_by: 'showdate', direction: 'desc' },
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			hook: (result) => {
				if (date) {
					result = result.filter(s => dayjs(s.showdate).isAfter(dayjs(date)));
				}
				if (!result.length) return showSample;
				return result || [];
			}
		};

		const shows = await fetch([getShowOptions]);
		if (!shows.length) throw new Error("No data returned from phish.net");
		return shows;
	});
}


/** Fetching Venues - not incremental */
export async function getVenues() {
	const filename = 'venues.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching venues from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getVenueOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/venues',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			// no incremental is needed for venues; only 1615 venues
			hook: (result) => {
				if (!result.length) return venueSample;
				return result;
			}
		};

		const venues = await fetch([getVenueOptions]);
		return venues;
	});
}

/** Fetching Jam Notes - not incremental */
export async function getJamNotes(date) {
	const filename = 'notes.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching jam notes from phish.net");

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getJamNoteOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/jamcharts/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			// no incremental for jam notes as it is only used as a lookup table
			hook: (result) => {
				if (date) {
					result = result.filter(n => dayjs(n.showdate).isAfter(dayjs(date)));
				}
				if (!result.length) return noteSample;
				return result;
			}


		};

		const jamNotes = await fetch([getJamNoteOptions]);
		if (!jamNotes.length) throw new Error("No data returned from phish.net");
		return jamNotes;
	});
}



/** Fetching Songs - not incremental */
export async function getSongs() {
	const filename = 'songs.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching songs from phish.net");
		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getSongOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/songs/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			// songs is small enough that we don't need to incrementally fetch
			hook: (result) => {
				if (!result.length) return songSample;
				return result;
			}
		};

		const songs = await fetch([getSongOptions]);
		if (!songs.length) throw new Error("No data returned from phish.net");
		return songs;
	});
}




/** Fetching Performances */
export async function getPerformances(date) {
	const filename = 'performances.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching performances from phish.net");
		const shows = await getShows();
		let showsToFetch;
		if (date) showsToFetch = shows.filter(s => dayjs(s.showdate).isAfter(dayjs(date)));
		else showsToFetch = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = showsToFetch.map(s => s.showdate);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getPerformancesOptions = {
			...COMMON_OPT,
			noBatch: true,
			url: 'https://api.phish.net/v5/setlists/showdate/',
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			hook: (result) => {
				if (!result.length) return performanceSample;
				return result;
			}
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
export async function getMetaData(date) {
	const filename = 'metadata.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching metadata from phish.in");

		const shows = await getShows();
		let showsToFetch;
		if (date) {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isAfter(dayjs(date)));
		}
		else {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		}
		const showDates = showsToFetch.map(s => s.showdate);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getPerformanceMetaOptions = {
			...COMMON_OPT,
			url: 'https://phish.in/api/v2/shows/',
			noBatch: true,
			errorHandler: (e) => {
				if (e.status === 404) return null;
				if (NODE_ENV === 'dev') debugger;
			},
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: (r) => {
				return r;
			},
			hook: (result) => {
				if (Array.isArray(result)) {
					result = result.filter(a => a);
				}
				else {
					result = [];
				}

				if (!result.length) result = metadataSample;
				return result;
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
export async function getAttendance(date) {
	const filename = 'attendance.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching attendance from phish.net");

		const shows = await getShows();
		let showsToFetch;
		if (date) {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isAfter(dayjs(date)));
		}
		else {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		}

		const showIds = showsToFetch.map(s => s.showid);

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
export async function getReviews(date) {
	const filename = 'reviews.json';
	return getCachedData(filename, async () => {
		if (NODE_ENV === 'dev') console.log("Fetching reviews from phish.net");

		const shows = await getShows();
		let showsToFetch;
		if (date) {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isAfter(dayjs(date)));
		}
		else {
			showsToFetch = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		}

		const showIds = showsToFetch.map(s => s.showid);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getReviewOptions = {
			...COMMON_OPT,
			url: 'https://api.phish.net/v5/reviews/showid/',
			noBatch: true,
			logFile: `${TEMP_DIR}/${filename}`,
			responseHandler: nestedData,
			hook: (result) => {
				if (!result.length) return reviewSample;
				return result;
			}
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

export default async function main(date = "", errorHandler = (e) => { }) {
	if (date) date = dayjs.utc(date).format('YYYY-MM-DD');
	if (!date) date = dayjs.utc().subtract(7, 'd').format('YYYY-MM-DD');

	let users, shows, venues, notes, songs, performances, metadata, reviews, attendance;

	if (NODE_ENV === 'dev') console.log("\nGetting users, shows, venues, jam notes, and songs...");
	try {
		([users, shows, venues, notes, songs] = await Promise.all([
			getUsers(date),
			getShows(date),
			getVenues(date),
			getJamNotes(date),
			getSongs(date)

		]));
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		errorHandler(e);
	}


	if (NODE_ENV === 'dev') console.log("Getting performances, metadata, reviews, and attendance...");
	try {
		([performances, metadata, reviews, attendance] = await Promise.all([
			getPerformances(date),
			getMetaData(date),
			getReviews(date),
			getAttendance(date),
		]));

	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		errorHandler(e);
	}

	if (NODE_ENV === 'dev') console.log("Done fetching data\n");
	return {
		users: users.length,
		shows: shows.length,
		venues: venues.length,
		notes: notes.length,
		songs: songs.length,
		performances: performances.length,
		metadata: metadata.length,
		reviews: reviews.length,
		attendance: attendance.length

	};


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

		const args = process.argv.slice(2);
		if (args[0] === 'full') {
			console.log('\nRUNNING FULL BACKFILL!\n');
			result = await main('1980-01-01');

		} else {
			result = await main();


		}

		// await rm('./tmp/users.json');
		// await getUsers('2021-01-01');

	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
	}

	if (NODE_ENV === 'dev') debugger;
}
