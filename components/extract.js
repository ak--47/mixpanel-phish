
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

import u from 'ak-tools';
import fetch from "ak-fetch";
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import { existsSync } from "fs";
import { createWriteStream, createReadStream } from 'fs';
import readline from 'readline';
dayjs.extend(utc);


/** Utility function for caching */
async function getCachedData(filename, fetchData, sliceLimit = 10000) {
	try {
		let cachedData;
		if (existsSync(`${TEMP_DIR}/${filename}`)) {
			if (NODE_ENV === 'dev') console.log(`Loading cached ${filename}`);
			cachedData = await load(filename, true);
			if (NODE_ENV === 'dev') cachedData = cachedData.slice(0, sliceLimit);
			return cachedData;
		}

		else {
			const data = await fetchData();
			await touch(filename, data, true);
			if (NODE_ENV === 'dev') console.log(`Wrote ${filename} to cache`);
			return data;
		}
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
	}
}

/** Fetching Shows */
export async function getShows() {
	return getCachedData('shows.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching shows from phish.net");

		const getShowOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/shows/artist/phish.json',
			searchParams: { apikey: API_KEY, order_by: 'showdate', direction: 'desc' },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const shows = await fetch(getShowOptions);
		if (!shows?.data) throw new Error("No data returned from phish.net");
		return shows.data;
	});
}

/** Fetching Performances */
export async function getPerformances() {
	return getCachedData('performances.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching performances from phish.net");
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		const getPerformancesOptions = {
			concurrency: 10,
			delay: 1000,
			verbose: false,
			method: 'GET',
			url: 'https://api.phish.net/v5/setlists/showdate/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (NODE_ENV === 'dev') debugger;
			}
		};

		const getSetlistRequests = showDates.map(date => ({
			...getPerformancesOptions,
			url: `${getPerformancesOptions.url}${date}.json`
		}));

		const setlists = await fetch(getSetlistRequests);
		return setlists.map(s => s.data).flat();
	});
}

/** Fetching Venues */
export async function getVenues() {
	return getCachedData('venues.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching venues from phish.net");

		const getVenueOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/venues',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const venues = await fetch(getVenueOptions);
		return venues?.data || [];
	});
}

/** Fetching Attendance */
export async function getAttendance() {
	return getCachedData('attendance.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching attendance from phish.net");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);

		/** @type {import('ak-fetch').BatchRequestConfig} */
		const getAttendanceOptions = {
			concurrency: 25,
			delay: 1000,
			verbose: false,
			method: 'GET',
			url: 'https://api.phish.net/v5/attendance/showid/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (NODE_ENV === 'dev') debugger;
			},
			responseHandler: (r) => {
				return r.data;
			}
		};

		const getAttendanceRequests = showIds.map(id => ({
			...getAttendanceOptions,
			url: `${getAttendanceOptions.url}${id}.json`
		}))

		const attendanceData = await fetch(getAttendanceRequests);
		return attendanceData.flat();
	});
}

/** Fetching Reviews */
export async function getReviews() {
	return getCachedData('reviews.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching reviews from phish.net");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);

		const getReviewOptions = {
			concurrency: 25,
			delay: 1000,
			verbose: false,
			method: 'GET',
			url: 'https://api.phish.net/v5/reviews/showid/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (NODE_ENV === 'dev') debugger;
			}
		};

		const getReviewRequests = showIds.map(id => ({
			...getReviewOptions,
			url: `${getReviewOptions.url}${id}.json`
		}));

		const reviewData = await fetch(getReviewRequests);
		return reviewData.flat();
	});
}

/** Fetching Songs */
export async function getSongs() {
	return getCachedData('songs.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching songs from phish.net");

		const getSongOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/songs/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const songs = await fetch(getSongOptions);
		if (!songs?.data) throw new Error("No data returned from phish.net");
		return songs.data;
	});
}

/** Fetching Jam Notes */
export async function getJamNotes() {
	return getCachedData('jam-notes.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching jam notes from phish.net");

		const getJamNoteOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/jamcharts/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const jamNotes = await fetch(getJamNoteOptions);
		if (!jamNotes?.data) throw new Error("No data returned from phish.net");
		return jamNotes.data;
	});
}

/** Fetching Users */
export async function getUsers() {
	return getCachedData('users.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching users from phish.net");

		const getUserOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/users/uid/0.json',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const users = await fetch(getUserOptions);
		if (!users?.data) throw new Error("No data returned from phish.net");
		return users.data;
	});
}

/** Fetching Performance Metadata */
export async function getMetaData() {
	return getCachedData('metadata.json', async () => {
		if (NODE_ENV === 'dev') console.log("Fetching metadata from phish.in");

		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		const getPerformanceMetaOptions = {
			concurrency: 25,
			delay: 1500,
			verbose: false,
			method: 'GET',
			url: 'https://phish.in/api/v2/shows/',
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (e.status === 404) return {};
				if (NODE_ENV === 'dev') debugger;
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

async function touch(filename, data, append = false) {
    const filePath = `${TEMP_DIR}/${filename}`;
    const writeStream = createWriteStream(filePath, { flags: append ? 'a' : 'w' });

    for (const item of data) {
        writeStream.write(JSON.stringify(item) + '\n');
    }

    writeStream.end();
    return new Promise((resolve, reject) => {
        writeStream.on('finish', resolve);
        writeStream.on('error', reject);
    });
}


async function load(filename) {
    const filePath = `${TEMP_DIR}/${filename}`;
    const readStream = createReadStream(filePath);
    const rl = readline.createInterface({
        input: readStream,
        crlfDelay: Infinity
    });

    const data = [];
    for await (const line of rl) {
        data.push(JSON.parse(line));
    }

    return data;
}

if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const u = await getUsers();
	const perfMeta = await getMetaData();
	const j = await getJamNotes();
	const song = await getSongs();
	const r = await getReviews();
	const a = await getAttendance();
	const v = await getVenues();
	const s = await getShows();
	const st = await getPerformances();
	if (NODE_ENV === 'dev') debugger;
}
