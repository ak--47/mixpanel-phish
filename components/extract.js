
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
dayjs.extend(utc);


/** Fetching Shows */
export async function getShows() {
	try {
		let cachedShows = null;
		try {
			cachedShows = await u.load(`${TEMP_DIR}/shows.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached shows");
			return cachedShows;
		} catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}

		if (NODE_ENV === 'dev') console.log("Fetching shows from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getShowOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/shows/artist/phish.json',
			searchParams: { apikey: API_KEY, order_by: 'showdate', direction: 'desc' },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const shows = await fetch(getShowOptions);
		if (!shows?.data) throw new Error("No data returned from phish.net");
		cachedShows = await u.touch(`${TEMP_DIR}/shows.json`, shows.data, true);
		if (NODE_ENV === 'dev') console.log("wrote shows to cache");
		return cachedShows;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

/** Fetching Setlists */
export async function getPerformances() {
	try {
		let cachedPerformances = null;
		try {
			cachedPerformances = await u.load(`${TEMP_DIR}/performances.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached performances");
			return cachedPerformances;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching performances from phish.net");
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		/** @type {fetch.BatchRequestConfig} */
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

		const getSetlistRequests = showDates.map(date => {
			const url = `${getPerformancesOptions.url}${date}.json`;
			return { ...getPerformancesOptions, url };
		});

		const setlists = await fetch(getSetlistRequests);
		const performanceData = setlists.map(s => s.data);
		cachedPerformances = await u.touch(`${TEMP_DIR}/performances.json`, performanceData.flat(), true);
		if (NODE_ENV === 'dev') console.log("wrote setlists to cache");
		return cachedPerformances;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

export async function getVenues() {
	try {
		let cachedVenues = null;
		try {
			cachedVenues = await u.load(`${TEMP_DIR}/venues.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached vanues");
			return cachedVenues;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching venues from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getVenueOptions = {
			concurrency: 10,
			delay: 1000,
			verbose: false,
			method: 'GET',
			url: 'https://api.phish.net/v5/venues',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (NODE_ENV === 'dev') debugger;
			}
		};


		const venues = await fetch(getVenueOptions);
		const venueData = venues?.data || [];
		cachedVenues = await u.touch(`${TEMP_DIR}/venues.json`, venueData, true);
		if (NODE_ENV === 'dev') console.log("wrote venues to cache");
		return cachedVenues;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}


export async function getAttendance() {
	try {
		let cachedAttendance = null;
		try {
			cachedAttendance = await u.load(`${TEMP_DIR}/attendance.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached attendance");
			return cachedAttendance;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching attendance from phish.net");

		/** @type {fetch.BatchRequestConfig} */
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
				const resp = r.data.map(person => {
					return {
						uid: person.uid,
						username: person.username,
						venue: person.venue,
						showid: person.showid,
						venueid: person.venueid,
						date: person.showdate
					};
				});
				return resp;

			}
		};
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);
		const getAttendanceRequests = showIds.map(id => {
			const url = `${getAttendanceOptions.url}${id}.json`;
			return { ...getAttendanceOptions, url };
		});

		const attendanceData = await fetch(getAttendanceRequests);		
		cachedAttendance = await u.touch(`${TEMP_DIR}/attendance.json`, attendanceData.flat(), true);
		if (NODE_ENV === 'dev') console.log("wrote attendance to cache");
		return cachedAttendance;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}


export async function getReviews() {
	try {
		let cachedReviews = null;
		try {
			cachedReviews = await u.load(`${TEMP_DIR}/reviews.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached reviews");
			return cachedReviews;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching reviews from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getAttendanceOptions = {
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
			},
			responseHandler: (r) => {				
				const resp = r.data.map(review => {
					return {
						uid: review.uid,
						username: review.username,
						venue: review.venue,
						showid: review.showid,
						venueid: review.venueid,
						date: review.showdate,
						score: review.score,
						text: review.review_text,
						review_id: review.reviewid						
					};
				});
				return resp;

			}
		};
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showIds = pastShows.map(s => s.showid);
		const getReviewRequests = showIds.map(id => {
			const url = `${getAttendanceOptions.url}${id}.json`;
			return { ...getAttendanceOptions, url };
		});

		const reviewData = await fetch(getReviewRequests);		
		cachedReviews = await u.touch(`${TEMP_DIR}/reviews.json`, reviewData.flat(), true);
		if (NODE_ENV === 'dev') console.log("wrote reviews to cache");
		return cachedReviews;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}


export async function getSongs() {
	try {
		let cachedSongs = null;
		try {
			cachedSongs = await u.load(`${TEMP_DIR}/songs.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached songs");
			return cachedSongs;
		} catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}

		if (NODE_ENV === 'dev') console.log("Fetching songs from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getSongOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/songs/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const songs = await fetch(getSongOptions);
		if (!songs?.data) throw new Error("No data returned from phish.net");
		cachedSongs = await u.touch(`${TEMP_DIR}/songs.json`, songs.data, true);
		if (NODE_ENV === 'dev') console.log("wrote songs to cache");
		return cachedSongs;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

export async function getJamNotes() {
	try {
		let cachedJamNotes = null;
		try {
			cachedJamNotes = await u.load(`${TEMP_DIR}/jam-notes.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached jam notes");
			return cachedJamNotes;
		} catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}

		if (NODE_ENV === 'dev') console.log("Fetching jam notes from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getJamNoteOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/jamcharts/',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const jamNotes = await fetch(getJamNoteOptions);
		if (!jamNotes?.data) throw new Error("No data returned from phish.net");
		cachedJamNotes = await u.touch(`${TEMP_DIR}/jam-notes.json`, jamNotes.data, true);
		if (NODE_ENV === 'dev') console.log("wrote songs to cache");
		return cachedJamNotes;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}


export async function getUsers() {
	try {
		let cachedUsers = null;
		try {
			cachedUsers = await u.load(`${TEMP_DIR}/users.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached users");
			return cachedUsers;
		} catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}

		if (NODE_ENV === 'dev') console.log("Fetching users from phish.net");

		/** @type {fetch.BatchRequestConfig} */
		const getUserOptions = {
			method: 'GET',
			url: 'https://api.phish.net/v5/users/uid/0.json',
			searchParams: { apikey: API_KEY },
			headers: { 'User-Agent': 'mixpanel-phish' }
		};

		const users = await fetch(getUserOptions);
		if (!users?.data) throw new Error("No data returned from phish.net");
		cachedUsers = await u.touch(`${TEMP_DIR}/users.json`, users.data, true);
		if (NODE_ENV === 'dev') console.log("wrote users to cache");
		return cachedUsers;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

export async function getPerformanceMeta() {
	try {
		let cachedPerfMeta = null;
		try {
			cachedPerfMeta = await u.load(`${TEMP_DIR}/performance-metadata.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached performance metadata");
			return cachedPerfMeta;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching performance metadata from phish.in");
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		/** @type {fetch.BatchRequestConfig} */
		const getPerformancesOptions = {
			concurrency: 25,
			delay: 1000,
			verbose: false,
			method: 'GET',
			url: 'https://phish.in/api/v2/shows/',
			headers: { 'User-Agent': 'mixpanel-phish' },
			noBatch: true,
			errorHandler: (e) => {
				if (e.status === 404) return;
				if (NODE_ENV === 'dev') debugger;
			}
		};

		const getPerfMetaReqs = showDates.map(date => {
			const url = `${getPerformancesOptions.url}${date}.json`;
			return { ...getPerformancesOptions, url };
		});

		const performanceData = await fetch(getPerfMetaReqs);		
		cachedPerfMeta = await u.touch(`${TEMP_DIR}/performance-metadata.json`, performanceData, true);
		if (NODE_ENV === 'dev') console.log("wrote performance metadata to cache");
		return cachedPerfMeta;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}



if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const u = await getUsers();
	const perfMeta = await getPerformanceMeta();
	const j = await getJamNotes();
	const song = await getSongs();
	const r = await getReviews();
	const a = await getAttendance();
	const v = await getVenues();
	const s = await getShows();
	const st = await getPerformances();
	if (NODE_ENV === 'dev') debugger;
}
