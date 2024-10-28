
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
export async function getSetlists() {
	try {
		let cachedSetlists = null;
		try {
			cachedSetlists = await u.load(`${TEMP_DIR}/setlists.json`, true);
			if (NODE_ENV === 'dev') console.log("Loaded cached setlists");
			return cachedSetlists;
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
		if (NODE_ENV === 'dev') console.log("Fetching setlists from phish.net");
		const shows = await getShows();
		const pastShows = shows.filter(s => dayjs(s.showdate).isBefore(dayjs()));
		const showDates = pastShows.map(s => s.showdate);

		/** @type {fetch.BatchRequestConfig} */
		const getSetlistsOptions = {
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
			const url = `${getSetlistsOptions.url}${date}.json`;
			return { ...getSetlistsOptions, url };
		});

		const setlists = await fetch(getSetlistRequests);
		const setlistData = setlists.map(s => s.data);
		cachedSetlists = await u.touch(`${TEMP_DIR}/setlists.json`, setlistData, true);
		if (NODE_ENV === 'dev') console.log("wrote setlists to cache");
		return cachedSetlists;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}


if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
    const shows = await getShows();
    const setlists = await getSetlists();
    if (NODE_ENV === 'dev') debugger;
}
