
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
import { rm, ls, sleep } from 'ak-tools';
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
				if (!result.length) return [{ "uid": "84556", "username": "ErikaDixon", "date_joined": "2024-01-01 00:23:26" }];
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
				if (!result.length) return [{ "showid": "1718730981", "showyear": "2025", "showmonth": "2", "showday": "1", "showdate": "2025-02-01", "permalink": "https://phish.net/setlists/phish-february-01-2025-moon-palace-cancun-quintana-roo-mexico.html", "exclude_from_stats": "0", "venueid": "1481", "setlist_notes": "", "venue": "Moon Palace", "city": "Cancun, Quintana Roo", "state": "", "country": "Mexico", "artistid": "1", "artist_name": "Phish", "tourid": "208", "tour_name": "2025 Mexico", "created_at": "2024-06-18 13:16:21", "updated_at": "2024-07-02 15:24:49" }];
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
				if (!result.length) return [{ "venueid": "1", "venuename": "Meadows Music Theatre", "city": "Hartford", "state": "CT", "country": "USA", "venuenotes": "", "alias": "523", "short_name": "The Meadows" }];
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
				if (!result.length) return [{ "showid": "1684286358", "showdate": "2024-02-22", "permalink": "https://phish.net/setlists/phish-february-22-2024-moon-palace-cancun-quintana-roo-mexico.html", "showyear": "2024", "uniqueid": "494705", "meta": "", "reviews": "7", "exclude": "0", "setlistnotes": "Driver was performed for the first time since July 30, 2021 (131 shows). Shipwreck was quoted in Axilla (Part II)", "soundcheck": "", "songid": "7", "position": "10", "transition": "2", "footnote": "", "set": "2", "isjam": "0", "isreprise": "0", "isjamchart": "1", "jamchart_description": "Opening a second set for the first time since the 2017 Mexico shows, this \"ASIHTOS\" features a swirling, grimy intro that indicates the band may be ready to deliver the goods. The jam begins as a laid-back affair, an apt soundtrack for oceanside listening. The tempo picks up gradually; Trey's riffing and Page's play stand out. The jam slowly dissolves and > into a huge, must-hear \"Wave of Hope.\"", "tracktime": "", "gap": "6", "tourid": "205", "tourname": "2024 Mexico", "tourwhen": "2024 Mexico", "song": "A Song I Heard the Ocean Sing", "nickname": "ASIHTOS", "slug": "a-song-i-heard-the-ocean-sing", "is_original": "1", "venueid": "1481", "venue": "Moon Palace", "city": "Cancun, Quintana Roo", "state": "", "country": "Mexico", "trans_mark": " > ", "artistid": "1", "artist_slug": "phish", "artist_name": "Phish" }];
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
				if (!result.length) return [{ "songid": "690", "song": "You Enjoy Myself", "slug": "you-enjoy-myself", "abbr": "YEM", "artist": "Phish", "debut": "1986-02-03", "last_played": "2024-08-30", "times_played": "614", "last_permalink": "https://phish.net/setlists/phish-august-30-2024-dicks-sporting-goods-park-commerce-city-co-usa", "debut_permalink": "https://phish.net/setlists/phish-february-03-1986-hunts-burlington-vt-usa", "gap": "5" }];
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
				if (!result.length) return [{ "showid": "1709062035", "showdate": "2024-08-11", "permalink": "https://phish.net/setlists/phish-august-11-2024-bethel-woods-center-for-the-arts-bethel-ny-usa.html", "showyear": "2024", "uniqueid": "500349", "meta": "", "reviews": "5", "exclude": "0", "setlistnotes": "Fikus was performed for the first time since November 7, 1998 (801 show gap). Mike teased Fikus in AC/DC Bag.", "soundcheck": "Jam, My Soul", "songid": "197", "position": "4", "transition": "1", "footnote": "", "set": "1", "isjam": "0", "isreprise": "0", "isjamchart": "0", "jamchart_description": "", "tracktime": "", "gap": "800", "tourid": "172", "tourname": "2024 Summer Tour", "tourwhen": "2024 Summer", "song": "Fikus", "nickname": "Fikus", "slug": "fikus", "is_original": "1", "venueid": "809", "venue": "Bethel Woods Center for the Arts", "city": "Bethel", "state": "NY", "country": "USA", "trans_mark": ", ", "artistid": "1", "artist_slug": "phish", "artist_name": "Phish" }];
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
				result = result.filter(a => a);
				if (!result.length) result = [{ "id": 2219, "date": "2024-10-26", "cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "album_zip_url": "https://phish.in/blob/2xfuk33q9m9e7zdal6ko89j2gq5y.zip", "duration": 10874020, "incomplete": false, "admin_notes": null, "tour_name": "Divided Sky Foundation Benefit 2024", "venue_name": "MVP Arena", "venue": { "slug": "times-union-center", "name": "Times Union Center", "other_names": ["MVP Arena"], "latitude": 42.6484108, "longitude": -73.7545889, "city": "Albany", "state": "NY", "country": "USA", "location": "Albany, NY", "shows_count": 16, "updated_at": "2013-03-22T22:15:24-04:00" }, "taper_notes": "Phish\nBenefit For The Divided Sky Foundation\nThe MVP Arena\nAlbany, New York\n10/26/2024\n\nSource: Schoeps CCM4V'S>Sound Devices Mix-Pre 6 II (48/32)\nSOB/DRFC/KFC/ZFC/AARP 100' From Stage, 7 1/2' High\nDSP: Sound Devices Mix-Pre 6 II>Sound Forge 10.0>CD Wave>flac(16)\nID3 Tagged In Foobar 2000\nRecorded By: Z-Man\nSeeded By: Z-Man\nTaper's Assistant:Joni Crawford\n\nShow #349 Of 2024\n\nA Team Dirty South And Home Team Recording\n\nDisc I 1st Set\n\n01 Possum\n02 Sigma Oasis >\n03 Back On The Train\n04 Nothing\n05 Stash\n06 Bouncing Around The Room\n07 Tube >\n08 Bathtub Gin\n09 More\n\nDisc II 2nd Set\n\n01 Prince Caspian >\n02 Down With Disease >\n03 Ruby Waves >\n04 Fuego\n05 What's The Use? >\n06 Golden Age >\n07 Lonely Trip\n08 Harry Hood\nEncore:\n09 Golgi Apasrats >\n10 Slave To The Traffic Light\n\nTrey Anastasio - Guitar And Vocals\nMike Gordon - Bass And Vocals\nPage McConnell - Keyboards And Vocals\nJon Fishman - Drums And Vocals", "likes_count": 3, "updated_at": "2024-11-27T15:50:34-05:00", "tags": [], "tracks": [{ "id": 38035, "slug": "possum", "title": "Possum", "position": 1, "duration": 493923, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/cj7j82qtac5en7m3o9pqlkd3bsb4.mp3", "waveform_image_url": "https://phish.in/blob/mmhhnpwjwc9qs6i66lc2bhx4ft88.png", "tags": [], "updated_at": "2024-11-03T11:39:22-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "possum", "title": "Possum", "alias": null, "original": true, "artist": null, "tracks_count": 514, "updated_at": "2021-05-04T09:35:41-04:00", "previous_performance_gap": 2, "previous_performance_slug": "2024-08-30/possum", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38036, "slug": "sigma-oasis", "title": "Sigma Oasis", "position": 2, "duration": 672967, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 1, "mp3_url": "https://phish.in/blob/eqh9fr9qob1o0yu018x29r8c1ju5.mp3", "waveform_image_url": "https://phish.in/blob/g01x7pkkh69ymyqrqxqbd5e537nu.png", "tags": [], "updated_at": "2024-11-03T11:39:23-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "sigma-oasis", "title": "Sigma Oasis", "alias": null, "original": true, "artist": null, "tracks_count": 37, "updated_at": "2021-05-04T09:35:43-04:00", "previous_performance_gap": 3, "previous_performance_slug": "2024-08-29/sigma-oasis", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38037, "slug": "back-on-the-train", "title": "Back on the Train", "position": 3, "duration": 528692, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/j4qpql4gv9cksg3iqnuq2m2uxkfa.mp3", "waveform_image_url": "https://phish.in/blob/hqtac9jwm7e80u5eyggk178p9plz.png", "tags": [{ "name": "Tease", "description": "Transient musical reference", "color": "#888888", "priority": 15, "notes": "Rainy Day Women #12 &amp; 35 by Bob Dylan", "starts_at_second": null, "ends_at_second": null, "transcript": null }, { "name": "Tease", "description": "Transient musical reference", "color": "#888888", "priority": 15, "notes": "Rainy Day Women #12 &amp; 35 by Bob Dylan", "starts_at_second": null, "ends_at_second": null, "transcript": null }, { "name": "Tease", "description": "Transient musical reference", "color": "#888888", "priority": 15, "notes": "Rainy Day Women #12 &amp; 35 by Bob Dylan", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-27T15:50:34-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "back-on-the-train", "title": "Back on the Train", "alias": "BOTT", "original": true, "artist": null, "tracks_count": 160, "updated_at": "2021-05-09T01:13:22-04:00", "previous_performance_gap": 2, "previous_performance_slug": "2024-08-30/back-on-the-train", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38038, "slug": "nothing", "title": "Nothing", "position": 4, "duration": 359706, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/r0hwgblo5b4l36wf4ybyugq9qk3k.mp3", "waveform_image_url": "https://phish.in/blob/tc2nfudpu1udzz9mpb8rzoqpe0de.png", "tags": [{ "name": "Bustout", "description": "First performance of a song in at least 100 shows", "color": "#888888", "priority": 21, "notes": "First performance of Nothing in 116 shows", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-03T11:39:24-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "nothing", "title": "Nothing", "alias": null, "original": true, "artist": null, "tracks_count": 9, "updated_at": "2021-05-08T20:30:19-04:00", "previous_performance_gap": 116, "previous_performance_slug": "2022-06-01/nothing", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38039, "slug": "stash", "title": "Stash", "position": 5, "duration": 650632, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 1, "mp3_url": "https://phish.in/blob/wfoz9udll5yzw258rcpswmdy5d0q.mp3", "waveform_image_url": "https://phish.in/blob/emgxkamfq2cwjphjlm3otijtuvzl.png", "tags": [{ "name": "Audience", "description": "Contribution from audience during performance", "color": "#888888", "priority": 19, "notes": "\"Woos\" during pauses", "starts_at_second": null, "ends_at_second": null, "transcript": null }, { "name": "Tease", "description": "Transient musical reference", "color": "#888888", "priority": 15, "notes": "In Memory of Elizabeth Reed by The Allman Brothers Band", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-03T11:39:26-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "stash", "title": "Stash", "alias": null, "original": true, "artist": null, "tracks_count": 433, "updated_at": "2021-05-04T09:35:41-04:00", "previous_performance_gap": 0, "previous_performance_slug": "2024-09-01/stash", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38040, "slug": "bouncing-around-the-room", "title": "Bouncing Around the Room", "position": 6, "duration": 219298, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/5ydvayekdrxsmbfp0qk1w7xmodwm.mp3", "waveform_image_url": "https://phish.in/blob/d75ggnf025r5fg7nt7c332at7kln.png", "tags": [], "updated_at": "2024-11-03T11:39:26-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "bouncing-around-the-room", "title": "Bouncing Around the Room", "alias": null, "original": true, "artist": null, "tracks_count": 452, "updated_at": "2021-05-04T09:35:38-04:00", "previous_performance_gap": 6, "previous_performance_slug": "2024-08-16/bouncing-around-the-room", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38041, "slug": "tube", "title": "Tube", "position": 7, "duration": 424542, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/2j7jdvtxltx666n2qg4r2kicr3x9.mp3", "waveform_image_url": "https://phish.in/blob/nczb3y4svnr8f4tlzuwk6qswbn5y.png", "tags": [], "updated_at": "2024-11-03T11:39:27-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "tube", "title": "Tube", "alias": null, "original": true, "artist": null, "tracks_count": 187, "updated_at": "2018-12-26T22:40:53-05:00", "previous_performance_gap": 0, "previous_performance_slug": "2024-09-01/tube", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38042, "slug": "bathtub-gin", "title": "Bathtub Gin", "position": 8, "duration": 901642, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 1, "mp3_url": "https://phish.in/blob/03ntl0pkrc70llm7zhf4alr4yxsr.mp3", "waveform_image_url": "https://phish.in/blob/ie6hl1x69zsb42mfxaung5xjj8hm.png", "tags": [{ "name": "Jamcharts", "description": "Phish.net Jam Charts selections (phish.net/jamcharts)", "color": "#888888", "priority": 4, "notes": "> from \"Tube\". Brooding and searching from the outset of the jam, the tempo and intensity gradually pick up steam. By 11:00, the jam is a vortex of energy; the band functioning as one organism. Trey's guitar shines as Page underscores with great play. Trey winds things back to the \"Gin\" riff to put a bow on this one.", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-25T07:03:46-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "bathtub-gin", "title": "Bathtub Gin", "alias": null, "original": true, "artist": null, "tracks_count": 300, "updated_at": "2021-05-08T20:17:54-04:00", "previous_performance_gap": 1, "previous_performance_slug": "2024-08-31/bathtub-gin", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38043, "slug": "more", "title": "More", "position": 9, "duration": 367896, "jam_starts_at_second": null, "set_name": "Set 1", "likes_count": 0, "mp3_url": "https://phish.in/blob/uz36yviuhugixmhaq7nte5dushoq.mp3", "waveform_image_url": "https://phish.in/blob/imn0ij96d2r8fzteh5u4nzft0l6m.png", "tags": [], "updated_at": "2024-11-03T11:39:30-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "more", "title": "More", "alias": null, "original": true, "artist": null, "tracks_count": 44, "updated_at": "2018-12-26T22:43:10-05:00", "previous_performance_gap": 0, "previous_performance_slug": "2024-09-01/more", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38044, "slug": "prince-caspian", "title": "Prince Caspian", "position": 10, "duration": 354090, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 0, "mp3_url": "https://phish.in/blob/2cjb5gdhvroev8b4e3jv7m25ubj1.mp3", "waveform_image_url": "https://phish.in/blob/bhc2o8bafmm64aoy95xd8q97ofuy.png", "tags": [], "updated_at": "2024-11-03T11:39:30-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "prince-caspian", "title": "Prince Caspian", "alias": "Fuckerpants", "original": true, "artist": null, "tracks_count": 179, "updated_at": "2021-05-09T01:13:23-04:00", "previous_performance_gap": 0, "previous_performance_slug": "2024-09-01/prince-caspian", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38045, "slug": "down-with-disease", "title": "Down with Disease", "position": 11, "duration": 663144, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 0, "mp3_url": "https://phish.in/blob/cb1lvkv50v139xfjr4td9k5zpd7d.mp3", "waveform_image_url": "https://phish.in/blob/w0vu6u6rf2ps5qy7ls9x6voaj9fa.png", "tags": [{ "name": "Unfinished", "description": "Incomplete performance of a composition", "color": "#888888", "priority": 16, "notes": null, "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-03T11:39:32-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "down-with-disease", "title": "Down with Disease", "alias": null, "original": true, "artist": null, "tracks_count": 322, "updated_at": "2021-05-04T09:35:39-04:00", "previous_performance_gap": 3, "previous_performance_slug": "2024-08-29/down-with-disease", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38046, "slug": "ruby-waves", "title": "Ruby Waves", "position": 12, "duration": 988578, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 2, "mp3_url": "https://phish.in/blob/bpbfiv5iaeu3q8x03xi5s5soxff9.mp3", "waveform_image_url": "https://phish.in/blob/auvj30mkjd5wswnhurcty761am8h.png", "tags": [], "updated_at": "2024-11-03T11:39:32-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "ruby-waves", "title": "Ruby Waves", "alias": null, "original": true, "artist": null, "tracks_count": 35, "updated_at": "2021-05-08T21:26:47-04:00", "previous_performance_gap": 2, "previous_performance_slug": "2024-08-30/ruby-waves", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38047, "slug": "fuego", "title": "Fuego", "position": 13, "duration": 1165479, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 2, "mp3_url": "https://phish.in/blob/ts2m2qagj3vhrubuajnjfuvr6mdf.mp3", "waveform_image_url": "https://phish.in/blob/p1kiiitawznrgvtxnaqxglhiijgu.png", "tags": [{ "name": "Jamcharts", "description": "Phish.net Jam Charts selections (phish.net/jamcharts)", "color": "#888888", "priority": 4, "notes": "Continuing their capacity to surprise, Albany's \"context collapse\" comes when, exiting \"Fuego\" proper, Trey directly ignites another creative spark. It's fun to hear a callback to \"Jimmy .... Jimmy ....\" playing as the band follows an impossibly cool, unprogrammed pattern. After the 9:00 mark, Mike offers an idea, and while the improv doesn't change direction, a slew of sonic sounds emerge, bathing the audience in bending musical 'modes' pleasantly atonal, much different from, yet similar to, a \"modern\" Melt. Listen to the run's \"Mercury\" for a completely inverse improvisational approach.", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-25T07:03:46-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "fuego", "title": "Fuego", "alias": null, "original": true, "artist": null, "tracks_count": 87, "updated_at": "2018-12-26T22:40:54-05:00", "previous_performance_gap": 4, "previous_performance_slug": "2024-08-18/fuego", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38048, "slug": "whats-the-use", "title": "What's the Use?", "position": 14, "duration": 248268, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 2, "mp3_url": "https://phish.in/blob/tygmp1zwfr5k050gkant1nrlfv5a.mp3", "waveform_image_url": "https://phish.in/blob/tt7xh98hirg8ewe3q6ncdkn179re.png", "tags": [], "updated_at": "2024-11-03T11:39:34-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "what-s-the-use", "title": "What's the Use?", "alias": null, "original": true, "artist": null, "tracks_count": 67, "updated_at": "2021-05-04T09:35:42-04:00", "previous_performance_gap": 2, "previous_performance_slug": "2024-08-30/whats-the-use", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38049, "slug": "golden-age", "title": "Golden Age", "position": 15, "duration": 678374, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 2, "mp3_url": "https://phish.in/blob/cihej16hep8y0v0ncvnmp3vpg8md.mp3", "waveform_image_url": "https://phish.in/blob/p7dh6vnfr8rmvb6aflho0g7ip53w.png", "tags": [], "updated_at": "2024-11-03T11:39:35-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "golden-age", "title": "Golden Age", "alias": null, "original": false, "artist": "TV On The Radio", "tracks_count": 76, "updated_at": "2024-03-15T15:56:26-04:00", "previous_performance_gap": 4, "previous_performance_slug": "2024-08-18/golden-age", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38050, "slug": "lonely-trip", "title": "Lonely Trip", "position": 16, "duration": 381466, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 1, "mp3_url": "https://phish.in/blob/rnusibxdbi3oanpknidwvp8yf6ze.mp3", "waveform_image_url": "https://phish.in/blob/mg0w25mw3bht7hv7qov52ci14ari.png", "tags": [], "updated_at": "2024-11-03T11:39:36-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "lonely-trip", "title": "Lonely Trip", "alias": null, "original": true, "artist": null, "tracks_count": 21, "updated_at": "2021-08-12T13:25:23-04:00", "previous_performance_gap": 7, "previous_performance_slug": "2024-08-15/lonely-trip", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38051, "slug": "harry-hood", "title": "Harry Hood", "position": 17, "duration": 816379, "jam_starts_at_second": null, "set_name": "Set 2", "likes_count": 2, "mp3_url": "https://phish.in/blob/9hbery4uwbrkpoz9tir6436n7o25.mp3", "waveform_image_url": "https://phish.in/blob/o5pe0ol7hvcbyegzmjb0lulwjvdb.png", "tags": [{ "name": "Banter", "description": "Spoken interaction between band members or audience", "color": "#888888", "priority": 17, "notes": "Trey thanks the crowd", "starts_at_second": 798, "ends_at_second": null, "transcript": "TREY: Thanks everybody, thank you." }, { "name": "Tease", "description": "Transient musical reference", "color": "#888888", "priority": 15, "notes": "The Little Drummer Boy by Katherine Kennicott Davis", "starts_at_second": null, "ends_at_second": null, "transcript": null }], "updated_at": "2024-11-03T11:39:37-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "harry-hood", "title": "Harry Hood", "alias": null, "original": true, "artist": null, "tracks_count": 415, "updated_at": "2021-05-04T09:35:40-04:00", "previous_performance_gap": 3, "previous_performance_slug": "2024-08-29/harry-hood", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38052, "slug": "golgi-apparatus", "title": "Golgi Apparatus", "position": 18, "duration": 285744, "jam_starts_at_second": null, "set_name": "Encore", "likes_count": 0, "mp3_url": "https://phish.in/blob/tntjxc3opbk47rg46pk3yasaeafz.mp3", "waveform_image_url": "https://phish.in/blob/qnqx433zw9t293w6hxhncx733mdt.png", "tags": [], "updated_at": "2024-11-03T11:39:38-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "golgi-apparatus", "title": "Golgi Apparatus", "alias": null, "original": true, "artist": null, "tracks_count": 442, "updated_at": "2021-05-04T09:35:40-04:00", "previous_performance_gap": 3, "previous_performance_slug": "2024-08-29/golgi-apparatus", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }, { "id": 38053, "slug": "slave-to-the-traffic-light", "title": "Slave to the Traffic Light", "position": 19, "duration": 673200, "jam_starts_at_second": null, "set_name": "Encore", "likes_count": 1, "mp3_url": "https://phish.in/blob/9mgqe4963j5863s7zge2slm0p90n.mp3", "waveform_image_url": "https://phish.in/blob/kmr17v1td9wd6jpt4mp0fnrfud45.png", "tags": [{ "name": "Banter", "description": "Spoken interaction between band members or audience", "color": "#888888", "priority": 17, "notes": "Trey thanks the crowd", "starts_at_second": 635, "ends_at_second": null, "transcript": "TREY: Thanks so much everybody, thank you. Tonight was really fun, be safe, we love you, we'll see you tomorrow night. And thank you so much for supporting this beautiful cause, and we'll see you tomorrow night, thank you." }], "updated_at": "2024-11-03T11:39:39-05:00", "show_date": "2024-10-26", "show_cover_art_urls": { "large": "https://phish.in/blob/7bu2albz3x4721yqv8wvnd8m89kd.jpg", "medium": "https://phish.in/blob/6uz33zg54l02pjndscxjaf9ht4w4.jpg", "small": "https://phish.in/blob/nyjwlhhmt2temg1veybjowt6mzxe.jpg" }, "show_album_cover_url": "https://phish.in/blob/tzlb2pzcy07r8mspin8f55srp3bo.jpg", "venue_slug": "times-union-center", "venue_name": "MVP Arena", "venue_location": "Albany, NY", "songs": [{ "slug": "slave-to-the-traffic-light", "title": "Slave to the Traffic Light", "alias": null, "original": true, "artist": null, "tracks_count": 273, "updated_at": "2021-05-04T09:35:41-04:00", "previous_performance_gap": 0, "previous_performance_slug": "2024-09-01/slave-to-the-traffic-light", "next_performance_gap": null, "next_performance_slug": null }], "liked_by_user": false }], "liked_by_user": false, "previous_show_date": "2024-10-25", "next_show_date": "2024-10-27" }];
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
				if (!result.length) return [{ "reviewid": "1379921380", "uid": "82053", "username": "wharf_rat55", "review_text": "Gonna try to keep this as short, sweet, and courteous as possible. \r\n\r\nI was at the whole festival beginning to end and it just amazes me that every night was a solid show. The highlights were N2 and N3. \r\n\r\nThe weather forecasted was awful and was lowkey surprised we even got a show to begin with-my friend and I debated whether they'd cancel Sunday. So the fact we got a show at all was a win. Another thing was, lots of folks got the day set they all complained about wanting-got it-then complained about it!!! Literally wtf lol. \r\n\r\nAll I gotta say is looking at the setlist and having seen it in good company, it was a nice lil cherry on top of a fantastic festival.", "posted_at": "2024-08-19 23:21:13", "score": "3", "showid": "1694536576", "showdate": "2024-08-18", "showyear": "2024", "permalink": "https://phish.net/phish-august-18-2024-the-woodlands-dover-de-usa.html", "artistid": "1", "artist_name": "Phish", "venue": "The Woodlands", "city": "Dover", "state": "DE", "country": "USA" }];
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
