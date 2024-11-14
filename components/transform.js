import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import path from 'path';
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);

import u from 'ak-tools';
import {
	getPerformances,
	getAttendance,
	getJamNotes,
	getMetaData,
	getReviews,
	getShows,
	getSongs,
	getVenues,
	getUsers
} from './extract.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import { touch } from "./crud.js";
const { default: ScaleArray } = await import('scale-array');
dayjs.extend(utc);

import { reloadDatabase } from "./duck.js";





export function attendanceEvents(user) {
	if (!LOADED) throw new Error("Data not loaded");
	const events = [];
	const distinct_id = user.uid;
	const username = user.username;
	const show = shows.find(s => s.showid === user.showid) || {};
	const venue = venues.find(v => v.venueid === show.venueid) || {};
	const songsHeard = performances.filter(p => p.showid === user.showid);
	const time = dayjs.utc(show.showdate).toISOString();
	const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time) || {};

	const attendanceEvent = {
		event: "attended show",
		distinct_id,
		username,
		time,

		//group analytics
		show_id: show.showid,
		venue_id: venue.venueid,

		$latitude: metaData?.venue?.latitude,
		$longitude: metaData?.venue?.longitude,
		duration_mins: mins(metaData?.duration),
	};

	events.push(attendanceEvent);

	songsHeard.forEach((s, index) => {
		try {
			const song = songBank.find(sb => sb.songid === s.songid) || {};
			const songMeta = metaData?.tracks?.find(m => m?.slug === song?.slug) || {};
			const newTime = dayjs(time).add((index + 1) * 4.20, 'minute').toISOString(); // lulz
			const songEvent = {
				event: "heard song",
				distinct_id,
				username,
				time: newTime,
				$latitude: metaData?.venue?.latitude,
				$longitude: metaData?.venue?.longitude,
				song_id: s?.songid,
				venue_id: venue?.venueid,
				show_id: show?.showid,
				performance_id: `${show.showid}-${s.set}-${s.position}-${s.songid}`,
				song_name: song?.song,
				duration_mins: mins(songMeta?.duration),
				url: songMeta?.mp3_url,

			};
			events.push(songEvent);
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	});

	return events;
}

export function reviewEvents(reviewsPayload) {
	if (!LOADED) throw new Error("Data not loaded");
	const data = reviewsPayload.data;
	if (data.length === 0) return {};
	const events = data.map(review => {
		const date = review.showdate;
		const time = dayjs.utc(date).toISOString();
		const distinct_id = review.uid;
		const username = review.username;
		const show = shows.find(s => s.showid === review.showid) || {};
		const metaData = meta.find(m => m.date === date) || {};
		const reviewEvent = {
			event: "reviewed show",
			distinct_id,
			username,
			time,
			duration_mins: mins(metaData?.duration),
			show_id: review.showid,
			venue_id: show.venueid,
			review_id: review.reviewid,
			rating: review.score,
			review: review.review_text,
			$latitude: metaData?.venue?.latitude,
			$longitude: metaData?.venue?.longitude,

		};
		return reviewEvent;
	});
	return events;

}


export function performanceEvents(perf) {
	if (!LOADED) throw new Error("Data not loaded");
	const time = dayjs.utc(perf.showdate).toISOString();
	const song = songBank.find(s => s.songid === perf.songid) || {};
	const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time) || {};
	const trackMeta = metaData?.tracks?.find(m => m?.slug === song?.slug) || {};
	const performanceEvent = {
		event: "song played",
		time,
		show_id: perf.showid,
		venue_id: perf.venueid,
		song_id: perf.songid,
		performance_id: `${perf.showid}-${perf.set}-${perf.position}-${perf.songid}`,
		song_name: song.song,
		duration_mins: mins(trackMeta?.duration),
		url: trackMeta?.mp3_url,
	};
	return performanceEvent;
}


export function phanProfiles(user) {
	if (!LOADED) throw new Error("Data not loaded");
	const distinct_id = user.uid;
	const name = user.username;
	const email = `since ${dayjs(user.date_joined).format('YYYY')}`;
	const created = dayjs.utc(user.date_joined).toISOString();
	return {
		name,
		distinct_id,
		email,
		created,
		...user
	};
}



export function performanceProfiles(perf) {
	if (!LOADED) throw new Error("Data not loaded");
	const metaData = meta.find(m => m.date === perf.showdate) || {};
	const trackMeta = metaData?.tracks?.find(m => m.slug === perf.slug) || {};
	const jamNote = jamNotes.find(j => j.showid === perf.showid) || {};
	const avatar = metaData?.album_cover_url;

	if (Object.keys(jamNote) >= 1) debugger;

	const distinct_id = `${perf.showid}-${perf.set}-${perf.position}-${perf.songid}`;
	const topName = `${perf.song} (${perf.showdate})`;
	const bottomName = `${perf.venue} - ${perf.city}, ${perf.state}`;

	// ! the eras / epoch of phish
	// ? https://www.reddit.com/r/phish/comments/wp9gde/comment/ikfhqrm/
	let era = '';
	const parsedDate = dayjs.utc(perf.showdate);
	if (parsedDate.isBefore('2000-10-07')) era = "1.0";
	else if (parsedDate.isBefore('2004-08-15')) era = "2.0";
	else if (parsedDate.isBefore('2020-02-23')) era = "3.0";
	else if (parsedDate.isAfter('2020-02-23')) era = "4.0";
	else era = "unknown";

	// ! interpret the transition mark
	let transition = '';
	switch (p.trans_mark.trim()) {
		case ',':
			transition = 'pause (,)';
			break;
		case '>':
			transition = 'segue (>)';
			break;
		case '->':
			transition = 'jam (->)';
			break;
		case '':
			transition = 'closer (.)';
			break;

	}

	const profile = {

		//! ID Props
		distinct_id,
		$name: topName,
		$email: bottomName,
		$city: perf.city,
		$country: perf.country,
		$region: perf.state,
		songid: perf.songid,
		tourid: perf.tourid,
		$avatar: avatar,

		//! dimensions
		position: perf.position,
		set: perf.set,
		reviews: perf.reviews,
		date: perf.showdate,
		song: perf.song,
		venue: perf.venue,
		show_gap: perf.gap,
		reviews: perf.reviews,

		// ! computed props
		transition,
		era,
		duration_mins: mins(trackMeta?.duration),
		url: trackMeta?.mp3_url,

	};

	return profile;
}

export function venueProfiles(venue) {
	if (!LOADED) throw new Error("Data not loaded");
	const distinct_id = venue.venueid;
	const topName = venue.venuename;
	const bottomName = `${venue.city}, ${venue.state}`;
	const profile = {
		distinct_id,
		$name: topName,
		$email: bottomName,
		$city: venue.city,
		$country: venue.country,
		$region: venue.state
	};
	return profile;
}


export function songProfiles(song) {
	const distinct_id = song.songid;
	const topName = song.song;
	const bottomName = song.artist;
	const profile = {
		distinct_id,
		$name: topName,
		$email: bottomName,
		debut: song.debut,
		times_played: song.times_played,
		last_played: song.last_played
	};
	return profile;

}


export function showProfiles(show) {
	if (!LOADED) throw new Error("Data not loaded");
	const metaData = meta.find(m => m.date === show.showdate) || {};
	const jamNote = jamNotes.find(j => j.showid === show.showid) || {};
	const distinct_id = show.showid;
	const bottomName = dayjs(show.showdate).format('DD/MM/YYYY');
	const topName = `${show.venue} - ${show.city}, ${show.state}`;
	const avatar = metaData?.album_cover_url;
	const duration_mins = mins(metaData?.duration);

	const profile = {
		distinct_id,
		$name: topName,
		$email: bottomName,
		$city: show.city,
		$country: show.country,
		$region: show.state,
		$avatar: avatar,
		duration_mins,
		num_songs: metaData?.tracks?.length,
		likes: meta?.likes,

	};
	return profile;
}


function jsonFlattener(sep = ".") {
	function flatPropertiesRecurse(properties, roots = []) {
		return Object.keys(properties)
			.reduce((memo, prop) => {
				// Check if the property is an object but not an array
				const isObjectNotArray = properties[prop] !== null
					&& typeof properties[prop] === 'object'
					&& !Array.isArray(properties[prop]);

				return Object.assign({}, memo,
					isObjectNotArray
						? flatPropertiesRecurse(properties[prop], roots.concat([prop]))
						: { [roots.concat([prop]).join(sep)]: properties[prop] }
				);
			}, {});
	}

	return function (record) {
		if (record.properties && typeof record.properties === 'object') {
			record.properties = flatPropertiesRecurse(record.properties);
			return record;
		}

		if (record.$set && typeof record.$set === 'object') {
			record.$set = flatPropertiesRecurse(record.$set);
			return record;

		}

		return {};


	};
}


function mins(ms) {
	if (!ms) return null;
	return u.round(ms / 60000, 2);
}


if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	await reloadDatabase();
}
