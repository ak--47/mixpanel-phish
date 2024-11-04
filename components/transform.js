import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import u from 'ak-tools';
import {
	getPerformances,
	getAttendance,
	getJamNotes,
	getPerformanceMeta,
	getReviews,
	getShows,
	getSongs,
	getVenues,
	getUsers
} from './extract.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
dayjs.extend(utc);

/**
 * EVENTS
 */

export async function attendanceEvents() {

	const attendance = await getAttendance();
	const shows = await getShows();
	const performances = await getPerformances();
	const songBank = await getSongs();
	const venues = await getVenues();
	const meta = await getPerformanceMeta();
	const events = [];

	attendance.forEach(u => {
		try {
			const distinct_id = u.uid;
			const username = u.username;
			const show = shows.find(s => s.showid === u.showid);
			const venue = venues.find(v => v.venueid === show.venueid);
			const songsHeard = performances.filter(p => p.showid === u.showid);
			const time = dayjs.utc(show.showdate).toISOString();
			const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time);

			const attendanceEvent = {
				event: "attended show",
				distinct_id,
				username,
				time,

				//group analytics
				show_id: show.showid,
				venue_id: venue.venueid,

				latitude: metaData?.venue?.latitude,
				longitude: metaData?.venue?.longitude,
				duration_mins: Math.floor(metaData?.duration / 60000),
			};

			events.push(attendanceEvent);

			songsHeard.forEach(s => {
				try {
					const song = songBank.find(sb => sb.songid === s.songid);
					const songMeta = metaData?.tracks?.find(m => m?.slug === song?.slug);
					const songEvent = {
						event: "heard song",
						distinct_id,
						username,
						time,

						song_id: s?.songid,
						venue_id: venue?.venueid,
						show_id: show?.showid,
						performance_id: `${show.showid}-${s.set}-${s.position}-${s.songid}`,
						song_name: song?.song,
						duration_mins: Math.floor(songMeta?.duration / 60000),
						url: songMeta?.mp3_url,

					};
					events.push(songEvent);
				}
				catch (e) {
					if (NODE_ENV === 'dev') debugger;
				}
			});
		}

		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	});

	return events;
}

export async function reviewEvents() {
	const reviews = await getReviews();
	const shows = await getShows();
	const venues = await getVenues();
	const meta = await getPerformanceMeta();

	const events = [];

	reviews.forEach(r => {
		try {
			const time = dayjs.utc(r.date).toISOString();
			const distinct_id = r.uid;
			const username = r.username;
			const show = shows.find(s => s.showid === r.showid);
			const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time);
			const reviewEvent = {
				event: "reviewed show",
				distinct_id,
				username,
				time,
				duration_mins: Math.floor(metaData?.duration / 60000),
				show_id: r.showid,
				venue_id: show.venueid,
				review_id: r.review_id,
				rating: r.score,
				review: r.text,
				latitude: metaData?.venue?.latitude,
				longitude: metaData?.venue?.longitude,

			};
			events.push(reviewEvent);
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	});
	return events;
}

export async function performanceEvents() { }




/**
 * PROFILES
 */

export async function phanProfiles() {

	const users = await getUsers();
	const modeledUsers = users.map(u => {
		try {
			const distinct_id = u.uid;
			const email = u.username;
			const created = dayjs.utc(u.date_joined).toISOString();
			return {
				distinct_id,
				email,
				created,
				...u
			};
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
			return {};
		}
	});
	return modeledUsers;

}

export async function performanceProfiles() {
	try {
		const songs = await getPerformances();
		const profiles = songs.map(s => {
			// ! the songId is a unique identifier for each song
			const distinct_id = u.md5(`${s.showdate}-${s.slug}-${s.set}-${s.position}`);

			// ! these are special fields for mixpanel display
			const topName = `${s.song} (${s.showdate})`;
			const bottomName = `${s.venue} - ${s.city}, ${s.state}`;

			// ! the eras / epoch of phish
			// ? https://www.reddit.com/r/phish/comments/wp9gde/comment/ikfhqrm/
			let ERA = '';
			const parsedDate = dayjs.utc(s.showdate);
			if (parsedDate.isBefore('2000-10-07')) ERA = "1.0";
			else if (parsedDate.isBefore('2004-08-15')) ERA = "2.0";
			else if (parsedDate.isBefore('2020-02-23')) ERA = "3.0";
			else if (parsedDate.isAfter('2020-02-23')) ERA = "4.0";
			else ERA = "unknown";

			// ! interpret the transition mark
			let TRANSITION = '';
			switch (s.trans_mark.trim()) {
				case ',':
					TRANSITION = 'pause';
					break;
				case '>':
					TRANSITION = 'segue';
					break;
				case '->':
					TRANSITION = 'jam';
					break;
				case '':
					TRANSITION = 'closer';
					break;

			}

			const profile = {

				//! ID Props
				distinct_id,
				$name: topName,
				$email: bottomName,
				$city: s.city,
				$country: s.country,
				$region: s.state,
				songid: s.songid,
				tourid: s.tourid,

				//! dimensions
				POSITION: s.position,
				SET: s.set,
				REVIEWS: s.reviews,
				DATE: s.showdate,
				SONG: s.song,
				VENUE: s.venue,
				SHOW_GAP: s.gap,
				REVIEWS: s.reviews,

				// ! computed props
				TRANSITION,
				ERA,

			};



			profile.TRANSITION = TRANSITION;

			return profile;
		});

		return profiles;
	}

	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

export async function venueProfiles() { }

export async function songProfiles() {
	try {
		const songs = await getSongs();
		const profiles = songs.map(s => {
			const distinct_id = s.songid;
			const topName = s.song;
			const bottomName = s.artist;
			const profile = {
				distinct_id,
				$name: topName,
				$email: bottomName,


				DEBUT: s.debut,
				TIMES_PLAYED: s.times_played,
				LAST_PLAYED: s.last_played
			};
			return profile;
		});
		return profiles;
	} catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}






if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const p = await performanceProfiles();
	const u = await phanProfiles();
	const a = await attendanceEvents();
	const r = await reviewEvents();
	const s = await songProfiles();
	const pp = await performanceEvents();
	if (NODE_ENV === 'dev') debugger;
}
