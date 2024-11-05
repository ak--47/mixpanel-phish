import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
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
dayjs.extend(utc);


const attendance = await getAttendance();
const shows = await getShows();
const performances = await getPerformances();
const songBank = await getSongs();
const venues = await getVenues();
const meta = await getMetaData();
const reviews = await getReviews();
const users = await getUsers();
const jamNotes = await getJamNotes();




/**
 * EVENTS
 */

export async function attendanceEvents() {
	const events = [];

	attendance.forEach(u => {
		try {
			const distinct_id = u.uid;
			const username = u.username;
			const show = shows.find(s => s.showid === u.showid) || {};
			const venue = venues.find(v => v.venueid === show.venueid) || {};
			const songsHeard = performances.filter(p => p.showid === u.showid);
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

				latitude: metaData?.venue?.latitude,
				longitude: metaData?.venue?.longitude,
				duration_mins: mins(metaData?.duration),
			};

			events.push(attendanceEvent);

			songsHeard.forEach(s => {
				try {
					const song = songBank.find(sb => sb.songid === s.songid) || {};
					const songMeta = metaData?.tracks?.find(m => m?.slug === song?.slug) || {};
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
						duration_mins: mins(songMeta?.duration),
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

	const events = [];

	reviews.forEach(r => {
		try {
			const time = dayjs.utc(r.date).toISOString();
			const distinct_id = r.uid;
			const username = r.username;
			const show = shows.find(s => s.showid === r.showid) || {};
			const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time) || {};
			const reviewEvent = {
				event: "reviewed show",
				distinct_id,
				username,
				time,
				duration_mins: mins(metaData?.duration),
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

export async function performanceEvents() {

	const events = [];

	performances.forEach(p => {
		try {
			const time = dayjs.utc(p.showdate).toISOString();
			const song = songBank.find(s => s.songid === p.songid) || {};
			const metaData = meta.find(m => dayjs.utc(m?.date).toISOString() === time) || {};
			const trackMeta = metaData?.tracks?.find(m => m?.slug === song?.slug) || {};
			const performanceEvent = {
				event: "song played",
				time,
				show_id: p.showid,
				venue_id: p.venueid,
				song_id: p.songid,
				performance_id: `${p.showid}-${p.set}-${p.position}-${p.songid}`,
				song_name: song.song,
				duration_mins: mins(trackMeta?.duration),
				url: trackMeta?.mp3_url,
			};
			events.push(performanceEvent);
		}
		catch (e) {
			if (NODE_ENV === 'dev') debugger;
		}
	});

	return events;


}

/**
 * PROFILES
 */

export async function phanProfiles() {
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
		const profiles = performances.map(s => {
			const metaData = meta.find(m => m.date === s.showdate) || {};
			const trackMeta = metaData?.tracks?.find(m => m.slug === s.slug) || {};
			const jamNote = jamNotes.find(j => j.showid === s.showid) || {};
			const avatar = meta?.album_cover_url;

			if (Object.keys(jamNote) >= 1) debugger;

			const distinct_id = `${s.showid}-${s.set}-${s.position}-${s.songid}`;
			const topName = `${s.song} (${s.showdate})`;
			const bottomName = `${s.venue} - ${s.city}, ${s.state}`;

			// ! the eras / epoch of phish
			// ? https://www.reddit.com/r/phish/comments/wp9gde/comment/ikfhqrm/
			let era = '';
			const parsedDate = dayjs.utc(s.showdate);
			if (parsedDate.isBefore('2000-10-07')) era = "1.0";
			else if (parsedDate.isBefore('2004-08-15')) era = "2.0";
			else if (parsedDate.isBefore('2020-02-23')) era = "3.0";
			else if (parsedDate.isAfter('2020-02-23')) era = "4.0";
			else era = "unknown";

			// ! interpret the transition mark
			let transition = '';
			switch (s.trans_mark.trim()) {
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
				$city: s.city,
				$country: s.country,
				$region: s.state,
				songid: s.songid,
				tourid: s.tourid,
				$avatar_url: avatar,

				//! dimensions
				position: s.position,
				set: s.set,
				reviews: s.reviews,
				date: s.showdate,
				song: s.song,
				venue: s.venue,
				show_gap: s.gap,
				reviews: s.reviews,

				// ! computed props
				transition,
				era,
				duration_mins: mins(trackMeta?.duration),
				url: trackMeta?.mp3_url,

			};



			return profile;
		});

		return profiles;
	}

	catch (e) {
		if (NODE_ENV === 'dev') debugger;
		throw e;
	}
}

export async function venueProfiles() {
	const profiles = venues.map(v => {
		const distinct_id = v.venueid;
		const topName = v.venuename;
		const bottomName = `${v.city}, ${v.state}`;
		const profile = {
			distinct_id,
			$name: topName,
			$email: bottomName,
			$city: v.city,
			$country: v.country,
			$region: v.state
		};
		return profile;
	});

	return profiles;
}

export async function songProfiles() {
	try {
		const profiles = songBank.map(s => {
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



function mins(ms) {
	if (!ms) return null;
	return u.round(ms / 60000, 2);
}


if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const perfProf = await performanceProfiles();
	const perfEv = await performanceEvents();
	const userProfiles = await phanProfiles();
	const attendEv = await attendanceEvents();
	const revEv = await reviewEvents();
	const songProf = await songProfiles();
	const venProf = await venueProfiles();
	if (NODE_ENV === 'dev') debugger;
}
