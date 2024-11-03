import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import u from 'ak-tools';
import { getPerformances } from './extract.js';
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
dayjs.extend(utc);

export async function songsToGroupProfiles() {
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
					TRANSITION = 'closer'
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


if (import.meta.url === new URL(`file://${process.argv[1]}`).href) {
	const p = await songsToGroupProfiles();
	const uniq = Array.from(new Set(p.map(a => a.ERA)))
	if (NODE_ENV === 'dev') debugger;
}
