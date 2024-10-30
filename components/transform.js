import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import u from 'ak-tools';
import { getSetlists } from './extract.js';

export async function songsToGroupProfiles() {
	try {
		const songs = (await getSetlists()).flat();
		const profiles = songs.map(s => {
			const songId = u.md5(`${s.showdate}-${s.slug}-${s.set}-${s.position}`);
			const topName = `${s.song} (${s.showdate})`;
			const bottomName = `${s.venue} - ${s.city}, ${s.state}`;
			const profile = {
				distinct_id: songId,
				$name: topName,
				$email: bottomName,
				$city: s.city,
				$country: s.country,
				$region: s.state,

				//! important props!
				POSITION: s.position,
				SET: s.set,
				REVIEWS: s.reviews,
				DATE: s.showdate,
				SONG: s.song,
				VENUE: s.venue,
				SHOW_GAP: s.gap,
				REVIEWS: s.reviews,



				// ! lookup props
				songid: s.songid,
				tourid: s.tourid,
			};

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
	const profiles = await songsToGroupProfiles();
	if (NODE_ENV === 'dev') debugger;
}
