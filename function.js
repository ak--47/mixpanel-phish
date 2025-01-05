import { http } from '@google-cloud/functions-framework';
// import { Storage } from '@google-cloud/storage';
import { tmpdir } from 'os';
import { sLog, uid, timer, ls, rm } from 'ak-tools';
import dotenv from 'dotenv';
import extract from "./components/extract.js";
import transform from "./components/transform.js";
import load from "./components/load.js";
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc.js';
import path from 'path';
import fs from "fs/promises";
dayjs.extend(utc);
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);

/**
 * @typedef {Object} Params
 * @property {string} [date] - start date for processing
 */

/** @typedef {'/'} Endpoints  */

// ? https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Errors/BigInt_not_serializable
BigInt.prototype.toJSON = function () {
	return Number(this);
};

// http entry point
// ? https://cloud.google.com/functions/docs/writing/write-http-functions
http('entry', async (req, res) => {
	const runId = uid();
	const reqData = { url: req.url, method: req.method, headers: req.headers, body: req.body, query: req.query, runId };
	delete reqData.headers.authorization;
	let response = {};

	try {
		/** @type {Params} */
		const { body = {} } = req;
		/** @type {Endpoints} */
		const { path } = req;

		for (const key in req.query || {}) {
			let value = req.query[key];
			if (value === 'true') value = true;
			if (value === 'false') value = false;
			if (value === 'null') value = null;
			body[key] = value;
		}

		const t = timer('job');
		t.start();
		sLog(`[MP-PHISH] REQ: ${req.path}`, reqData);

		//setup the job
		const [job] = route(path);

		// @ts-ignore
		const result = await job(body);
		t.end();
		sLog(`[MP-PHISH] RESP: ${req.path} ... ${t.report(false).human}`, result);

		//finished
		res.status(200);
		response = result;


	} catch (e) {
		console.error(`[MP-PHISH] ERROR: ${req.path}`, e);
		res.status(500);
		response = { error: e };
	}
	res.send(JSON.stringify(response));
});


async function main(data) {
	let date;
	try {
		const parsedDate = dayjs.utc(data.date || "invalid date");
		if (parsedDate.isValid()) {
			date = parsedDate.format('YYYY-MM-DD');
		} else {
			date = dayjs().utc().subtract(7, 'd').format('YYYY-MM-DD');
		}
	} catch (e) {
		date = dayjs().utc().subtract(7, 'd').format('YYYY-MM-DD');
	}
	const { sendEvents = true, sendProfiles = true, sendAnnotations = true } = data || {};
	sLog(`[MP-PHISH] GO: ${date}`, data);

	const extracted = await extract(date);
	const transformed = await transform();
	const loaded = await load({ sendEvents, sendProfiles, sendAnnotations });
	// cleanup
	const files = await ls(TEMP_DIR);
	for (const file of files) {
		await rm(file, false, false);
	}

	return { date, extracted, transformed, loaded, files };

}


/*
----
ROUTER
----
*/

/**
 * determine routes based on path in request
 * @param  {Endpoints} path
 */
function route(path) {
	switch (path) {
		case "/":
			return [main];
		default:
			return [main];
	}
}

