import dotenv from 'dotenv';
dotenv.config();
const { NODE_ENV = "" } = process.env;
if (!NODE_ENV) throw new Error("NODE_ENV is required");
import path from 'path';
import fs from 'fs';
import { createWriteStream, createReadStream } from 'fs';
import readline from 'readline';

let TEMP_DIR;
if (NODE_ENV === 'dev') TEMP_DIR = './tmp';
else TEMP_DIR = tmpdir();
TEMP_DIR = path.resolve(TEMP_DIR);



const fileHandles = {}; // Cache for file handles

export async function touch(filename, data, append = false) {
	const filePath = path.join(TEMP_DIR, filename);

	// Ensure all directories in the path exist
	await fs.promises.mkdir(path.dirname(filePath), { recursive: true });

	// Check if a file handle already exists, or create a new one if not
	if (!fileHandles[filePath]) {
		fileHandles[filePath] = fs.createWriteStream(filePath, {
			flags: append ? 'a' : 'w',
			highWaterMark: 128 * 1024 // Adjust based on needs
		});

		// Clean up the file handle on error
		fileHandles[filePath].on('error', (e) => {
			if (NODE_ENV === 'dev') debugger;
			delete fileHandles[filePath]; // Remove from cache on error
			throw e;
		});

		// Optionally close the stream on process exit to release resources
		process.on('exit', () => {
			fileHandles[filePath].end();
		});
	}

	const writeStream = fileHandles[filePath];

	return new Promise((resolve, reject) => {
		// Write each item to the stream
		for (const item of data) {
			if (!writeStream.write(JSON.stringify(item) + '\n')) {
				writeStream.once('drain', () => resolve(filePath));
				return;
			}
		}

		resolve(filePath);
	});
}

// Call this function to close all file handles when they are no longer needed
export function closeAllFileHandles() {
	Object.values(fileHandles).forEach((handle) => {
		if (handle.end) handle.end();
	});

	Object.keys(fileHandles).forEach((key) => {
		delete fileHandles[key];
	});
}
export async function load(filename, limit = 10000) {
	const filePath = path.join(TEMP_DIR, filename);
	const readStream = createReadStream(filePath, { highWaterMark: 64 * 1024 * 2 * 2 });
	const rl = readline.createInterface({
		input: readStream,
		crlfDelay: Infinity,
	});
	
	limit = Infinity;
	
	const data = [];
	let lines = 0;
	const isDev = NODE_ENV === 'dev'; // Check NODE_ENV once

	return new Promise((resolve, reject) => {
		readStream.on('error', reject); // Handle file read errors

		rl.on('line', (line) => {
			try {
				data.push(JSON.parse(line));
			} catch (error) {
				if (NODE_ENV === 'dev') debugger;
				// return reject(new Error(`Failed to parse JSON: ${error.message}`));
			}

			lines++;
			if (isDev && data.length >= limit) {
				rl.close(); // Stop reading if limit is reached in 'dev' mode
			}
		});

		rl.on('close', () => resolve(data));
		rl.on('error', reject); // Handle any readline errors
	});
}
export async function rm(filename) {
	const filePath = join(TEMP_DIR, filename);
	return new Promise((resolve, reject) => {
		unlink(filePath, (err) => {
			if (err) reject(err);
			resolve();
		});
	});
}
