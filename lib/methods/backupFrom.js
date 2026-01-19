'use strict';
const { cppdb } = require('../util');

/**
 * Synchronous backup from another database to this database.
 * Matches wa-sqlite's sqlite3.backup(dest, destName, source, sourceName) pattern.
 *
 * @param {Database} sourceDb - The source Database instance to backup from
 * @param {string} [destName='main'] - The attached database name for destination
 * @param {string} [srcName='main'] - The attached database name for source
 * @returns {this} The database instance for chaining
 */
module.exports = function backupFrom(sourceDb, destName, srcName) {
	// Validate sourceDb is a Database instance (must have cppdb symbol)
	if (sourceDb == null || typeof sourceDb !== 'object' || !sourceDb[cppdb]) {
		throw new TypeError('Expected first argument to be a Database instance');
	}

	// Validate optional string arguments
	if (destName !== undefined && typeof destName !== 'string') {
		throw new TypeError('Expected second argument to be a string');
	}
	if (srcName !== undefined && typeof srcName !== 'string') {
		throw new TypeError('Expected third argument to be a string');
	}

	// Pass the native cppdb object from source, not the JS wrapper
	// The C++ side unwraps it to get the Database pointer
	this[cppdb].backupFrom(sourceDb[cppdb], destName, srcName);
	return this;
};
