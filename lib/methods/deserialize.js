'use strict';
const { cppdb } = require('../util');

/**
 * Deserialize a buffer into the database.
 *
 * Uses the safe pattern (matches wa-sqlite's import pattern):
 * 1. Create temp in-memory database
 * 2. Deserialize buffer into temp
 * 3. Backup from temp to current database
 * 4. Close temp database
 *
 * This preserves file-based persistence and works with both in-memory and file databases.
 *
 * @param {Buffer} buffer - The serialized database buffer
 * @param {Object} [options] - Options object
 * @param {string} [options.attached='main'] - The attached database name
 * @returns {this} The database instance for chaining
 */
module.exports = function deserialize(buffer, options) {
	if (options == null) options = {};

	// Validate arguments
	if (!Buffer.isBuffer(buffer)) throw new TypeError('Expected first argument to be a buffer');
	if (typeof options !== 'object') throw new TypeError('Expected second argument to be an options object');

	// Interpret and validate options
	const attachedName = 'attached' in options ? options.attached : 'main';
	if (typeof attachedName !== 'string') throw new TypeError('Expected the "attached" option to be a string');
	if (!attachedName) throw new TypeError('The "attached" option cannot be an empty string');

	this[cppdb].deserialize(buffer, attachedName);
	return this; // Return the JS Database instance for chaining
};
