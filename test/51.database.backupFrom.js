'use strict';
const Database = require('../.');
const crypto = require('crypto');

describe('Database#backupFrom()', function () {
	// This tests the synchronous backup API that matches wa-sqlite's sqlite3.backup() pattern.
	// REF: wa-sqlite sqlite-api.js backup(dest, destName, source, sourceName)

	afterEach(function () {
		if (this.dest && this.dest.open) this.dest.close();
		if (this.source && this.source.open) this.source.close();
		if (this.tmp && this.tmp.open) this.tmp.close();
	});

	describe('basic functionality', function () {
		it('should backup from one in-memory database to another', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)').run();
			this.source.prepare('INSERT INTO test (name) VALUES (?)').run('Alice');
			this.source.prepare('INSERT INTO test (name) VALUES (?)').run('Bob');

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const rows = this.dest.prepare('SELECT * FROM test ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, name: 'Alice' },
				{ id: 2, name: 'Bob' }
			]);
		});

		it('should backup from in-memory to file-backed database', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE products (sku TEXT PRIMARY KEY, price REAL)').run();
			this.source.prepare('INSERT INTO products VALUES (?, ?)').run('ABC123', 29.99);
			this.source.prepare('INSERT INTO products VALUES (?, ?)').run('XYZ789', 149.50);

			this.dest = new Database(util.next());
			this.dest.backupFrom(this.source);

			const rows = this.dest.prepare('SELECT * FROM products ORDER BY sku').all();
			expect(rows).to.deep.equal([
				{ sku: 'ABC123', price: 29.99 },
				{ sku: 'XYZ789', price: 149.50 }
			]);

			// Verify persistence after close/reopen
			const dbPath = util.current();
			this.dest.close();
			this.dest = new Database(dbPath);
			const rowsAfterReopen = this.dest.prepare('SELECT * FROM products ORDER BY sku').all();
			expect(rowsAfterReopen).to.deep.equal(rows);
		});

		it('should backup from file-backed to in-memory database', function () {
			this.source = new Database(util.next());
			this.source.prepare('CREATE TABLE data (x INT)').run();
			this.source.prepare('INSERT INTO data VALUES (?)').run(42);
			this.source.prepare('INSERT INTO data VALUES (?)').run(100);

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const rows = this.dest.prepare('SELECT * FROM data ORDER BY x').all();
			expect(rows).to.deep.equal([{ x: 42 }, { x: 100 }]);
		});

		it('should completely replace destination database contents', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE source_table (x INT)').run();
			this.source.prepare('INSERT INTO source_table VALUES (?)').run(42);

			this.dest = new Database(':memory:');
			this.dest.prepare('CREATE TABLE dest_table (y TEXT)').run();
			this.dest.prepare('INSERT INTO dest_table VALUES (?)').run('original');

			this.dest.backupFrom(this.source);

			// Source table should exist
			const sourceRows = this.dest.prepare('SELECT * FROM source_table').all();
			expect(sourceRows).to.deep.equal([{ x: 42 }]);

			// Dest table should NOT exist (database was replaced)
			expect(() => this.dest.prepare('SELECT * FROM dest_table').all())
				.to.throw(Database.SqliteError);
		});

		it('should return the destination database instance for chaining', function () {
			this.source = new Database(':memory:');
			this.dest = new Database(':memory:');
			const result = this.dest.backupFrom(this.source);
			expect(result).to.equal(this.dest);
		});
	});

	describe('wa-sqlite import pattern', function () {
		// These tests match the exact usage pattern from wa-sqlite make-sqlite-db.ts
		// which uses: tmpDb = create, deserialize into tmp, backup from tmp to dest, close tmp

		it('should support the wa-sqlite import pattern: deserialize + backupFrom', function () {
			// Create source data via serialize (simulates getting a snapshot)
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INT)').run();
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(1, 'Widget', 10);
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(2, 'Gadget', 5);
			const snapshot = this.source.serialize();

			// This is the wa-sqlite pattern: create temp, deserialize, backup, close
			this.tmp = new Database(':memory:');
			this.tmp.deserialize(snapshot);

			this.dest = new Database(':memory:');
			this.dest.prepare('CREATE TABLE existing (x INT)').run(); // Will be replaced
			this.dest.backupFrom(this.tmp);
			this.tmp.close();
			this.tmp = null;

			// Verify data was imported
			const rows = this.dest.prepare('SELECT * FROM items ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, name: 'Widget', qty: 10 },
				{ id: 2, name: 'Gadget', qty: 5 }
			]);

			// Verify old data was replaced
			expect(() => this.dest.prepare('SELECT * FROM existing').all())
				.to.throw(Database.SqliteError);
		});

		it('should support the wa-sqlite import pattern with file-backed destination', function () {
			// Simulate a snapshot from another session
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE events (id INTEGER PRIMARY KEY, type TEXT, payload TEXT)').run();
			for (let i = 0; i < 100; i++) {
				this.source.prepare('INSERT INTO events (type, payload) VALUES (?, ?)').run('event', `payload_${i}`);
			}
			const snapshot = this.source.serialize();

			// wa-sqlite pattern with file-backed destination
			this.tmp = new Database(':memory:');
			this.tmp.deserialize(snapshot);

			this.dest = new Database(util.next());
			this.dest.backupFrom(this.tmp);
			this.tmp.close();
			this.tmp = null;

			// Verify and test persistence
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM events').get().c).to.equal(100);

			const dbPath = util.current();
			this.dest.close();
			this.dest = new Database(dbPath);
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM events').get().c).to.equal(100);
		});

		it('should handle multiple sequential imports (wa-sqlite pattern)', function () {
			// First snapshot
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE data (version INT)').run();
			this.source.prepare('INSERT INTO data VALUES (1)').run();
			const snapshot1 = this.source.serialize();

			// Second snapshot (newer state)
			this.source.prepare('UPDATE data SET version = 2').run();
			this.source.prepare('INSERT INTO data VALUES (3)').run();
			const snapshot2 = this.source.serialize();

			this.dest = new Database(':memory:');

			// First import
			this.tmp = new Database(':memory:');
			this.tmp.deserialize(snapshot1);
			this.dest.backupFrom(this.tmp);
			this.tmp.close();

			expect(this.dest.prepare('SELECT * FROM data ORDER BY version').all())
				.to.deep.equal([{ version: 1 }]);

			// Second import (replaces first)
			this.tmp = new Database(':memory:');
			this.tmp.deserialize(snapshot2);
			this.dest.backupFrom(this.tmp);
			this.tmp.close();
			this.tmp = null;

			expect(this.dest.prepare('SELECT * FROM data ORDER BY version').all())
				.to.deep.equal([{ version: 2 }, { version: 3 }]);
		});
	});

	describe('attached databases', function () {
		it('should support custom source database name', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE main_data (x INT)').run();
			this.source.prepare('INSERT INTO main_data VALUES (100)').run();
			this.source.prepare('ATTACH DATABASE ? AS other').run(':memory:');
			this.source.prepare('CREATE TABLE other.other_data (y INT)').run();
			this.source.prepare('INSERT INTO other.other_data VALUES (200)').run();

			this.dest = new Database(':memory:');

			// Backup only from the 'other' attached database
			this.dest.backupFrom(this.source, 'main', 'other');

			// Should have other_data, not main_data
			const rows = this.dest.prepare('SELECT * FROM other_data').all();
			expect(rows).to.deep.equal([{ y: 200 }]);

			expect(() => this.dest.prepare('SELECT * FROM main_data').all())
				.to.throw(Database.SqliteError);
		});

		it('should support custom destination database name', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE data (x INT)').run();
			this.source.prepare('INSERT INTO data VALUES (42)').run();

			this.dest = new Database(':memory:');
			this.dest.prepare('CREATE TABLE main_table (y INT)').run();
			this.dest.prepare('ATTACH DATABASE ? AS other').run(':memory:');

			// Backup to the 'other' attached database
			this.dest.backupFrom(this.source, 'other', 'main');

			// main should still have main_table
			const mainRows = this.dest.prepare('SELECT * FROM main_table').all();
			expect(mainRows).to.deep.equal([]);

			// other should have the backed up data
			const otherRows = this.dest.prepare('SELECT * FROM other.data').all();
			expect(otherRows).to.deep.equal([{ x: 42 }]);
		});
	});

	describe('data integrity', function () {
		it('should preserve all SQLite data types', function () {
			this.source = new Database(':memory:');
			this.source.prepare(`
				CREATE TABLE types_test (
					int_col INTEGER,
					real_col REAL,
					text_col TEXT,
					blob_col BLOB,
					null_col TEXT
				)
			`).run();

			const blobData = Buffer.from([0x00, 0x01, 0x02, 0xff, 0xfe, 0xfd]);
			this.source.prepare('INSERT INTO types_test VALUES (?, ?, ?, ?, ?)').run(
				9007199254740991, // Max safe integer
				3.141592653589793,
				'Hello, World!',
				blobData,
				null
			);

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const row = this.dest.prepare('SELECT * FROM types_test').get();
			expect(row.int_col).to.equal(9007199254740991);
			expect(row.real_col).to.be.closeTo(3.141592653589793, 0.0000000001);
			expect(row.text_col).to.equal('Hello, World!');
			expect(Buffer.compare(row.blob_col, blobData)).to.equal(0);
			expect(row.null_col).to.be.null;
		});

		it('should preserve unicode and special characters', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE unicode_test (text TEXT)').run();

			const testStrings = [
				'Hello 世界 🌍',
				'Ελληνικά',
				'עברית',
				'العربية',
				'日本語',
				'한국어',
				'emoji: 🎉🔥💯🚀',
				'newlines:\n\r\n\ttabs',
				"quotes: 'single' \"double\" `backtick`",
				'null char: \x00 in middle',
			];

			const insert = this.source.prepare('INSERT INTO unicode_test VALUES (?)');
			testStrings.forEach(s => insert.run(s));

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const rows = this.dest.prepare('SELECT * FROM unicode_test').all();
			expect(rows.map(r => r.text)).to.deep.equal(testStrings);
		});

		it('should preserve complex schemas with foreign keys, indexes, triggers', function () {
			this.source = new Database(':memory:');
			this.source.pragma('foreign_keys = ON');

			// Complex schema
			this.source.prepare(`
				CREATE TABLE users (
					id INTEGER PRIMARY KEY,
					name TEXT NOT NULL,
					email TEXT UNIQUE
				)
			`).run();
			this.source.prepare(`
				CREATE TABLE posts (
					id INTEGER PRIMARY KEY,
					user_id INTEGER NOT NULL REFERENCES users(id),
					title TEXT
				)
			`).run();
			this.source.prepare('CREATE INDEX idx_posts_user ON posts(user_id)').run();
			this.source.prepare('CREATE TABLE audit_log (action TEXT, ts TEXT)').run();
			this.source.prepare(`
				CREATE TRIGGER log_post AFTER INSERT ON posts
				BEGIN INSERT INTO audit_log VALUES ('INSERT', datetime('now')); END
			`).run();

			this.source.prepare('INSERT INTO users VALUES (1, ?, ?)').run('Alice', 'alice@test.com');
			this.source.prepare('INSERT INTO posts VALUES (1, 1, ?)').run('First Post');

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);
			this.dest.pragma('foreign_keys = ON');

			// Verify data
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM users').get().c).to.equal(1);
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM posts').get().c).to.equal(1);
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM audit_log').get().c).to.equal(1);

			// Verify trigger works
			this.dest.prepare('INSERT INTO posts VALUES (2, 1, ?)').run('Second Post');
			expect(this.dest.prepare('SELECT COUNT(*) as c FROM audit_log').get().c).to.equal(2);

			// Verify foreign keys work
			expect(() => this.dest.prepare('INSERT INTO posts VALUES (3, 999, ?)').run('Bad'))
				.to.throw(Database.SqliteError);
		});
	});

	describe('large scale data', function () {
		it('should handle 100,000 rows efficiently', function () {
			this.timeout(30000);

			this.source = new Database(':memory:');
			this.source.prepare(`
				CREATE TABLE large_table (
					id INTEGER PRIMARY KEY,
					uuid TEXT,
					data TEXT,
					value REAL
				)
			`).run();

			// Insert 100,000 rows
			const insert = this.source.prepare('INSERT INTO large_table VALUES (?, ?, ?, ?)');
			const insertMany = this.source.transaction((count) => {
				for (let i = 0; i < count; i++) {
					insert.run(i, crypto.randomUUID(), `data_${i}`, Math.random() * 10000);
				}
			});
			insertMany(100000);

			this.dest = new Database(':memory:');
			const startBackup = Date.now();
			this.dest.backupFrom(this.source);
			const backupTime = Date.now() - startBackup;

			// Verify
			const count = this.dest.prepare('SELECT COUNT(*) as c FROM large_table').get().c;
			expect(count).to.equal(100000);

			console.log(`      100K rows backup: ${backupTime}ms`);
			// Backup should be fast (under 500ms for 100K rows)
			expect(backupTime).to.be.below(500);
		});

		it('should handle large blobs (1MB)', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE blob_test (data BLOB)').run();

			const largeBlob = crypto.randomBytes(1024 * 1024);
			this.source.prepare('INSERT INTO blob_test VALUES (?)').run(largeBlob);

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const row = this.dest.prepare('SELECT * FROM blob_test').get();
			expect(Buffer.compare(row.data, largeBlob)).to.equal(0);
		});
	});

	describe('session extension compatibility', function () {
		it('should work with changesets after backupFrom', function () {
			// Create and populate source
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, qty INT)').run();
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(1, 'Widget', 10);
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(2, 'Gadget', 5);

			// Backup to dest
			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			// Create session and make changes on dest
			const session = this.dest.createSession();
			session.attach('items');

			this.dest.prepare('UPDATE items SET qty = ? WHERE id = ?').run(20, 1);
			this.dest.prepare('INSERT INTO items VALUES (?, ?, ?)').run(3, 'Doohickey', 15);

			const changeset = session.changeset();
			session.close();

			expect(changeset).to.be.an.instanceof(Buffer);
			expect(changeset.length).to.be.above(0);

			// Apply changeset to another database with same schema
			const other = new Database(':memory:');
			other.backupFrom(this.source);
			other.applyChangeset(changeset);

			const rows = other.prepare('SELECT * FROM items ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, name: 'Widget', qty: 20 },
				{ id: 2, name: 'Gadget', qty: 5 },
				{ id: 3, name: 'Doohickey', qty: 15 }
			]);

			other.close();
		});

		it('should support invert changeset workflow after backupFrom', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE data (id INTEGER PRIMARY KEY, value INT)').run();
			this.source.prepare('INSERT INTO data VALUES (1, 100)').run();

			this.dest = new Database(':memory:');
			this.dest.backupFrom(this.source);

			const session = this.dest.createSession();
			session.attach('data');

			this.dest.prepare('UPDATE data SET value = ? WHERE id = ?').run(200, 1);

			const changeset = session.changeset();
			session.close();

			// Invert and apply to rollback
			const inverted = this.dest.invertChangeset(changeset);
			this.dest.applyChangeset(inverted);

			const row = this.dest.prepare('SELECT * FROM data').get();
			expect(row).to.deep.equal({ id: 1, value: 100 });
		});
	});

	describe('error handling', function () {
		it('should throw when source is not a Database instance', function () {
			this.dest = new Database(':memory:');
			expect(() => this.dest.backupFrom()).to.throw(TypeError);
			expect(() => this.dest.backupFrom(null)).to.throw(TypeError);
			expect(() => this.dest.backupFrom('not a db')).to.throw(TypeError);
			expect(() => this.dest.backupFrom({})).to.throw(TypeError);
			expect(() => this.dest.backupFrom(123)).to.throw(TypeError);
		});

		it('should throw when source database is closed', function () {
			this.source = new Database(':memory:');
			this.source.close();

			this.dest = new Database(':memory:');
			expect(() => this.dest.backupFrom(this.source)).to.throw(TypeError);
		});

		it('should throw when destination database is closed', function () {
			this.source = new Database(':memory:');
			this.dest = new Database(':memory:');
			this.dest.close();

			expect(() => this.dest.backupFrom(this.source)).to.throw(TypeError);
		});

		it('should throw when destination has active iterators', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE t (x INT)').run();

			this.dest = new Database(':memory:');
			this.dest.prepare('CREATE TABLE t (x INT)').run();
			for (let i = 0; i < 100; i++) {
				this.dest.prepare('INSERT INTO t VALUES (?)').run(i);
			}

			const iterator = this.dest.prepare('SELECT * FROM t').iterate();
			iterator.next();

			expect(() => this.dest.backupFrom(this.source)).to.throw(TypeError);

			iterator.return();
		});

		it('should throw for invalid database name arguments', function () {
			this.source = new Database(':memory:');
			this.dest = new Database(':memory:');

			expect(() => this.dest.backupFrom(this.source, 123)).to.throw(TypeError);
			expect(() => this.dest.backupFrom(this.source, 'main', 123)).to.throw(TypeError);
		});
	});

	describe('roundtrip integrity', function () {
		it('should maintain perfect integrity through backup cycles', function () {
			this.source = new Database(':memory:');
			this.source.prepare(`
				CREATE TABLE test_integrity (
					id INTEGER PRIMARY KEY,
					int_val INTEGER,
					real_val REAL,
					text_val TEXT,
					blob_val BLOB
				)
			`).run();

			// Insert deterministic test data
			const insert = this.source.prepare('INSERT INTO test_integrity VALUES (?, ?, ?, ?, ?)');
			for (let i = 0; i < 1000; i++) {
				const blob = Buffer.alloc(100);
				for (let j = 0; j < 100; j++) blob[j] = (i + j) % 256;
				insert.run(i, i * 1000000, i * 0.123456789, `text_${i}_${'x'.repeat(i % 100)}`, blob);
			}

			// Multiple backup cycles
			let current = this.source;
			for (let cycle = 0; cycle < 5; cycle++) {
				const next = new Database(':memory:');
				next.backupFrom(current);
				if (current !== this.source) current.close();
				current = next;
			}
			this.dest = current;

			// Verify
			const sourceRows = this.source.prepare('SELECT * FROM test_integrity ORDER BY id').all();
			const destRows = this.dest.prepare('SELECT * FROM test_integrity ORDER BY id').all();

			expect(destRows.length).to.equal(sourceRows.length);
			for (let i = 0; i < sourceRows.length; i++) {
				expect(destRows[i].id).to.equal(sourceRows[i].id);
				expect(destRows[i].int_val).to.equal(sourceRows[i].int_val);
				expect(destRows[i].real_val).to.be.closeTo(sourceRows[i].real_val, 0.0000001);
				expect(destRows[i].text_val).to.equal(sourceRows[i].text_val);
				expect(Buffer.compare(destRows[i].blob_val, sourceRows[i].blob_val)).to.equal(0);
			}
		});
	});
});
