'use strict';
const Database = require('../.');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

describe('Database#deserialize()', function () {
	afterEach(function () {
		if (this.db && this.db.open) this.db.close();
		if (this.source && this.source.open) this.source.close();
		if (this.target && this.target.open) this.target.close();
	});

	describe('basic functionality', function () {
		it('should deserialize a buffer into an in-memory database', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)').run();
			this.source.prepare('INSERT INTO test (name) VALUES (?)').run('Alice');
			this.source.prepare('INSERT INTO test (name) VALUES (?)').run('Bob');

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const rows = this.target.prepare('SELECT * FROM test ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, name: 'Alice' },
				{ id: 2, name: 'Bob' }
			]);
		});

		it('should deserialize a buffer into a file-backed database', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE products (sku TEXT PRIMARY KEY, price REAL)').run();
			this.source.prepare('INSERT INTO products VALUES (?, ?)').run('ABC123', 29.99);
			this.source.prepare('INSERT INTO products VALUES (?, ?)').run('XYZ789', 149.50);

			const buffer = this.source.serialize();

			this.target = new Database(util.next());
			this.target.deserialize(buffer);

			const rows = this.target.prepare('SELECT * FROM products ORDER BY sku').all();
			expect(rows).to.deep.equal([
				{ sku: 'ABC123', price: 29.99 },
				{ sku: 'XYZ789', price: 149.50 }
			]);

			// Verify persistence after close/reopen
			const dbPath = util.current();
			this.target.close();
			this.target = new Database(dbPath);
			const rowsAfterReopen = this.target.prepare('SELECT * FROM products ORDER BY sku').all();
			expect(rowsAfterReopen).to.deep.equal(rows);
		});

		it('should completely replace the target database contents', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE source_table (x INT)').run();
			this.source.prepare('INSERT INTO source_table VALUES (?)').run(42);

			this.target = new Database(':memory:');
			this.target.prepare('CREATE TABLE target_table (y TEXT)').run();
			this.target.prepare('INSERT INTO target_table VALUES (?)').run('original');

			const buffer = this.source.serialize();
			this.target.deserialize(buffer);

			// Source table should exist
			const sourceRows = this.target.prepare('SELECT * FROM source_table').all();
			expect(sourceRows).to.deep.equal([{ x: 42 }]);

			// Target table should NOT exist (database was replaced)
			expect(() => this.target.prepare('SELECT * FROM target_table').all())
				.to.throw(Database.SqliteError);
		});

		it('should return the database instance for chaining', function () {
			this.source = new Database(':memory:');
			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			const result = this.target.deserialize(buffer);
			expect(result).to.equal(this.target);
		});

		it('should accept the "attached" option', function () {
			// Create source with data
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE data (val INT)').run();
			this.source.prepare('INSERT INTO data VALUES (?)').run(100);
			const buffer = this.source.serialize();

			// Create target with attached database
			this.target = new Database(':memory:');
			this.target.prepare('CREATE TABLE main_data (x INT)').run();
			this.target.prepare('ATTACH DATABASE ? AS other').run(':memory:');

			// Deserialize into the attached database
			this.target.deserialize(buffer, { attached: 'other' });

			// Verify main is unchanged and other has the data
			const mainRows = this.target.prepare('SELECT * FROM main_data').all();
			expect(mainRows).to.deep.equal([]);

			const otherRows = this.target.prepare('SELECT * FROM other.data').all();
			expect(otherRows).to.deep.equal([{ val: 100 }]);
		});
	});

	describe('data type handling', function () {
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

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const row = this.target.prepare('SELECT * FROM types_test').get();
			expect(row.int_col).to.equal(9007199254740991);
			expect(row.real_col).to.be.closeTo(3.141592653589793, 0.0000000001);
			expect(row.text_col).to.equal('Hello, World!');
			expect(Buffer.compare(row.blob_col, blobData)).to.equal(0);
			expect(row.null_col).to.be.null;
		});

		it('should handle unicode and special characters', function () {
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
				'', // empty string
			];

			const insert = this.source.prepare('INSERT INTO unicode_test VALUES (?)');
			testStrings.forEach(s => insert.run(s));

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const rows = this.target.prepare('SELECT * FROM unicode_test').all();
			expect(rows.map(r => r.text)).to.deep.equal(testStrings);
		});

		it('should handle large blobs', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE blob_test (data BLOB)').run();

			// 1MB random blob
			const largeBlob = crypto.randomBytes(1024 * 1024);
			this.source.prepare('INSERT INTO blob_test VALUES (?)').run(largeBlob);

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const row = this.target.prepare('SELECT * FROM blob_test').get();
			expect(Buffer.compare(row.data, largeBlob)).to.equal(0);
		});
	});

	describe('complex schemas', function () {
		it('should preserve multiple tables with foreign keys', function () {
			this.source = new Database(':memory:');
			this.source.pragma('foreign_keys = ON');
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
					title TEXT,
					created_at TEXT DEFAULT CURRENT_TIMESTAMP
				)
			`).run();
			this.source.prepare(`
				CREATE TABLE comments (
					id INTEGER PRIMARY KEY,
					post_id INTEGER NOT NULL REFERENCES posts(id),
					user_id INTEGER NOT NULL REFERENCES users(id),
					body TEXT
				)
			`).run();

			this.source.prepare('INSERT INTO users VALUES (?, ?, ?)').run(1, 'Alice', 'alice@example.com');
			this.source.prepare('INSERT INTO users VALUES (?, ?, ?)').run(2, 'Bob', 'bob@example.com');
			this.source.prepare('INSERT INTO posts VALUES (?, ?, ?, ?)').run(1, 1, 'First Post', '2024-01-01');
			this.source.prepare('INSERT INTO comments VALUES (?, ?, ?, ?)').run(1, 1, 2, 'Great post!');

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);
			this.target.pragma('foreign_keys = ON');

			// Verify all data
			expect(this.target.prepare('SELECT COUNT(*) as c FROM users').get().c).to.equal(2);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM posts').get().c).to.equal(1);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM comments').get().c).to.equal(1);

			// Verify foreign keys work
			expect(() => this.target.prepare('INSERT INTO posts VALUES (?, ?, ?, ?)').run(2, 999, 'Bad', '2024-01-01'))
				.to.throw(Database.SqliteError);
		});

		it('should preserve indexes', function () {
			this.source = new Database(':memory:');
			this.source.prepare(`
				CREATE TABLE indexed_data (
					id INTEGER PRIMARY KEY,
					category TEXT,
					value REAL
				)
			`).run();
			this.source.prepare('CREATE INDEX idx_category ON indexed_data(category)').run();
			this.source.prepare('CREATE INDEX idx_value ON indexed_data(value DESC)').run();
			this.source.prepare('CREATE UNIQUE INDEX idx_cat_val ON indexed_data(category, value)').run();

			// Insert enough data to make indexes meaningful
			const insert = this.source.prepare('INSERT INTO indexed_data VALUES (?, ?, ?)');
			for (let i = 0; i < 1000; i++) {
				insert.run(i, `cat${i % 10}`, Math.random() * 1000);
			}

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			// Verify indexes exist
			const indexes = this.target.prepare(
				"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='indexed_data' ORDER BY name"
			).all();
			expect(indexes.map(i => i.name)).to.include.members(['idx_category', 'idx_value', 'idx_cat_val']);

			// Verify data
			expect(this.target.prepare('SELECT COUNT(*) as c FROM indexed_data').get().c).to.equal(1000);
		});

		it('should preserve triggers', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE audit_log (action TEXT, timestamp TEXT)').run();
			this.source.prepare('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)').run();
			this.source.prepare(`
				CREATE TRIGGER log_insert AFTER INSERT ON items
				BEGIN
					INSERT INTO audit_log VALUES ('INSERT', datetime('now'));
				END
			`).run();

			this.source.prepare('INSERT INTO items (name) VALUES (?)').run('test');
			expect(this.source.prepare('SELECT COUNT(*) as c FROM audit_log').get().c).to.equal(1);

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			// Verify trigger exists and works
			this.target.prepare('INSERT INTO items (name) VALUES (?)').run('new item');
			expect(this.target.prepare('SELECT COUNT(*) as c FROM audit_log').get().c).to.equal(2);
		});

		it('should preserve views', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL, status TEXT)').run();
			this.source.prepare('CREATE VIEW pending_orders AS SELECT * FROM orders WHERE status = \'pending\'').run();
			this.source.prepare('CREATE VIEW order_summary AS SELECT customer, SUM(amount) as total FROM orders GROUP BY customer').run();

			this.source.prepare('INSERT INTO orders VALUES (?, ?, ?, ?)').run(1, 'Alice', 100.00, 'pending');
			this.source.prepare('INSERT INTO orders VALUES (?, ?, ?, ?)').run(2, 'Alice', 50.00, 'complete');
			this.source.prepare('INSERT INTO orders VALUES (?, ?, ?, ?)').run(3, 'Bob', 75.00, 'pending');

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			// Verify views work
			const pending = this.target.prepare('SELECT * FROM pending_orders ORDER BY id').all();
			expect(pending).to.have.length(2);

			const summary = this.target.prepare('SELECT * FROM order_summary ORDER BY customer').all();
			expect(summary).to.deep.equal([
				{ customer: 'Alice', total: 150.00 },
				{ customer: 'Bob', total: 75.00 }
			]);
		});
	});

	describe('large scale data', function () {
		it('should handle databases with 100,000 rows efficiently', function () {
			this.timeout(30000); // Allow 30 seconds

			this.source = new Database(':memory:');
			this.source.prepare(`
				CREATE TABLE large_table (
					id INTEGER PRIMARY KEY,
					uuid TEXT,
					data TEXT,
					value REAL
				)
			`).run();

			// Insert 100,000 rows in batches
			const insert = this.source.prepare('INSERT INTO large_table VALUES (?, ?, ?, ?)');
			const insertMany = this.source.transaction((count) => {
				for (let i = 0; i < count; i++) {
					insert.run(i, crypto.randomUUID(), `data_${i}`, Math.random() * 10000);
				}
			});
			insertMany(100000);

			const sourceCount = this.source.prepare('SELECT COUNT(*) as c FROM large_table').get().c;
			expect(sourceCount).to.equal(100000);

			// Serialize
			const startSerialize = Date.now();
			const buffer = this.source.serialize();
			const serializeTime = Date.now() - startSerialize;

			// Deserialize
			this.target = new Database(':memory:');
			const startDeserialize = Date.now();
			this.target.deserialize(buffer);
			const deserializeTime = Date.now() - startDeserialize;

			// Verify
			const targetCount = this.target.prepare('SELECT COUNT(*) as c FROM large_table').get().c;
			expect(targetCount).to.equal(100000);

			// Log performance (informational)
			console.log(`      100K rows: serialize=${serializeTime}ms, deserialize=${deserializeTime}ms, buffer=${(buffer.length/1024/1024).toFixed(2)}MB`);

			// Deserialize should be fast (under 1 second for 100K rows)
			expect(deserializeTime).to.be.below(1000);
		});

		it('should handle multiple tables with complex relationships', function () {
			this.timeout(10000);

			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)').run();
			this.source.prepare('CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, salary REAL)').run();
			this.source.prepare('CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, budget REAL)').run();
			this.source.prepare('CREATE TABLE employee_projects (employee_id INTEGER, project_id INTEGER, role TEXT)').run();
			this.source.prepare('CREATE TABLE timesheets (id INTEGER PRIMARY KEY, employee_id INTEGER, project_id INTEGER, hours REAL, date TEXT)').run();

			// Populate with realistic data
			const insertDept = this.source.prepare('INSERT INTO departments VALUES (?, ?)');
			const insertEmp = this.source.prepare('INSERT INTO employees VALUES (?, ?, ?, ?)');
			const insertProj = this.source.prepare('INSERT INTO projects VALUES (?, ?, ?)');
			const insertEmpProj = this.source.prepare('INSERT INTO employee_projects VALUES (?, ?, ?)');
			const insertTime = this.source.prepare('INSERT INTO timesheets VALUES (?, ?, ?, ?, ?)');

			this.source.transaction(() => {
				// 10 departments
				for (let d = 0; d < 10; d++) {
					insertDept.run(d, `Department ${d}`);
				}
				// 1000 employees
				for (let e = 0; e < 1000; e++) {
					insertEmp.run(e, e % 10, `Employee ${e}`, 50000 + Math.random() * 100000);
				}
				// 100 projects
				for (let p = 0; p < 100; p++) {
					insertProj.run(p, `Project ${p}`, 100000 + Math.random() * 1000000);
				}
				// 5000 employee-project assignments
				for (let ep = 0; ep < 5000; ep++) {
					insertEmpProj.run(ep % 1000, ep % 100, ['Developer', 'Manager', 'QA', 'Designer'][ep % 4]);
				}
				// 50000 timesheet entries
				for (let t = 0; t < 50000; t++) {
					insertTime.run(t, t % 1000, t % 100, Math.random() * 8, '2024-01-01');
				}
			})();

			const buffer = this.source.serialize();

			this.target = new Database(':memory:');
			const startDeserialize = Date.now();
			this.target.deserialize(buffer);
			const deserializeTime = Date.now() - startDeserialize;

			// Verify all tables
			expect(this.target.prepare('SELECT COUNT(*) as c FROM departments').get().c).to.equal(10);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM employees').get().c).to.equal(1000);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM projects').get().c).to.equal(100);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM employee_projects').get().c).to.equal(5000);
			expect(this.target.prepare('SELECT COUNT(*) as c FROM timesheets').get().c).to.equal(50000);

			console.log(`      Complex schema: deserialize=${deserializeTime}ms, buffer=${(buffer.length/1024/1024).toFixed(2)}MB`);
		});
	});

	describe('error handling', function () {
		it('should throw when given a non-buffer argument', function () {
			this.db = new Database(':memory:');
			expect(() => this.db.deserialize('not a buffer')).to.throw(TypeError);
			expect(() => this.db.deserialize(12345)).to.throw(TypeError);
			expect(() => this.db.deserialize(null)).to.throw(TypeError);
			expect(() => this.db.deserialize(undefined)).to.throw(TypeError);
			expect(() => this.db.deserialize({})).to.throw(TypeError);
			expect(() => this.db.deserialize([])).to.throw(TypeError);
		});

		it('should throw when the database is closed', function () {
			this.source = new Database(':memory:');
			const buffer = this.source.serialize();

			this.db = new Database(':memory:');
			this.db.close();

			expect(() => this.db.deserialize(buffer)).to.throw(TypeError);
		});

		it('should throw when given an invalid/corrupted buffer', function () {
			this.db = new Database(':memory:');

			// Random garbage
			const garbage = crypto.randomBytes(1000);
			expect(() => this.db.deserialize(garbage)).to.throw(Database.SqliteError);

			// Empty buffer is actually valid - creates an empty database
			this.db.deserialize(Buffer.alloc(0));
			expect(this.db.prepare("SELECT * FROM sqlite_master").all()).to.deep.equal([]);

			// Truncated valid buffer
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE t (x INT)').run();
			const buffer = this.source.serialize();
			const truncated = buffer.slice(0, Math.floor(buffer.length / 2));
			expect(() => this.db.deserialize(truncated)).to.throw(Database.SqliteError);
		});

		it('should throw when the attached option is invalid', function () {
			this.source = new Database(':memory:');
			const buffer = this.source.serialize();

			this.db = new Database(':memory:');
			expect(() => this.db.deserialize(buffer, { attached: 123 })).to.throw(TypeError);
			expect(() => this.db.deserialize(buffer, { attached: '' })).to.throw(TypeError);
		});

		it('should throw when database is busy', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE t (x INT)').run();
			for (let i = 0; i < 100; i++) {
				this.source.prepare('INSERT INTO t VALUES (?)').run(i);
			}
			const buffer = this.source.serialize();

			this.db = new Database(':memory:');
			this.db.prepare('CREATE TABLE t (x INT)').run();
			for (let i = 0; i < 100; i++) {
				this.db.prepare('INSERT INTO t VALUES (?)').run(i);
			}

			// Start an iterator (makes database "busy" with iterators)
			const iterator = this.db.prepare('SELECT * FROM t').iterate();
			iterator.next(); // Start iteration

			expect(() => this.db.deserialize(buffer)).to.throw(TypeError);

			// Clean up iterator
			iterator.return();
		});
	});

	describe('session extension compatibility', function () {
		it('should work with changesets after deserialize', function () {
			// Create and populate source
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, quantity INT)').run();
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(1, 'Widget', 10);
			this.source.prepare('INSERT INTO items VALUES (?, ?, ?)').run(2, 'Gadget', 5);

			const buffer = this.source.serialize();

			// Deserialize into target
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			// Create session and make changes
			const session = this.target.createSession();
			session.attach('items');

			this.target.prepare('UPDATE items SET quantity = ? WHERE id = ?').run(20, 1);
			this.target.prepare('INSERT INTO items VALUES (?, ?, ?)').run(3, 'Doohickey', 15);

			const changeset = session.changeset();
			session.close();

			expect(changeset).to.be.an.instanceof(Buffer);
			expect(changeset.length).to.be.above(0);

			// Apply changeset to another database
			const other = new Database(':memory:');
			other.deserialize(buffer);
			other.applyChangeset(changeset);

			const rows = other.prepare('SELECT * FROM items ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, name: 'Widget', quantity: 20 },
				{ id: 2, name: 'Gadget', quantity: 5 },
				{ id: 3, name: 'Doohickey', quantity: 15 }
			]);

			other.close();
		});

		it('should support invert changeset workflow after deserialize', function () {
			// Create source with initial state
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE data (id INTEGER PRIMARY KEY, value INT)').run();
			this.source.prepare('INSERT INTO data VALUES (?, ?)').run(1, 100);

			const buffer = this.source.serialize();

			// Deserialize and make trackable changes
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const session = this.target.createSession();
			session.attach('data');

			this.target.prepare('UPDATE data SET value = ? WHERE id = ?').run(200, 1);

			const changeset = session.changeset();
			session.close();

			// Invert and apply to rollback
			const inverted = this.target.invertChangeset(changeset);
			this.target.applyChangeset(inverted);

			const row = this.target.prepare('SELECT * FROM data').get();
			expect(row).to.deep.equal({ id: 1, value: 100 });
		});
	});

	describe('roundtrip integrity', function () {
		it('should maintain perfect data integrity through serialize/deserialize cycles', function () {
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

			// Multiple serialize/deserialize cycles
			let buffer = this.source.serialize();
			for (let cycle = 0; cycle < 5; cycle++) {
				const db = new Database(':memory:');
				db.deserialize(buffer);
				buffer = db.serialize();
				db.close();
			}

			// Final verification
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const sourceRows = this.source.prepare('SELECT * FROM test_integrity ORDER BY id').all();
			const targetRows = this.target.prepare('SELECT * FROM test_integrity ORDER BY id').all();

			expect(targetRows.length).to.equal(sourceRows.length);
			for (let i = 0; i < sourceRows.length; i++) {
				expect(targetRows[i].id).to.equal(sourceRows[i].id);
				expect(targetRows[i].int_val).to.equal(sourceRows[i].int_val);
				expect(targetRows[i].real_val).to.be.closeTo(sourceRows[i].real_val, 0.0000001);
				expect(targetRows[i].text_val).to.equal(sourceRows[i].text_val);
				expect(Buffer.compare(targetRows[i].blob_val, sourceRows[i].blob_val)).to.equal(0);
			}
		});

		it('should produce identical buffers for serialize after deserialize', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE t (a INT, b TEXT)').run();
			this.source.prepare('INSERT INTO t VALUES (?, ?)').run(1, 'hello');
			this.source.prepare('INSERT INTO t VALUES (?, ?)').run(2, 'world');

			const originalBuffer = this.source.serialize();

			this.target = new Database(':memory:');
			this.target.deserialize(originalBuffer);

			const newBuffer = this.target.serialize();

			expect(originalBuffer.length).to.equal(newBuffer.length);
			expect(Buffer.compare(originalBuffer, newBuffer)).to.equal(0);
		});
	});

	describe('wa-sqlite parity tests', function () {
		// These tests are ported from wa-sqlite serialize-deserialize.test.ts
		// to ensure our native implementation has the same robustness

		function checkExportHeader(buffer) {
			// SQLite format 3\0 - first 16 bytes
			return buffer[0] === 0x53 && // 'S'
			       buffer[1] === 0x51 && // 'Q'
			       buffer[2] === 0x4c && // 'L'
			       buffer[3] === 0x69 && // 'i'
			       buffer[4] === 0x74 && // 't'
			       buffer[5] === 0x65;   // 'e'
		}

		it('should handle multiple sequential exports without corruption', function () {
			// This test verifies that multiple serialize calls don't corrupt the database
			// (Originally caught a corruption bug in wa-sqlite)

			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)').run();
			for (let i = 0; i < 100; i++) {
				this.source.prepare('INSERT INTO test (name) VALUES (?)').run(`Item ${i}`);
			}

			// First export
			const export1 = this.source.serialize();
			expect(checkExportHeader(export1)).to.be.true;

			// Second export - this previously caused corruption in wa-sqlite
			const export2 = this.source.serialize();
			expect(checkExportHeader(export2)).to.be.true;

			// Both should be identical
			expect(export1.length).to.equal(export2.length);
			expect(Buffer.compare(export1, export2)).to.equal(0);

			// Now deserialize into new database - should not be corrupted
			this.target = new Database(':memory:');
			this.target.deserialize(export2);

			const rows = this.target.prepare('SELECT COUNT(*) as c FROM test').get();
			expect(rows.c).to.equal(100);
			expect(checkExportHeader(this.target.serialize())).to.be.true;
		});

		it('should maintain data integrity across multiple export operations', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)').run();
			for (let i = 0; i < 50; i++) {
				this.source.prepare('INSERT INTO test (data) VALUES (?)').run(`data_${i}`);
			}

			const exports = [];
			for (let i = 0; i < 5; i++) {
				exports.push(this.source.serialize());
				expect(checkExportHeader(exports[i])).to.be.true;
			}

			// All exports should be identical
			for (let i = 1; i < exports.length; i++) {
				expect(exports[i].length).to.equal(exports[0].length);
				expect(Buffer.compare(exports[i], exports[0])).to.equal(0);
			}

			// Verify each can be deserialized correctly
			for (let i = 0; i < exports.length; i++) {
				const db = new Database(':memory:');
				db.deserialize(exports[i]);
				const count = db.prepare('SELECT COUNT(*) as c FROM test').get().c;
				expect(count).to.equal(50);
				db.close();
			}
		});

		it('should handle many deserialize/serialize cycles without memory issues', function () {
			this.timeout(10000);

			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE test (id INTEGER PRIMARY KEY, data BLOB)').run();

			// Insert 1MB of data
			const blob = crypto.randomBytes(1024 * 1024);
			this.source.prepare('INSERT INTO test (data) VALUES (?)').run(blob);

			let buffer = this.source.serialize();
			const originalSize = buffer.length;

			// Perform many cycles
			for (let i = 0; i < 20; i++) {
				const db = new Database(':memory:');
				db.deserialize(buffer);
				buffer = db.serialize();
				db.close();

				// Size should remain constant
				expect(buffer.length).to.equal(originalSize);
				expect(checkExportHeader(buffer)).to.be.true;
			}

			// Final verification
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);
			const row = this.target.prepare('SELECT data FROM test').get();
			expect(Buffer.compare(row.data, blob)).to.equal(0);
		});

		it('should have valid SQLite header after deserialize', function () {
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE t (x INT)').run();
			this.source.prepare('INSERT INTO t VALUES (42)').run();

			const buffer = this.source.serialize();

			// Check SQLite header format
			const header = Array.from(buffer.slice(0, 16))
				.map(b => b.toString(16).padStart(2, '0'))
				.join(' ');

			// "SQLite format 3\0"
			expect(header).to.equal('53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00');

			// After deserialize, re-serializing should produce same header
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			const newBuffer = this.target.serialize();
			const newHeader = Array.from(newBuffer.slice(0, 16))
				.map(b => b.toString(16).padStart(2, '0'))
				.join(' ');

			expect(newHeader).to.equal(header);
		});

		it('should handle deserialize followed by modifications then serialize', function () {
			// Create source with initial data
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE items (id INTEGER PRIMARY KEY, value INT)').run();
			this.source.prepare('INSERT INTO items VALUES (1, 100)').run();
			this.source.prepare('INSERT INTO items VALUES (2, 200)').run();

			const buffer = this.source.serialize();

			// Deserialize, modify, serialize
			this.target = new Database(':memory:');
			this.target.deserialize(buffer);

			// Make modifications
			this.target.prepare('UPDATE items SET value = 150 WHERE id = 1').run();
			this.target.prepare('INSERT INTO items VALUES (3, 300)').run();
			this.target.prepare('DELETE FROM items WHERE id = 2').run();

			// Serialize modified database
			const modifiedBuffer = this.target.serialize();
			expect(checkExportHeader(modifiedBuffer)).to.be.true;

			// Verify modifications persisted
			const final = new Database(':memory:');
			final.deserialize(modifiedBuffer);

			const rows = final.prepare('SELECT * FROM items ORDER BY id').all();
			expect(rows).to.deep.equal([
				{ id: 1, value: 150 },
				{ id: 3, value: 300 }
			]);

			final.close();
		});

		it('should handle concurrent database operations with deserialize', function () {
			// Test that deserialize works correctly when there are multiple databases
			const databases = [];

			// Create source
			this.source = new Database(':memory:');
			this.source.prepare('CREATE TABLE shared (id INT, db_num INT)').run();
			const buffer = this.source.serialize();

			// Create multiple databases from same source
			for (let i = 0; i < 5; i++) {
				const db = new Database(':memory:');
				db.deserialize(buffer);
				db.prepare('INSERT INTO shared VALUES (?, ?)').run(i, i);
				databases.push(db);
			}

			// Each should have independent data
			for (let i = 0; i < databases.length; i++) {
				const count = databases[i].prepare('SELECT COUNT(*) as c FROM shared').get().c;
				expect(count).to.equal(1);

				const row = databases[i].prepare('SELECT * FROM shared').get();
				expect(row.id).to.equal(i);
				expect(row.db_num).to.equal(i);
			}

			// Cleanup
			databases.forEach(db => db.close());
		});
	});
});
