'use strict';
const Database = require('../.');

describe('Database#createSession()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		// Session extension requires tables with PRIMARY KEY to track changes
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
		this.db.prepare('CREATE TABLE other (id INTEGER PRIMARY KEY, x REAL)').run();
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw when the database is closed', function () {
		this.db.close();
		expect(() => this.db.createSession()).to.throw(TypeError);
	});
	it('should return a session object', function () {
		const session = this.db.createSession();
		expect(session).to.be.an('object');
		expect(session.attach).to.be.a('function');
		expect(session.changeset).to.be.a('function');
		expect(session.close).to.be.a('function');
		session.close();
	});
	it('should accept an optional database name argument', function () {
		const session1 = this.db.createSession();
		const session2 = this.db.createSession('main');
		expect(session1).to.be.an('object');
		expect(session2).to.be.an('object');
		session1.close();
		session2.close();
	});
	it('should not throw for a non-attached database name', function () {
		// SQLite creates sessions for non-existent databases without throwing
		// The session simply won't track any changes
		const session = this.db.createSession('nonexistent');
		expect(session).to.be.an('object');
		session.close();
	});
});

describe('Session#attach()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		// Session extension requires tables with PRIMARY KEY to track changes
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
		this.db.prepare('CREATE TABLE other (id INTEGER PRIMARY KEY, x REAL)').run();
		this.session = this.db.createSession();
	});
	afterEach(function () {
		this.session.close();
		this.db.close();
	});

	it('should attach a specific table for tracking', function () {
		this.session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare('INSERT INTO other VALUES (1, 3.14)').run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should attach all tables when passed null', function () {
		this.session.attach(null);
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare('INSERT INTO other VALUES (1, 3.14)').run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should attach all tables when passed undefined', function () {
		this.session.attach(undefined);
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should attach all tables when called with no arguments', function () {
		this.session.attach();
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should allow attaching multiple tables', function () {
		this.session.attach('entries');
		this.session.attach('other');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare('INSERT INTO other VALUES (1, 3.14)').run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
});

describe('Session#changeset()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
		this.session = this.db.createSession();
		this.session.attach('entries');
	});
	afterEach(function () {
		this.session.close();
		this.db.close();
	});

	it('should return undefined when no changes were made', function () {
		const changeset = this.session.changeset();
		expect(changeset).to.be.undefined;
	});
	it('should return a buffer containing INSERT changes', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should return a buffer containing UPDATE changes', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.session.close();
		this.session = this.db.createSession();
		this.session.attach('entries');
		this.db.prepare("UPDATE entries SET a = 'bar' WHERE b = 1").run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should return a buffer containing DELETE changes', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.session.close();
		this.session = this.db.createSession();
		this.session.attach('entries');
		this.db.prepare('DELETE FROM entries WHERE b = 1').run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should capture multiple changes', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();
		this.db.prepare("INSERT INTO entries VALUES ('baz', 3)").run();
		const changeset = this.session.changeset();
		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);
	});
	it('should be callable multiple times', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset1 = this.session.changeset();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();
		const changeset2 = this.session.changeset();
		expect(changeset1).to.be.an.instanceof(Buffer);
		expect(changeset2).to.be.an.instanceof(Buffer);
		expect(changeset2.length).to.be.above(changeset1.length);
	});
});

describe('Session#close()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
	});
	afterEach(function () {
		this.db.close();
	});

	it('should close the session', function () {
		const session = this.db.createSession();
		session.attach('entries');
		session.close();
		expect(() => session.attach('entries')).to.throw(TypeError);
	});
	it('should be idempotent', function () {
		const session = this.db.createSession();
		session.close();
		expect(() => session.close()).to.not.throw();
	});
	it('should throw when calling changeset after close', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		session.close();
		expect(() => session.changeset()).to.throw(TypeError);
	});
});

describe('Database#applyChangeset()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw when given a non-buffer argument', function () {
		expect(() => this.db.applyChangeset()).to.throw(TypeError);
		expect(() => this.db.applyChangeset(null)).to.throw(TypeError);
		expect(() => this.db.applyChangeset(123)).to.throw(TypeError);
		expect(() => this.db.applyChangeset('string')).to.throw(TypeError);
		expect(() => this.db.applyChangeset({})).to.throw(TypeError);
		expect(() => this.db.applyChangeset([])).to.throw(TypeError);
	});
	it('should throw when the database is closed', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset = session.changeset();
		session.close();
		this.db.close();
		expect(() => this.db.applyChangeset(changeset)).to.throw(TypeError);
	});
	it('should apply a changeset to the database', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();
		const changeset = session.changeset();
		session.close();

		// Clear the table
		this.db.prepare('DELETE FROM entries').run();
		expect(this.db.prepare('SELECT * FROM entries').all()).to.deep.equal([]);

		// Apply the changeset to restore the data
		this.db.applyChangeset(changeset);
		const rows = this.db.prepare('SELECT * FROM entries ORDER BY b').all();
		expect(rows).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'bar', b: 2 },
		]);
	});
	it('should apply a changeset to a different database', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();
		const changeset = session.changeset();
		session.close();

		// Create a new database with the same schema
		const db2 = new Database(util.next());
		db2.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();

		// Apply the changeset
		db2.applyChangeset(changeset);
		const rows = db2.prepare('SELECT * FROM entries ORDER BY b').all();
		expect(rows).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'bar', b: 2 },
		]);
		db2.close();
	});
});

describe('Database#invertChangeset()', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE entries (a TEXT, b INTEGER PRIMARY KEY)').run();
	});
	afterEach(function () {
		this.db.close();
	});

	it('should throw when given a non-buffer argument', function () {
		expect(() => this.db.invertChangeset()).to.throw(TypeError);
		expect(() => this.db.invertChangeset(null)).to.throw(TypeError);
		expect(() => this.db.invertChangeset(123)).to.throw(TypeError);
		expect(() => this.db.invertChangeset('string')).to.throw(TypeError);
		expect(() => this.db.invertChangeset({})).to.throw(TypeError);
		expect(() => this.db.invertChangeset([])).to.throw(TypeError);
	});
	it('should return an inverted changeset buffer', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		const changeset = session.changeset();
		session.close();

		const inverted = this.db.invertChangeset(changeset);
		expect(inverted).to.be.an.instanceof(Buffer);
		expect(inverted.length).to.be.above(0);
	});
	it('should produce a changeset that undoes INSERT operations', function () {
		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();
		const changeset = session.changeset();
		session.close();

		expect(this.db.prepare('SELECT COUNT(*) as count FROM entries').get().count).to.equal(2);

		const inverted = this.db.invertChangeset(changeset);
		this.db.applyChangeset(inverted);

		expect(this.db.prepare('SELECT COUNT(*) as count FROM entries').get().count).to.equal(0);
	});
	it('should produce a changeset that undoes DELETE operations', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();
		this.db.prepare("INSERT INTO entries VALUES ('bar', 2)").run();

		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare('DELETE FROM entries').run();
		const changeset = session.changeset();
		session.close();

		expect(this.db.prepare('SELECT COUNT(*) as count FROM entries').get().count).to.equal(0);

		const inverted = this.db.invertChangeset(changeset);
		this.db.applyChangeset(inverted);

		const rows = this.db.prepare('SELECT * FROM entries ORDER BY b').all();
		expect(rows).to.deep.equal([
			{ a: 'foo', b: 1 },
			{ a: 'bar', b: 2 },
		]);
	});
	it('should produce a changeset that undoes UPDATE operations', function () {
		this.db.prepare("INSERT INTO entries VALUES ('foo', 1)").run();

		const session = this.db.createSession();
		session.attach('entries');
		this.db.prepare("UPDATE entries SET a = 'bar' WHERE b = 1").run();
		const changeset = session.changeset();
		session.close();

		expect(this.db.prepare('SELECT a FROM entries WHERE b = 1').get().a).to.equal('bar');

		const inverted = this.db.invertChangeset(changeset);
		this.db.applyChangeset(inverted);

		expect(this.db.prepare('SELECT a FROM entries WHERE b = 1').get().a).to.equal('foo');
	});
});

describe('Session extension rollback workflow', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
		this.db.prepare('CREATE TABLE todos (id INTEGER PRIMARY KEY, title TEXT, completed INTEGER)').run();
	});
	afterEach(function () {
		this.db.close();
	});

	it('should support a complete rollback workflow', function () {
		// Initial state
		this.db.prepare("INSERT INTO todos VALUES (1, 'Buy groceries', 0)").run();
		this.db.prepare("INSERT INTO todos VALUES (2, 'Walk the dog', 0)").run();

		// Start tracking changes
		const session = this.db.createSession();
		session.attach('todos');

		// Make some changes
		this.db.prepare("INSERT INTO todos VALUES (3, 'New task', 0)").run();
		this.db.prepare("UPDATE todos SET completed = 1 WHERE id = 1").run();
		this.db.prepare('DELETE FROM todos WHERE id = 2').run();

		// Capture the changes
		const changeset = session.changeset();
		session.close();

		// Verify current state
		let rows = this.db.prepare('SELECT * FROM todos ORDER BY id').all();
		expect(rows).to.deep.equal([
			{ id: 1, title: 'Buy groceries', completed: 1 },
			{ id: 3, title: 'New task', completed: 0 },
		]);

		// Rollback all changes
		const inverted = this.db.invertChangeset(changeset);
		this.db.applyChangeset(inverted);

		// Verify rollback restored original state
		rows = this.db.prepare('SELECT * FROM todos ORDER BY id').all();
		expect(rows).to.deep.equal([
			{ id: 1, title: 'Buy groceries', completed: 0 },
			{ id: 2, title: 'Walk the dog', completed: 0 },
		]);
	});
	it('should support multiple sequential rollbacks', function () {
		// Make first set of changes
		const session1 = this.db.createSession();
		session1.attach('todos');
		this.db.prepare("INSERT INTO todos VALUES (1, 'Task 1', 0)").run();
		const changeset1 = session1.changeset();
		session1.close();

		// Make second set of changes
		const session2 = this.db.createSession();
		session2.attach('todos');
		this.db.prepare("INSERT INTO todos VALUES (2, 'Task 2', 0)").run();
		const changeset2 = session2.changeset();
		session2.close();

		// Make third set of changes
		const session3 = this.db.createSession();
		session3.attach('todos');
		this.db.prepare("INSERT INTO todos VALUES (3, 'Task 3', 0)").run();
		const changeset3 = session3.changeset();
		session3.close();

		// Verify we have 3 tasks
		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(3);

		// Rollback third change
		this.db.applyChangeset(this.db.invertChangeset(changeset3));
		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(2);

		// Rollback second change
		this.db.applyChangeset(this.db.invertChangeset(changeset2));
		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(1);

		// Rollback first change
		this.db.applyChangeset(this.db.invertChangeset(changeset1));
		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(0);
	});
	it('should handle transactions correctly', function () {
		const session = this.db.createSession();
		session.attach('todos');

		// Use a transaction
		const insertTodos = this.db.transaction((todos) => {
			for (const todo of todos) {
				this.db.prepare('INSERT INTO todos VALUES (?, ?, ?)').run(todo.id, todo.title, 0);
			}
		});

		insertTodos([
			{ id: 1, title: 'Task 1' },
			{ id: 2, title: 'Task 2' },
			{ id: 3, title: 'Task 3' },
		]);

		const changeset = session.changeset();
		session.close();

		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(3);

		// Rollback the entire transaction
		this.db.applyChangeset(this.db.invertChangeset(changeset));
		expect(this.db.prepare('SELECT COUNT(*) as count FROM todos').get().count).to.equal(0);
	});
});

describe('Session extension edge cases', function () {
	beforeEach(function () {
		this.db = new Database(util.next());
	});
	afterEach(function () {
		this.db.close();
	});

	it('should handle tables with various column types', function () {
		this.db.prepare(`
			CREATE TABLE mixed (
				id INTEGER PRIMARY KEY,
				text_col TEXT,
				int_col INTEGER,
				real_col REAL,
				blob_col BLOB
			)
		`).run();

		const session = this.db.createSession();
		session.attach('mixed');

		this.db.prepare('INSERT INTO mixed VALUES (1, ?, ?, ?, ?)').run(
			'hello',
			42,
			3.14159,
			Buffer.from([0xde, 0xad, 0xbe, 0xef])
		);

		const changeset = session.changeset();
		session.close();

		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(0);

		// Clear and restore
		this.db.prepare('DELETE FROM mixed').run();
		this.db.applyChangeset(changeset);

		const row = this.db.prepare('SELECT * FROM mixed').get();
		expect(row.id).to.equal(1);
		expect(row.text_col).to.equal('hello');
		expect(row.int_col).to.equal(42);
		expect(row.real_col).to.be.closeTo(3.14159, 0.00001);
		expect(row.blob_col).to.deep.equal(Buffer.from([0xde, 0xad, 0xbe, 0xef]));
	});
	it('should handle NULL values', function () {
		this.db.prepare('CREATE TABLE nulltest (id INTEGER PRIMARY KEY, value TEXT)').run();

		const session = this.db.createSession();
		session.attach('nulltest');

		this.db.prepare('INSERT INTO nulltest VALUES (1, NULL)').run();

		const changeset = session.changeset();
		session.close();

		this.db.prepare('DELETE FROM nulltest').run();
		this.db.applyChangeset(changeset);

		const row = this.db.prepare('SELECT * FROM nulltest').get();
		expect(row.id).to.equal(1);
		expect(row.value).to.be.null;
	});
	it('should handle empty strings', function () {
		this.db.prepare('CREATE TABLE emptytest (id INTEGER PRIMARY KEY, value TEXT)').run();

		const session = this.db.createSession();
		session.attach('emptytest');

		this.db.prepare("INSERT INTO emptytest VALUES (1, '')").run();

		const changeset = session.changeset();
		session.close();

		this.db.prepare('DELETE FROM emptytest').run();
		this.db.applyChangeset(changeset);

		const row = this.db.prepare('SELECT * FROM emptytest').get();
		expect(row.value).to.equal('');
	});
	it('should handle unicode text', function () {
		this.db.prepare('CREATE TABLE unicode (id INTEGER PRIMARY KEY, value TEXT)').run();

		const session = this.db.createSession();
		session.attach('unicode');

		const unicodeText = 'Hello \u4e16\u754c \ud83c\udf0d \u0645\u0631\u062d\u0628\u0627';
		this.db.prepare('INSERT INTO unicode VALUES (1, ?)').run(unicodeText);

		const changeset = session.changeset();
		session.close();

		this.db.prepare('DELETE FROM unicode').run();
		this.db.applyChangeset(changeset);

		const row = this.db.prepare('SELECT * FROM unicode').get();
		expect(row.value).to.equal(unicodeText);
	});
	it('should handle large blobs', function () {
		this.db.prepare('CREATE TABLE largeblob (id INTEGER PRIMARY KEY, data BLOB)').run();

		const session = this.db.createSession();
		session.attach('largeblob');

		const largeBuffer = Buffer.alloc(1024 * 100); // 100KB
		for (let i = 0; i < largeBuffer.length; i++) {
			largeBuffer[i] = i % 256;
		}
		this.db.prepare('INSERT INTO largeblob VALUES (1, ?)').run(largeBuffer);

		const changeset = session.changeset();
		session.close();

		expect(changeset).to.be.an.instanceof(Buffer);
		expect(changeset.length).to.be.above(largeBuffer.length);

		this.db.prepare('DELETE FROM largeblob').run();
		this.db.applyChangeset(changeset);

		const row = this.db.prepare('SELECT * FROM largeblob').get();
		expect(row.data).to.deep.equal(largeBuffer);
	});
	it('should handle multiple sessions on the same database', function () {
		this.db.prepare('CREATE TABLE multi (id INTEGER PRIMARY KEY, value TEXT)').run();

		const session1 = this.db.createSession();
		const session2 = this.db.createSession();
		session1.attach('multi');
		session2.attach('multi');

		this.db.prepare("INSERT INTO multi VALUES (1, 'one')").run();
		const changeset1 = session1.changeset();

		this.db.prepare("INSERT INTO multi VALUES (2, 'two')").run();
		const changeset2 = session2.changeset();

		session1.close();
		session2.close();

		// Both sessions captured changes
		expect(changeset1).to.be.an.instanceof(Buffer);
		expect(changeset2).to.be.an.instanceof(Buffer);
		// Session 2 captured both inserts, so it should be larger
		expect(changeset2.length).to.be.above(changeset1.length);
	});
});
