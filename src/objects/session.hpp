class Session : public node::ObjectWrap {
public:

	~Session();

	// Whenever this is used, db->RemoveSession must be invoked beforehand.
	void CloseHandles();

	// Used to support ordered containers.
	static inline bool Compare(Session const * const a, Session const * const b) {
		return a->id < b->id;
	}

	static INIT(Init);

private:

	explicit Session(
		Database* db,
		sqlite3_session* session_handle,
		sqlite3_uint64 id
	);

	static NODE_METHOD(JS_new);
	static NODE_METHOD(JS_attach);
	static NODE_METHOD(JS_changeset);
	static NODE_METHOD(JS_close);

	Database* const db;
	sqlite3_session* session_handle;
	const sqlite3_uint64 id;
	bool alive;
};
