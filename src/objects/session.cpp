Session::Session(
	Database* db,
	sqlite3_session* session_handle,
	sqlite3_uint64 id
) :
	node::ObjectWrap(),
	db(db),
	session_handle(session_handle),
	id(id),
	alive(true) {
	assert(db != NULL);
	assert(session_handle != NULL);
	db->AddSession(this);
}

Session::~Session() {
	if (alive) db->RemoveSession(this);
	CloseHandles();
}

// Whenever this is used, db->RemoveSession must be invoked beforehand.
void Session::CloseHandles() {
	if (alive) {
		alive = false;
		sqlite3session_delete(session_handle);
	}
}

INIT(Session::Init) {
	v8::Local<v8::FunctionTemplate> t = NewConstructorTemplate(isolate, data, JS_new, "Session");
	SetPrototypeMethod(isolate, data, t, "attach", JS_attach);
	SetPrototypeMethod(isolate, data, t, "changeset", JS_changeset);
	SetPrototypeMethod(isolate, data, t, "enable", JS_enable);
	SetPrototypeMethod(isolate, data, t, "close", JS_close);
	return t->GetFunction(OnlyContext).ToLocalChecked();
}

NODE_METHOD(Session::JS_new) {
	UseAddon;
	if (!addon->privileged_info) return ThrowTypeError("Disabled constructor");
	assert(info.IsConstructCall());
	Database* db = Unwrap<Database>(addon->privileged_info->This());
	REQUIRE_DATABASE_OPEN(db->GetState());
	REQUIRE_DATABASE_NOT_BUSY(db->GetState());

	v8::Local<v8::Object> database = (*addon->privileged_info)[0].As<v8::Object>();
	v8::Local<v8::String> dbName = (*addon->privileged_info)[1].As<v8::String>();

	UseIsolate;
	v8::String::Utf8Value db_name(isolate, dbName);

	sqlite3_session* session_handle;
	int status = sqlite3session_create(db->GetHandle(), *db_name, &session_handle);

	if (status != SQLITE_OK) {
		Database::ThrowSqliteError(addon, db->GetHandle());
		return;
	}

	Session* session = new Session(db, session_handle, addon->NextId());
	session->Wrap(info.This());
	SetFrozen(isolate, OnlyContext, info.This(), addon->cs.database, database);

	info.GetReturnValue().Set(info.This());
}

NODE_METHOD(Session::JS_attach) {
	Session* session = Unwrap<Session>(info.This());
	if (!session->alive) return ThrowTypeError("The session has been closed");
	REQUIRE_DATABASE_OPEN(session->db->GetState());

	UseIsolate;
	const char* table_name = NULL;

	// If a string argument is provided, attach only that table
	// If null/undefined or no argument, attach all tables (NULL)
	if (info.Length() > 0 && !info[0]->IsNullOrUndefined()) {
		if (!info[0]->IsString()) {
			return ThrowTypeError("Expected first argument to be a string or null");
		}
		v8::String::Utf8Value utf8(isolate, info[0].As<v8::String>());
		table_name = *utf8;
		int status = sqlite3session_attach(session->session_handle, table_name);
		if (status != SQLITE_OK) {
			session->db->ThrowDatabaseError();
		}
	} else {
		// NULL means attach all tables
		int status = sqlite3session_attach(session->session_handle, NULL);
		if (status != SQLITE_OK) {
			session->db->ThrowDatabaseError();
		}
	}
}

NODE_METHOD(Session::JS_enable) {
	Session* session = Unwrap<Session>(info.This());
	if (!session->alive) return ThrowTypeError("The session has been closed");
	REQUIRE_DATABASE_OPEN(session->db->GetState());

	UseIsolate;

	// Require a boolean argument (matches wa-sqlite API pattern)
	if (info.Length() < 1 || !info[0]->IsBoolean()) {
		return ThrowTypeError("Expected first argument to be a boolean");
	}

	int enable_flag = info[0]->BooleanValue(isolate) ? 1 : 0;
	sqlite3session_enable(session->session_handle, enable_flag);

	// Return void (matches wa-sqlite API)
}

// Custom destructor for sqlite3_free
static void FreeSqliteMemory(char* data, void* hint) {
	sqlite3_free(data);
}

NODE_METHOD(Session::JS_changeset) {
	Session* session = Unwrap<Session>(info.This());
	if (!session->alive) return ThrowTypeError("The session has been closed");
	REQUIRE_DATABASE_OPEN(session->db->GetState());

	int size = 0;
	void* buffer = NULL;
	int status = sqlite3session_changeset(session->session_handle, &size, &buffer);

	if (status != SQLITE_OK) {
		session->db->ThrowDatabaseError();
		return;
	}

	UseIsolate;

	// If no changes, return undefined
	if (buffer == NULL || size == 0) {
		if (buffer) sqlite3_free(buffer);
		info.GetReturnValue().Set(v8::Undefined(isolate));
		return;
	}

	// Create a Node.js Buffer from the changeset data
	// sqlite3_free will be called when the buffer is garbage collected
	auto result = SAFE_NEW_BUFFER(
		isolate,
		reinterpret_cast<char*>(buffer),
		size,
		FreeSqliteMemory,
		NULL
	);

	if (result.IsEmpty()) {
		sqlite3_free(buffer);
		return ThrowError("Failed to create buffer for changeset");
	}

	info.GetReturnValue().Set(result.ToLocalChecked());
}

NODE_METHOD(Session::JS_close) {
	Session* session = Unwrap<Session>(info.This());
	if (session->alive) {
		session->db->RemoveSession(session);
		session->CloseHandles();
	}
	info.GetReturnValue().Set(info.This());
}
