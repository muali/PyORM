# PyORM
Simple python ORM/ODM for MySQL/MongoDB

Functionality:
    Creating class for each entity on connection, i.e. for table in MySQL or for collection in MongoDB.
        Only one connection for each database is possible. Conflicts of class name on connection to multiple database with same entity
        names is not resolvable.
    Each created class has constructor only by id (primary key in MySQL, _id field in MongoDB).
        If object not exists in database it will be added on commit. Only one object with unique id exists.
        All record field (columns value) is presented as field of object. Adding custom field didn't affect database if DBMS is MySQL.
        Otherwise, if field type is valid to store, it will be added to record.
    All changes performs in database on commit method call. Changes performs in order.
        If some consecutive changes affect single object, then this changes may be merged in single query (and some changes can be skipped).
        In other case no changes  can be skipped, even if it's not important for final state of database.
        There is no any warranty of valid changes if multiple users work with database.

Possible extension of functionality:
    Resolve foreign keys as related object, but not as column value.