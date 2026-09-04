# Scout

Scout inventories file trees into SQLite manifests keyed on content hash.
Run against a disk, a directory, or a NAS share and you get a `.scout.db` file:
one row per file with its:

- path
- size
- mtime
- b3c32 hash
- plus:
  - what the manifest knows about the filesystem it came from.

Manifests are plain SQLite.
Open them in `sqlite3` or Datasette, attach two and join on `hash`, or
pipe Scout's own verbs together to ask which files on an old disk have
already made it to the NAS.

Scout is in early development.
See [doc/README.md](doc/README.md) for the plan, contribution rules,
and QA process.

## Credit

Scout is inspired by Simon Willison's Datasette.
I thought what data-set do I constantly wrestle with
that would be nice to offline and analyze?
My file trees across many different disks and shares.
Hopefully using a tool like datasette and scout makes your data hoard easier to manage.
The design of the schema is intentionally meant to be highly legible.
You should be able to run the `scan` subcommand.
Then trivially run SQL queries against the results to do whatever you need.
Otherwise use the coming subcommands that can do the same thing.
The scout CLI will do its best to be UNIX like and highly composed with other tools.

## License

AGPL-3.0-or-later.
