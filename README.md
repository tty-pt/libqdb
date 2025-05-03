# libqhash

A library for easy C databases (disk backed if needed). Built on top of libdb but optimized
for simpler usage.

I'll start this documentation with the most common functions, but know that
there are others which can be useful sometimes which you can find later on.
# Common functions
## qdb\_init
```c
void qdb_init(void);
```
> This initializes the system.

Please run it before anything else!
## qdb\_open
```c
int qdb_open(char *database, char *key_tid, char *value_tid, unsigned flags);
```
> Open a database without having to specify too much stuff.

*key_tid* and *value_tid* are strings which indicated the usual data type for keys and values.
Why? Because we don't want to specify lengths all the time, just pointers.
And if we know about your data types in advance we can do just that.

But how do we register types of data? (you might ask)
Let's answer that right away.

PS: More information in the section "Less common functions / qdb\_openc".

## qdb\_reg
```c
void qdb_reg(char *key, size_t len);
```
> Register a new data type using only a length

## qdb\_put
```c
unsigned qdb_put(unsigned hd, void *key, void *value);
```
> Put a key value pair into the database

Yeah, now that the database is type aware, we can easily put and retrieve values.

If you use NULL as the key, it will generate an automatic index
if the database is configured for it, and returns it.

## qdb\_get
```c
int qdb_get(unsigned hd, void *value, void *key);
```
> Get a value from a key

See how easy that makes it?
## qdb\_del
```c
void qdb_del(unsigned hd, void *key, void *value);
```
> Delete a key-value pair. If value is NULL, delete all values from the key
## qdb\_close
```c
void qdb_close(unsigned hd, unsigned flags);
```
> Closing a database

## qdb\_exists
```c
int qdb_exists(unsigned hd, void *key);
```
> Check for key's existence

With those out of the way, let's get into cursors and iteration!

## qdb\_iter
```c
qdb_cur_t qdb_iter(unsigned hd, void *key);
```
> Start an iteration

Use this to start an iteration of all values in the given key.
You might use NULL as key to not filter out any values.

## qdb\_next
```c
int qdb_next(void *key, void *value, qdb_cur_t *cur);
```
> Get the next key / value in the iteration

I recommend you use it like so:
```c
char person[BUFSIZ], pet[BUFSIZ];
qdb_cur_t c = qdb_iter(person_pets_hd, "Joe");

while (qdb_next(person, pet, &c))
    fprint("Joe has pet: %s\n", pet);
```
## qdb\_fin
```c
void qdb_fin(qdb_cur_t *cur);
```
> Stop an iteration early

Please call this before you might break or return from
a while loop like that early. So that the cursor is
cleanly closed. Otherwise you might have problems
later on.

Well. That gets the most common functions out of the way.
We even included some more uncommon ones.
I guess we can call that an early exit.

# Less common functions
## qdb\_openc
```c
unsigned qdb_openc(const char *file, const char *database, int mode, unsigned flags, int type, char *key_tid, char *value_tid);
```
> Open a database, but be more specific.

What if you want to specify a file, for disk-based database?
And maybe you want to specify that you want a DB\_BTREE instead of a DB\_HASH.
Well, you have another way of doing that by using qdb\_config, which is an
object with the defaults for open operations. But in case you want to specify
everything in one go, you have this option.

## qdb\_regc
```c
void qdb_regc(char *key, qdb_type_t *type);
```
> Register a new data type, but we might want to calculate the size dynamically. Or provide it a print callback, or something.

## qdb\_putc
```c
int qdb_putc(unsigned hd, void *key, size_t key_len, void *value, size_t value_len);
```
> A low-level way to put keys and values that is not type-aware.

## qdb\_getc
```c
void *qdb_getc(unsigned hd, size_t *size, void *key_r, size_t key_len);
```
> And a low-level way to get items from the database that is not type-aware.

## qdb\_pget
```c
int qdb_pget(unsigned hd, void *pkey, void *key);
```
> Get the primary key corresponding to a key of a secondary database

Now that we mention it:
## qdb\_assoc
```c
void qdb_assoc(unsigned hd, unsigned link, qdb_assoc_t assoc);
typedef void (*qdb_assoc_t)(void **data, uint32_t *len, void *key, void *value);
```
> Associate a secondary database to a primary one.

If you use NULL as the callback, a simple mapping of the primary's key will be done.
## qdb\_cdel
```c
int qdb_cdel(qdb_cur_t *cur);
```
> Delete the item under the current iteration of the cursor

## qdb\_drop
```c
int qdb_drop(unsigned hd);
```
> Drop everything in a database (except metadata)

## qdb\_sync
```c
void qdb_sync(unsigned hd);
```
> Sync a database to disk without closing it.

## qdb\_existsc
```c
int qdb_existsc(unsigned hd, void *key, size_t key_len)
```
> A low-level way to check if a key exists. Not type-aware.

## qdb\_piter
```c
qdb_cur_t qdb_piter(unsigned hd, void *key, unsigned reverse);
```
> Just a little helper to iterate THRICE databases.

## qdb\_len
```c
void qdb_len(unsigned hd, unsigned type, void *thing);
```
> Return the length of a key (QDB\_KEY) or a value (QDB\_VALUE)

## qdb\_print
```c
void qdb_print(unsigned hd, unsigned type, void *thing);
```
> Print a key or a value

Here and when checking types for THRICE, you might use the
least significant bit or QDB\_REVERSE to check the inverse.

# Logging
```c
void qdb_set_logger(log_t logger);
typedef void (*log_t)(int type, const char *fmt, ...);
```
This is how you configure how logging is made.
It defaults to printing to stderr,
but you might as well use syslog for example.

# Environments and transactions (Rarer)
## qdb\_env\_create
```c
DB_ENV *qdb_env_create(void);
```
> Create a database environment,
and set it to some good defaults.

## qdb\_env\_open
```c
void *qdb_env_open(DB_ENV *env, char *dir);
```
> Open it and all ops will use it by default

## qdb\_begin
```c
DB_TXN *qdb_begin(void);
```
> Begin a transaction

## qdb\_commit
```c
void qdb_commit(void);
```
> Commit the upmost transaction on the stack.

## qdb\_abort
```c
void qdb_abort(DB_TXN *txn);
```
> Abort the upmost transaction on the stack.

## qdb\_checkpoint
```c
void qdb_checkpoint(unsigned kbytes, unsigned min, unsigned flags);
```
> This creates a checkpoint. You'll need the libdb docs
for some more detail on some of this.

## transaction stack
Although it is unlikely, you might need to copy the qdb\_config.txnl object.
And replace it temporarily in case you are working with multiple databases
and each needs its own transaction stack.

# Types
These are the built-in types we provide. You can add more if you like.
## "s" - char \*
> The first to arrive is the odd one! A type of variable length!
## "u" - unsigned
> Pretty self explanatory
## "p" - void \*
> Just a pointer. If what it points to doesn't change, that's all you need.
## "ul" - unsigned long
> You know, at this point.

# Flags
These are the flags you can provide when opening databases:
## QH\_AINDEX
> Use automatic indexes (don't forget to use 'u' as the key type).
## QH\_RDONLY
> Don't change the database, we're just interested in reading.

This avoids having to have write permissions.
## QH\_SEC
> This should be a secondary database
## QH\_TXN
> Use transaction support
## QH\_DUP
> Allow duplicate values for the same key
## QH\_THRICE
> We want to have forward and reverse lookup (one primary two secondary).

# Reusable indexes
We also export some features for automatic reusable indexes.
We're internally interested in them. But if you also want to
use them, you are free to.

## struct idml
Is a singly-linked list of unsigned numbers, basically.

## struct idm
Well, this is what we really need for reusable indexes.
Just an idml of free numbers, and the biggest number in the db..

## idm\_init
```c
struct idm idm_init(void);
```
> Just initialize one of these.
## idm\_del
```c
void idm_del(struct idm *idm, unsigned id);
```
> Delete (in other words consider free) a certain id.
## idm\_new
```c
unsigned idm_new(struct idm *idm);
```
> Get an id we can use.

## FILO(name, TYPE, INVALID)
> This is just a macro to easily declare a FIFO. We use it for idml and txnl.

I'm going to be brief describing the provided features.
###  name\_init(void)
> Initializes a stack of this kind.

### name\_push(&stack, TYPE thing)
> Pushes an element into it.

### name\_peek(&stack)
> Returns at the element at the top without popping it.

### name\_pop(&stack)
> Returns at the element at the top and pops it out of the stack.

### name\_iter(&stack)
> Starts iterating over the stack's elements.

### name\_next(&slot, &stack)
> Gets the next iteration and copies the element into the slot.

# The cli tool
This executable is a way to make indexes easily right from the shell.

> Documentation is a Work in progress, since we're changing how it works.

For help:
```sh
qhash -?
```

This will you a listing of ids and pet owner names:
```sh
qhash -l owners.db
```

This will put a pet owner into the database, and output his id:
```sh
qhash -p Mathew owners.db # example output: 4
```

Then insert pets into the pet database:
```sh
qhash -p cat -p dog pets.db # example output: 2 and 3
```

And finally create a database that says that Mathew owns both:
```sh
qhash -m1 -p 4:2 -p 4:3 assoc.db
```

Or list all of Mathew's pets (names included):
```sh
qhash -a pets.db -g4 assoc.db
```

And now get a random pet name that corresponds to Mathew:
```sh
qhash -q owners.db -a pets.db -RMathew assoc.db # example output: dog
```
