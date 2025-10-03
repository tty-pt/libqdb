# libqdb
This is basically a libdb adapter for [libqmap](https://github.com/tty-pt/qmap). It just provides a way to use file backing (using libdb in this case). But this repo also provides a binary to easily make indices in the shell.

## Installation
> Check out [these instructions](https://github.com/tty-pt/ci/blob/main/docs/install.md#install-ttypt-packages).

## Usage

### The cli tool
This executable is a way to make indexes easily right from the shell.

It allows for a few different kinds of databases to be created, queried and changed
easily and with flexibility. You should be able to find documentation using:
```sh
qdb -?
```
But we still want to give you some examples.

First of all, there are some things you should know.

#### Put
The first thing you do is to put something into a database.
That is very easy! Look:
```sh
qdb -p hi a.db
```

But what about the types envolved? Well... It defaults to
unsigned to string. With automatic indexes, so you can easily
put a value like that. How would you do another one?
```sh
qdb -p 3 b.db:a:u
```
This specifies that you want unsigned-type values. Dandy.
But how to specify value types? Easy again!
Just add another colon, like a roman! They fix everything.
```sh
qdb -p 3 b.db:s:u # You guessed it. String keys, unsigned values.
```

#### Key types
There are some key types built-in in this first version. Here they are:

 - 'a' means unsigned but with (optional) automatic indexes - that's the default.
 - 't\<what\>' means type 'what' and possibly duplicate keys!
 - 'u' means unsigned.
 - 's' means string.

For value types, remove 'a' and 't' from that list.

#### Examples
Put a person into the owner database:
```sh
qdb -p Mathew owners.db # Output: 4
```

List owners!
```sh
qdb -l owners.db
```

Insert pets into the pet database:
```sh
qdb -p cat -p dog pets.db # Output: 2 and 3
```

Let's associate them!
```sh
qdb -p 4:2 -p 4:3 assoc.db:t:u
```

Let's see all of Mathew's pets (show their names):
```sh
qdb -a pets.db -g4 assoc.db
```

Get a random one:
```sh
qdb -q owners.db -a pets.db -RMathew assoc.db
```

#### More information
See, those **-q** and **-a** flags can be handy.
Since with them you can print (-a) or query (-q)
using values in another database.
We also have a **-r** flag that can reverse the lookups.
And more! Be sure to check the help (-?).

Hope this is useful for you!
